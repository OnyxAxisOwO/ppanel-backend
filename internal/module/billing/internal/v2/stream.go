package v2

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	dto "github.com/perfect-panel/server/internal/module/billing/contract"
	"github.com/perfect-panel/server/internal/module/billing/entity/order"
	"github.com/perfect-panel/server/pkg/logger"
	"github.com/redis/go-redis/v9"
)

// ErrTooManyStreams reports that the order already holds the maximum number
// of concurrent event streams.
var ErrTooManyStreams = errors.New("too many concurrent event streams")

// maxStreamsPerOrder caps the concurrent streams one order may hold, across
// all the tickets that stream it.
const maxStreamsPerOrder = 3

// replayBatch is how many events one replay query reads.
const replayBatch = 500

// EventSink receives the events of one stream, in order. The first call
// starts the stream; nothing is sent to the sink when the stream is refused.
type EventSink interface {
	Event(id, name string, data []byte) error
	KeepAlive() error
}

// EventReader reads an order's durable events.
type EventReader interface {
	ListAfter(ctx context.Context, orderNo string, afterID int64, limit int) ([]*order.Event, error)
	EarliestID(ctx context.Context, orderNo string) (int64, error)
}

// Subscription wakes a stream when an order's events were published. Its
// channel closes when the subscription ends.
type Subscription interface {
	Wake() <-chan struct{}
	Close() error
}

// Broker subscribes to the publication of an order's events.
type Broker interface {
	Subscribe(ctx context.Context, orderNo string) (Subscription, error)
}

// StreamLimiter bounds the concurrent streams of one order.
type StreamLimiter interface {
	// Acquire takes one of the order's stream slots for at most ttl and
	// returns its release.
	Acquire(ctx context.Context, orderNo string, ttl time.Duration) (release func(), ok bool)
}

// StreamTiming paces a stream; zero values select the production pacing.
type StreamTiming struct {
	Heartbeat   time.Duration
	CatchUp     time.Duration
	Resubscribe time.Duration
}

// StreamDeps serves the order event streams. The event table is the source
// of truth; the broker only wakes a stream quickly after an outbox
// publication, and a periodic catch-up keeps streams correct while the
// broker or a subscription is unavailable.
type StreamDeps struct {
	Events  EventReader
	Broker  Broker
	Limiter StreamLimiter
	Timing  StreamTiming
}

// StreamRequest names the stream: the order, its ticket and the replay
// cursor (Last-Event-ID, or the after query parameter).
type StreamRequest struct {
	OrderNo string
	Ticket  string
	AfterID int64
}

// StreamEvents serves one order event stream until ctx ends or the ticket
// expires. The order is authorization, subscribe, replay, then live events:
// subscribing before the replay query closes the window in which a payment
// between the query and the subscription would be missed, and the event id
// makes the overlap of both safe to deliver twice.
func (s *Service) StreamEvents(ctx context.Context, req StreamRequest, sink EventSink) error {
	snapshot, expiresAt, err := s.AuthorizeEventStream(ctx, req.OrderNo, req.Ticket)
	if err != nil {
		return err
	}
	if limiter := s.deps.Stream.Limiter; limiter != nil {
		release, ok := limiter.Acquire(ctx, req.OrderNo, time.Until(expiresAt))
		if !ok {
			return ErrTooManyStreams
		}
		defer release()
	}
	stream := &eventStream{service: s, orderNo: req.OrderNo, sink: sink, afterID: req.AfterID}
	defer stream.unsubscribe()
	stream.subscribe(ctx)
	if err := stream.start(ctx, snapshot); err != nil {
		return nil // the client went away while the stream started.
	}

	timing := s.deps.Stream.Timing.withDefaults()
	heartbeat := time.NewTicker(timing.Heartbeat)
	defer heartbeat.Stop()
	catchUp := time.NewTicker(timing.CatchUp)
	defer catchUp.Stop()
	resubscribe := time.NewTicker(timing.Resubscribe)
	defer resubscribe.Stop()
	expiration := time.NewTimer(time.Until(expiresAt))
	defer expiration.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-expiration.C:
			data, _ := json.Marshal(map[string]string{"reason": "ticket_expired"})
			_ = sink.Event("", "stream.expiring", data)
			return nil
		case _, ok := <-stream.wake():
			if !ok {
				stream.unsubscribe()
				continue
			}
			if err := stream.replay(ctx); err != nil {
				return nil
			}
		case <-catchUp.C:
			if err := stream.replay(ctx); err != nil {
				return nil
			}
		case <-heartbeat.C:
			if err := sink.KeepAlive(); err != nil {
				return nil
			}
		case <-resubscribe.C:
			stream.subscribe(ctx)
		}
	}
}

func (t StreamTiming) withDefaults() StreamTiming {
	if t.Heartbeat <= 0 {
		t.Heartbeat = 20 * time.Second
	}
	if t.CatchUp <= 0 {
		t.CatchUp = 5 * time.Second
	}
	if t.Resubscribe <= 0 {
		t.Resubscribe = 5 * time.Second
	}
	return t
}

// eventStream is the state of one stream. It owns at most one subscription
// at a time and closes it when the stream ends, including a subscription
// taken after the first attempt failed.
type eventStream struct {
	service      *Service
	orderNo      string
	sink         EventSink
	afterID      int64
	subscription Subscription
}

// subscribe takes a subscription unless the stream holds one. A failed
// attempt is retried by the resubscribe tick; until then the catch-up tick
// keeps the stream current.
func (e *eventStream) subscribe(ctx context.Context) {
	if e.subscription != nil || e.service.deps.Stream.Broker == nil {
		return
	}
	subscription, err := e.service.deps.Stream.Broker.Subscribe(ctx, e.orderNo)
	if err != nil {
		return
	}
	e.subscription = subscription
}

func (e *eventStream) unsubscribe() {
	if e.subscription == nil {
		return
	}
	_ = e.subscription.Close()
	e.subscription = nil
}

// wake is the current subscription's channel; nil blocks forever while the
// stream holds no subscription.
func (e *eventStream) wake() <-chan struct{} {
	if e.subscription == nil {
		return nil
	}
	return e.subscription.Wake()
}

// start sends the current snapshot, a reset when the cursor is older than
// the retained events, and the events after the cursor.
func (e *eventStream) start(ctx context.Context, snapshot dto.V2OrderSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := e.sink.Event("", "order.snapshot", data); err != nil {
		return err
	}
	if e.afterID > 0 {
		earliestID, err := e.service.deps.Stream.Events.EarliestID(ctx, e.orderNo)
		if err != nil {
			logger.WithContext(ctx).Errorw("[V2OrderEvents] inspect replay cursor failed", logger.Field("error", err.Error()), logger.Field("order_no", e.orderNo))
		} else if earliestID > e.afterID {
			// The cursor points before the retained events: the client must
			// rebuild its state from the snapshot instead of missing events.
			if err := e.sink.Event("", "order.reset", data); err != nil {
				return err
			}
			e.afterID = earliestID - 1
		}
	}
	if err := e.replay(ctx); err != nil {
		logger.WithContext(ctx).Errorw("[V2OrderEvents] initial replay failed", logger.Field("error", err.Error()), logger.Field("order_no", e.orderNo))
	}
	return nil
}

// replay sends every stored event after the cursor.
func (e *eventStream) replay(ctx context.Context) error {
	for {
		events, err := e.service.deps.Stream.Events.ListAfter(ctx, e.orderNo, e.afterID, replayBatch)
		if err != nil {
			return err
		}
		for _, event := range events {
			if event.ID <= e.afterID {
				continue
			}
			if err := e.sink.Event(strconv.FormatInt(event.ID, 10), event.EventType, []byte(event.Payload)); err != nil {
				return err
			}
			e.afterID = event.ID
		}
		if len(events) < replayBatch {
			return nil
		}
	}
}

// RedisBroker wakes streams through the Redis channel the order event
// publisher broadcasts on.
type RedisBroker struct {
	Client *redis.Client
}

// Subscribe confirms the subscription before returning it, so a stream
// only counts on wake-ups Redis actually delivers.
func (b RedisBroker) Subscribe(ctx context.Context, orderNo string) (Subscription, error) {
	if b.Client == nil {
		return nil, errors.New("redis is not configured")
	}
	pubsub := b.Client.Subscribe(ctx, order.EventChannel(orderNo))
	confirmCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := pubsub.Receive(confirmCtx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}
	subscription := &redisSubscription{pubsub: pubsub, wake: make(chan struct{})}
	go subscription.forward(pubsub.Channel())
	return subscription, nil
}

type redisSubscription struct {
	pubsub *redis.PubSub
	wake   chan struct{}
}

// forward turns published messages into wake-ups and closes the wake
// channel once the Redis channel closes, which Close causes.
func (s *redisSubscription) forward(messages <-chan *redis.Message) {
	defer close(s.wake)
	for range messages {
		select {
		case s.wake <- struct{}{}:
		default: // a wake-up is already pending; the replay reads every event.
		}
	}
}

func (s *redisSubscription) Wake() <-chan struct{} { return s.wake }

func (s *redisSubscription) Close() error { return s.pubsub.Close() }

// RedisLimiter counts an order's streams in Redis, across all of its tickets:
// a re-minted ticket draws on the same slots. During a Redis outage every
// stream is admitted: the event table can still sustain it, and the cap is a
// best-effort abuse control, not a reason to hide a paid order from its owner.
type RedisLimiter struct {
	Client *redis.Client
}

// streamCounterGrace keeps an order's counter a minute past the expiry of the
// stream that expires last, so the counter outlives every stream it counts.
const streamCounterGrace = time.Minute

// acquireStreamSlot admits a stream while the order holds fewer than the
// maximum. The expiry only ever grows to cover the stream, so a stream on a
// shorter ticket never shortens the expiry of an open one, and a refused
// attempt changes nothing.
var acquireStreamSlot = redis.NewScript(`
if tonumber(redis.call("GET", KEYS[1]) or "0") >= tonumber(ARGV[1]) then
  return 0
end
redis.call("INCR", KEYS[1])
if redis.call("PTTL", KEYS[1]) < tonumber(ARGV[2]) then
  redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 1
`)

// releaseStreamSlot returns a slot. It creates no counter and never takes one
// below zero, so a release that finds the counter gone leaves nothing behind.
var releaseStreamSlot = redis.NewScript(`
if tonumber(redis.call("GET", KEYS[1]) or "0") > 0 then
  return redis.call("DECR", KEYS[1])
end
return 0
`)

func (l RedisLimiter) Acquire(ctx context.Context, orderNo string, ttl time.Duration) (func(), bool) {
	if l.Client == nil {
		return func() {}, true
	}
	key := "order:sse:connections:" + orderNo
	// PEXPIRE takes milliseconds: go-redis's Expire truncates to whole seconds,
	// which would expire the counter before the stream it counts ends.
	expiry := ttl + streamCounterGrace
	admitted, err := acquireStreamSlot.Run(ctx, l.Client, []string{key}, maxStreamsPerOrder, expiry.Milliseconds()).Int()
	if err != nil {
		return func() {}, true
	}
	if admitted == 0 {
		return func() {}, false
	}
	return func() {
		// The release runs when the stream ends, usually because the client
		// went away and cancelled ctx; the slot must be returned anyway.
		_ = releaseStreamSlot.Run(context.WithoutCancel(ctx), l.Client, []string{key}).Err()
	}, true
}
