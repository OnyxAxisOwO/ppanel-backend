package telegram

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/perfect-panel/server/internal/config"
	"github.com/perfect-panel/server/pkg/random"
)

func newBindHarness(tokens map[string]string) (*Bot, *fakeRedisStore, *fakeAccounts, *recordingMessenger) {
	store := &fakeRedisStore{values: tokens}
	accounts := newFakeAccounts()
	messenger := &recordingMessenger{}
	bot := NewBot(BotDependencies{
		Messenger: messenger,
		Sessions:  store,
		Accounts:  accounts,
	})
	return bot, store, accounts, messenger
}

func bindKey(token string) string {
	return fmt.Sprintf("%v:%v", config.TelegramBindKey, token)
}

// Tokens of the issued shape: 32 characters of [A-Za-z0-9], as
// random.KeyNew(32, 1) draws them.
const (
	testBindToken       = "hxjh99bESRm7ap4CJrYa4UFA9MScIEDo"
	testBindTokenSecond = "rq9oHyHGqvy8ngrSkGfBupq0pW5NgtMF"
)

const invalidBindToken = "Bind token is invalid or expired. Please request a new one."

// A session id must no longer work as a bind token: the deep link travels
// through Telegram chats, and accepting session ids let whoever saw one bind
// their own Telegram account — and therefore log in — as that user.
func TestBindRejectsSessionIdAsToken(t *testing.T) {
	for _, command := range []string{"/bind", "/start"} {
		token := testBindToken
		bot, _, accounts, messenger := newBindHarness(map[string]string{
			fmt.Sprintf("%v:%v", config.SessionIdKey, token): "7",
		})

		bot.HandleUpdate(context.Background(), privateUpdate(1001, command+" "+token))

		if len(accounts.bound) != 0 {
			t.Fatalf("%s: session id was accepted as a bind token: %+v", command, accounts.bound[0])
		}
		if got := messenger.last().message; got != invalidBindToken {
			t.Fatalf("%s: message = %q", command, got)
		}
	}
}

// A dedicated bind token binds the account once and is invalidated, so a link
// that leaks after use cannot rebind the account. /start (the deep link) and
// /bind share the one implementation.
func TestBindConsumesDedicatedTokenExactlyOnce(t *testing.T) {
	for _, command := range []string{"/bind", "/start"} {
		t.Run(command, func(t *testing.T) {
			token := testBindToken
			bot, store, accounts, messenger := newBindHarness(map[string]string{bindKey(token): "7"})

			bot.HandleUpdate(context.Background(), privateUpdate(1001, command+" "+token))

			if len(accounts.bound) != 1 {
				t.Fatalf("bindings = %d, want 1", len(accounts.bound))
			}
			if got := accounts.bound[0]; got.UserId != 7 || got.AuthIdentifier != "1001" {
				t.Fatalf("binding = %+v, want user 7 bound to chat 1001", got)
			}
			if _, present := store.values[bindKey(token)]; present {
				t.Fatal("the redeemed token is still stored")
			}
			// The binding lock is released; nothing else is left behind.
			if len(store.values) != 0 || len(store.deleted) != 1 || store.deleted[0] != bindLockKey(7) {
				t.Fatalf("store = %v, deleted = %v; want the lock released and the token consumed", store.values, store.deleted)
			}
			if !messenger.last().markdown {
				t.Fatal("bind confirmation must be sent as MarkdownV2, not plain text")
			}

			// Replaying the same link finds nothing to redeem.
			bot.HandleUpdate(context.Background(), privateUpdate(2002, command+" "+token))
			if len(accounts.bound) != 1 {
				t.Fatal("a consumed bind token was accepted again")
			}
			if got := messenger.last().message; got != invalidBindToken {
				t.Fatalf("message = %q", got)
			}
		})
	}
}

func TestBindWithoutTokenPromptsPerEntryPoint(t *testing.T) {
	for command, want := range map[string]string{
		"/bind":  "Please provide a bind token. Usage: /bind <token>",
		"/start": "Please bind account!",
	} {
		bot, _, _, messenger := newBindHarness(nil)
		bot.HandleUpdate(context.Background(), privateUpdate(42, command))
		if got := messenger.last(); got.chatID != 42 || got.message != want {
			t.Fatalf("%s: message = (%d, %q), want %q", command, got.chatID, got.message, want)
		}
	}
}

// One Telegram account binds one panel account and the other way round; an
// existing binding is never overwritten. A token refused for a reason the
// user can fix is put back with the life it had left, so the user retries
// without a new link and the link's life is not extended; a token that finds
// its account already bound here has done its work and stays consumed.
func TestBindKeepsExistingBindings(t *testing.T) {
	for name, tt := range map[string]struct {
		seed     func(*fakeAccounts)
		want     string
		restored bool
	}{
		"chat bound elsewhere": {
			seed:     func(a *fakeAccounts) { a.addBinding(8, "telegram", "1001") },
			want:     "This Telegram account is already bound to another user.",
			restored: true,
		},
		"account bound to another chat": {
			seed:     func(a *fakeAccounts) { a.addBinding(7, "telegram", "5005") },
			want:     "Your account is already bound to a different Telegram account. Please unbind it first.",
			restored: true,
		},
		"already bound here": {
			seed: func(a *fakeAccounts) { a.addBinding(7, "telegram", "1001") },
			want: "This account is already bound to your Telegram.",
		},
	} {
		t.Run(name, func(t *testing.T) {
			bot, store, accounts, messenger := newBindHarness(map[string]string{bindKey(testBindToken): "7"})
			tt.seed(accounts)

			bot.HandleUpdate(context.Background(), privateUpdate(1001, "/bind "+testBindToken))

			if len(accounts.bound) != 0 {
				t.Fatalf("binding written: %+v", accounts.bound)
			}
			if got := messenger.last().message; got != tt.want {
				t.Fatalf("message = %q, want %q", got, tt.want)
			}
			value, present := store.values[bindKey(testBindToken)]
			if present != tt.restored || (present && (value != "7" || store.ttls[bindKey(testBindToken)] != fakeTokenTTL)) {
				t.Fatalf("token stored = %v (%q, ttl %v), want restored %v with its remaining life", present, value, store.ttls[bindKey(testBindToken)], tt.restored)
			}
			if _, locked := store.values[bindLockKey(7)]; locked {
				t.Fatal("the binding lock was not released")
			}
		})
	}
}

func TestBindRejectsMalformedTokenValue(t *testing.T) {
	bot, _, accounts, messenger := newBindHarness(map[string]string{bindKey(testBindToken): "not-a-user"})
	bot.HandleUpdate(context.Background(), privateUpdate(1001, "/start "+testBindToken))
	if len(accounts.bound) != 0 || !strings.Contains(messenger.last().message, "Invalid session data") {
		t.Fatalf("bound = %+v, message = %q", accounts.bound, messenger.last().message)
	}
}

// A /bind or /start argument that is not a token of the issued shape is
// refused before the store is asked for it. The argument used to be the
// store key itself: "lock:<id>" addressed the binding lock of that account,
// and redeeming it consumed the lock and bound the chat to the user the
// lock's value named. Every argument here is sent while the locks of
// accounts 1 and 7 are held, and none may reach the store.
func TestBindRefusesMalformedTokensBeforeTheStore(t *testing.T) {
	for _, arg := range []string{
		"lock:1",
		"lock:7",
		"lock:",
		"leaked-session-id",
		strings.Repeat("a", 31),
		strings.Repeat("a", 33),
		testBindToken[:31] + "_",
		testBindToken[:31] + ":",
		" " + testBindToken,
		testBindToken + " ",
		testBindToken + "\n",
	} {
		for _, command := range []string{"/bind", "/start"} {
			bot, store, accounts, messenger := newBindHarness(map[string]string{
				bindLockKey(1):         "1",
				bindLockKey(7):         "1",
				bindKey(testBindToken): "7",
			})

			bot.HandleUpdate(context.Background(), privateUpdate(1001, command+" "+arg))

			if got := messenger.last().message; got != invalidBindToken {
				t.Fatalf("%s %q: message = %q, want the invalid token refusal", command, arg, got)
			}
			if len(accounts.bound) != 0 {
				t.Fatalf("%s %q: chat bound: %+v", command, arg, accounts.bound)
			}
			if len(store.takes) != 0 || len(store.deleted) != 0 {
				t.Fatalf("%s %q: store asked for the token (takes %v) or keys released (deleted %v)", command, arg, store.takes, store.deleted)
			}
			if len(store.values) != 3 || store.values[bindLockKey(1)] != "1" || store.values[bindLockKey(7)] != "1" || store.values[bindKey(testBindToken)] != "7" {
				t.Fatalf("%s %q: store changed to %v", command, arg, store.values)
			}
		}
	}
}

// The binding lock of an account lives outside the token keyspace, so no
// token can address it, whatever its spelling.
func TestBindLockKeyLivesOutsideTheTokenKeyspace(t *testing.T) {
	for _, userID := range []int64{1, 7, 1 << 40} {
		if strings.HasPrefix(bindLockKey(userID), config.TelegramBindKey+":") {
			t.Fatalf("bindLockKey(%d) = %q is in the token keyspace", userID, bindLockKey(userID))
		}
	}
}

// isBindToken accepts exactly the shape that random.KeyNew(32, 1) issues.
func TestIsBindTokenAcceptsOnlyTheIssuedShape(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if token := random.KeyNew(32, 1); !isBindToken(token) {
			t.Fatalf("issued token %q is refused", token)
		}
	}
	for _, token := range []string{
		"",
		"1",
		"lock:1",
		"lock:7",
		strings.Repeat("a", 31),
		strings.Repeat("a", 33),
		testBindToken[:31] + "_",
		testBindToken[:31] + ":",
		testBindToken[:31] + "-",
		testBindToken[:31] + " ",
		"\n" + testBindToken[:31],
		strings.Repeat("é", 16),
	} {
		if isBindToken(token) {
			t.Fatalf("isBindToken(%q) = true", token)
		}
	}
}
