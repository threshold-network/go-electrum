package electrum

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSubscribeHeadersDeliversInitialAndPushHeaders(t *testing.T) {
	client, transport := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A server may push a notification before returning the initial response.
	send := transport.send
	transport.send = func(message []byte) error {
		push(t, transport, `{"method":"blockchain.headers.subscribe","params":[{"height":101,"hex":"next"}]}`)
		return send(message)
	}
	headers, err := client.SubscribeHeaders(ctx)
	require.NoError(t, err)
	require.Equal(t, &SubscribeHeadersResult{Height: 100, Hex: "initial"}, receive(t, headers))
	require.Equal(t, &SubscribeHeadersResult{Height: 101, Hex: "next"}, receive(t, headers))
	cancel()
	awaitClosed(t, headers)
	awaitNoPushHandlers(t, client)
}

func TestSubscribeHeadersStopsWithoutReader(t *testing.T) {
	for _, stop := range []string{"cancel", "shutdown"} {
		for _, blocked := range []bool{false, true} {
			t.Run(stop+map[bool]string{false: "/idle", true: "/blocked"}[blocked], func(t *testing.T) {
				client, transport := newTestClient(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				headers, err := client.SubscribeHeaders(ctx)
				require.NoError(t, err)
				if blocked {
					push(t, transport, `{"method":"blockchain.headers.subscribe","params":[{"height":101},{"height":102}]}`)
				}
				if stop == "cancel" {
					cancel()
				} else {
					client.Shutdown()
				}
				// Check cleanup before draining the full output channel: reading it
				// must not be needed to let the forwarding goroutine exit.
				awaitNoPushHandlers(t, client)
				awaitClosed(t, headers)
			})
		}
	}
}

func TestSubscribeHeadersCleansUpOnRequestFailure(t *testing.T) {
	client, _ := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	headers, err := client.SubscribeHeaders(ctx)
	require.ErrorIs(t, err, ErrTimeout)
	require.Nil(t, headers)
	awaitNoPushHandlers(t, client)
}

func TestCancelingOneHeaderSubscriptionPreservesAnother(t *testing.T) {
	client, transport := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first, err := client.SubscribeHeaders(ctx)
	require.NoError(t, err)
	secondCtx, secondCancel := context.WithCancel(context.Background())
	defer secondCancel()
	second, err := client.SubscribeHeaders(secondCtx)
	require.NoError(t, err)
	receive(t, first)
	receive(t, second)
	cancel()
	awaitClosed(t, first)
	push(t, transport, `{"method":"blockchain.headers.subscribe","params":[{"height":101}]}`)
	require.Equal(t, int32(101), receive(t, second).Height)
	secondCancel()
	awaitClosed(t, second)
	awaitNoPushHandlers(t, client)
}

func TestSubscribeHeadersSingleDoesNotRegisterStream(t *testing.T) {
	client, _ := newTestClient(t)
	header, err := client.SubscribeHeadersSingle(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(100), header.Height)
	awaitNoPushHandlers(t, client)
}

func TestSubscribeMasternodeDeliversAndStops(t *testing.T) {
	client, transport := newTestClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notifications, err := client.SubscribeMasternode(ctx, "collateral")
	require.NoError(t, err)
	require.Equal(t, "initial", receive(t, notifications))
	push(t, transport, `{"method":"blockchain.masternode.subscribe","params":["collateral","enabled"]}`)
	require.Equal(t, "collateral", receive(t, notifications))
	require.Equal(t, "enabled", receive(t, notifications))
	cancel()
	awaitClosed(t, notifications)
	awaitNoPushHandlers(t, client)
}

func TestSubscribeMasternodeStopsWithoutReader(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "shutdown"}[shutdown], func(t *testing.T) {
			client, transport := newTestClient(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			notifications, err := client.SubscribeMasternode(ctx, "collateral")
			require.NoError(t, err)
			push(t, transport, `{"method":"blockchain.masternode.subscribe","params":["collateral","enabled"]}`)
			if shutdown {
				client.Shutdown()
			} else {
				cancel()
			}
			awaitNoPushHandlers(t, client)
			awaitClosed(t, notifications)
		})
	}
}

func TestSubscribeScripthashDeliversAndFilters(t *testing.T) {
	client, transport := newTestClient(t)
	sub, notifications := client.SubscribeScripthash()
	defer sub.Close()
	require.NoError(t, sub.Add(context.Background(), "script", "address"))
	require.Equal(t, [2]string{"script", "initial"}, receive(t, notifications).Params)
	push(t, transport, `{"method":"blockchain.scripthash.subscribe","params":["other","ignored"]}`)
	push(t, transport, `{"method":"blockchain.scripthash.subscribe","params":["script","updated"]}`)
	require.Equal(t, [2]string{"script", "updated"}, receive(t, notifications).Params)
	address, err := sub.GetAddress("script")
	require.NoError(t, err)
	require.Equal(t, "address", address)
	sub.Close()
	awaitClosed(t, notifications)
	awaitNoPushHandlers(t, client)
	require.ErrorIs(t, sub.Add(context.Background(), "script"), context.Canceled)
}

func TestScripthashInitialStatusPrecedesPushActivation(t *testing.T) {
	client, transport := newTestClient(t)
	sub, notifications := client.SubscribeScripthash()
	defer sub.Close()
	// Pause initial-status publication after the RPC completes. A new script
	// must stay ineligible for push delivery until that status is published.
	sub.notifyLock.Lock()
	locked := true
	t.Cleanup(func() {
		if locked {
			sub.notifyLock.Unlock()
		}
	})
	responded := make(chan struct{})
	send := transport.send
	transport.send = func(message []byte) error {
		err := send(message)
		close(responded)
		return err
	}
	added := make(chan error, 1)
	go func() { added <- sub.Add(context.Background(), "script") }()
	awaitClosed(t, responded)
	require.Eventually(t, func() bool {
		client.handlersLock.RLock()
		defer client.handlersLock.RUnlock()
		return len(client.handlers) == 0
	}, testTimeout, time.Millisecond, "initial RPC did not finish")
	require.Never(t, func() bool {
		sub.lock.RLock()
		defer sub.lock.RUnlock()
		return len(sub.subscribedSH) > 0
	}, 50*time.Millisecond, time.Millisecond, "push delivery enabled before initial status")
	sub.notifyLock.Unlock()
	locked = false
	require.NoError(t, receive(t, added))
	require.Equal(t, [2]string{"script", "initial"}, receive(t, notifications).Params)
	push(t, transport, `{"method":"blockchain.scripthash.subscribe","params":["script","updated"]}`)
	require.Equal(t, [2]string{"script", "updated"}, receive(t, notifications).Params)
}

func TestScripthashAddCancellationDuringRequest(t *testing.T) {
	for _, stop := range []string{"close", "subscription_context", "request_context"} {
		t.Run(stop, func(t *testing.T) {
			client, transport := newTestClient(t)
			subscriptionCtx, cancelSubscription := context.WithCancel(context.Background())
			defer cancelSubscription()
			sub, notifications := client.SubscribeScripthashContext(subscriptionCtx)
			defer sub.Close()
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			sent := make(chan struct{})
			// Keep the connection open but never answer this request.
			transport.send = func([]byte) error { close(sent); return nil }
			added := make(chan error, 1)
			go func() { added <- sub.Add(requestCtx, "script") }()
			awaitClosed(t, sent)
			client.handlersLock.RLock()
			pending := len(client.handlers)
			client.handlersLock.RUnlock()
			require.Equal(t, 1, pending)
			wantError := error(context.Canceled)
			switch stop {
			case "close":
				sub.Close()
			case "subscription_context":
				cancelSubscription()
			case "request_context":
				cancelRequest()
				wantError = ErrTimeout
			}
			require.ErrorIs(t, receive(t, added), wantError)
			require.False(t, client.IsShutdown(), "canceling Add must keep the connection open")
			client.handlersLock.RLock()
			pending = len(client.handlers)
			client.handlersLock.RUnlock()
			require.Zero(t, pending, "canceled request handler was not removed")
			sub.lock.RLock()
			active := len(sub.subscribedSH)
			sub.lock.RUnlock()
			require.Zero(t, active, "canceled request activated the script")
			if stop == "request_context" {
				require.NoError(t, sub.ctx.Err(), "request cancellation ended the subscription")
				sub.Close()
			}
			awaitClosed(t, notifications)
			awaitNoPushHandlers(t, client)
		})
	}
}

func TestSubscribeScripthashStopsBlockedAdd(t *testing.T) {
	for _, stop := range []string{"cancel", "close", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			client, transport := newTestClient(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sub, notifications := client.SubscribeScripthashContext(ctx)
			defer sub.Close()
			require.NoError(t, sub.Add(context.Background(), "first"))
			responded := make(chan struct{})
			send := transport.send
			transport.send = func(message []byte) error {
				err := send(message)
				close(responded)
				return err
			}
			added := make(chan error, 1)
			go func() { added <- sub.Add(context.Background(), "second") }()
			awaitClosed(t, responded)
			require.Eventually(t, func() bool {
				client.handlersLock.RLock()
				defer client.handlersLock.RUnlock()
				return len(client.handlers) == 0
			}, testTimeout, time.Millisecond)
			switch stop {
			case "cancel":
				cancel()
			case "close":
				sub.Close()
			case "shutdown":
				client.Shutdown()
			}
			err := receive(t, added)
			require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, ErrServerShutdown), "%v", err)
			awaitNoPushHandlers(t, client)
			awaitClosed(t, notifications)
		})
	}
}

func TestSubscribeScripthashStopsBlockedPush(t *testing.T) {
	client, transport := newTestClient(t)
	sub, notifications := client.SubscribeScripthash()
	require.NoError(t, sub.Add(context.Background(), "script"))
	push(t, transport, `{"method":"blockchain.scripthash.subscribe","params":["script","updated"]}`)
	sub.Close()
	awaitNoPushHandlers(t, client)
	awaitClosed(t, notifications)
}

func TestSubscriptionsCloseOnInvalidNotification(t *testing.T) {
	client, transport := newTestClient(t)
	headers, err := client.SubscribeHeaders(context.Background())
	require.NoError(t, err)
	masternodes, err := client.SubscribeMasternode(context.Background(), "collateral")
	require.NoError(t, err)
	_, scripthashes := client.SubscribeScripthash()
	for _, method := range []string{"headers", "masternode", "scripthash"} {
		push(t, transport, `{"method":"blockchain.`+method+`.subscribe","params":42}`)
	}
	awaitClosed(t, headers)
	awaitClosed(t, masternodes)
	awaitClosed(t, scripthashes)
	awaitNoPushHandlers(t, client)
}

func TestSubscriptionRegistrationAndShutdownStress(t *testing.T) {
	client, _ := newTestClient(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, channel := client.SubscribeScripthash()
			sub.Close()
			awaitClosed(t, channel)
		}()
	}
	client.Shutdown()
	wg.Wait()
	awaitNoPushHandlers(t, client)
}
