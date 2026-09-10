package electrum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testTimeout = 5 * time.Second

type stubTransport struct {
	responses chan []byte
	errors    chan error
	done      chan struct{}
	closeOnce sync.Once
	send      func([]byte) error
}

func (s *stubTransport) SendMessage(message []byte) error { return s.send(message) }
func (s *stubTransport) Responses() <-chan []byte         { return s.responses }
func (s *stubTransport) Errors() <-chan error             { return s.errors }
func (s *stubTransport) Close() error {
	s.closeOnce.Do(func() { close(s.done) })
	return nil
}

func newTestClient(t *testing.T) (*Client, *stubTransport) {
	t.Helper()
	transport := &stubTransport{
		responses: make(chan []byte),
		errors:    make(chan error, 1),
		done:      make(chan struct{}),
	}
	client := &Client{
		transport:    transport,
		handlers:     make(map[uint64]chan *container),
		pushHandlers: make(map[string][]chan *container),
		Error:        make(chan error, 1),
		quit:         make(chan struct{}),
	}
	transport.send = func(message []byte) error {
		var req request
		if err := json.Unmarshal(message, &req); err != nil {
			return err
		}
		result := `null`
		switch req.Method {
		case "blockchain.headers.subscribe":
			result = `{"height":100,"hex":"initial"}`
		case "blockchain.scripthash.subscribe", "blockchain.masternode.subscribe":
			result = `"initial"`
		}
		response := []byte(fmt.Sprintf(`{"id":%d,"result":%s}`, req.ID, result))
		select {
		case transport.responses <- response:
			return nil
		case <-transport.done:
			return ErrServerShutdown
		}
	}
	listenDone := make(chan struct{})
	go func() {
		defer close(listenDone)
		client.listen()
	}()
	t.Cleanup(func() {
		client.Shutdown()
		awaitClosed(t, listenDone)
	})
	return client, transport
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-channel:
		require.True(t, ok, "channel closed before a value arrived")
		return value
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for channel value")
		var zero T
		return zero
	}
}

func awaitClosed[T any](t *testing.T, channel <-chan T) {
	t.Helper()
	timer := time.NewTimer(testTimeout)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-channel:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("channel did not close")
		}
	}
}

func awaitNoPushHandlers(t *testing.T, client *Client) {
	t.Helper()
	require.Eventually(t, func() bool {
		client.pushHandlersLock.RLock()
		defer client.pushHandlersLock.RUnlock()
		return len(client.pushHandlers) == 0
	}, testTimeout, time.Millisecond, "subscription did not unregister")
}

func push(t *testing.T, transport *stubTransport, body string) {
	t.Helper()
	select {
	case transport.responses <- []byte(body):
	case <-time.After(testTimeout):
		t.Fatal("client stopped receiving notifications")
	}
}

func TestShutdownDuringPushRegistration(t *testing.T) {
	client, _ := newTestClient(t)
	// Hold registration at the map lock, then start shutdown and wait for its
	// quit signal before letting either operation touch the map. This ordering
	// reproduces the reported nil-map write without timing-dependent sleeps.
	client.pushHandlersLock.Lock()
	registered := make(chan interface{}, 1)
	go func() {
		defer func() { registered <- recover() }()
		client.listenPush("blockchain.headers.subscribe")
	}()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		client.Shutdown()
	}()
	<-client.quit
	client.pushHandlersLock.Unlock()

	require.Nil(t, receive(t, registered), "registration panicked during shutdown")
	awaitClosed(t, shutdownDone)
	awaitNoPushHandlers(t, client)
}

func TestConcurrentShutdown(t *testing.T) {
	client, _ := newTestClient(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client.Shutdown()
		}()
	}
	wg.Wait()
	require.True(t, client.IsShutdown())
}

func TestPushRegistrationAfterShutdown(t *testing.T) {
	client, _ := newTestClient(t)
	start := make(chan struct{})
	registered := make(chan interface{}, 1)
	go func() {
		defer func() { registered <- recover() }()
		<-start
		client.listenPush("blockchain.headers.subscribe")
	}()
	// Model a subscription goroutine that is not scheduled until shutdown has
	// finished. Unlike a stress loop, this always exercises the reported order.
	client.Shutdown()
	close(start)
	require.Nil(t, receive(t, registered), "late registration panicked")
	awaitNoPushHandlers(t, client)
}

func TestRequestRegistersBeforeSending(t *testing.T) {
	client, transport := newTestClient(t)
	transport.send = func(message []byte) error {
		var req request
		if err := json.Unmarshal(message, &req); err != nil {
			return err
		}
		client.handlersLock.RLock()
		handler := client.handlers[req.ID]
		client.handlersLock.RUnlock()
		if handler == nil {
			return errors.New("response handler was not registered before sending")
		}
		handler <- &container{content: []byte(`{"result":null}`)}
		return nil
	}
	require.NoError(t, client.Ping(context.Background()))
}

func TestShutdownUnblocksPendingRequest(t *testing.T) {
	client, transport := newTestClient(t)
	sent := make(chan struct{})
	transport.send = func([]byte) error { close(sent); return nil }
	result := make(chan error, 1)
	go func() { result <- client.Ping(context.Background()) }()
	<-sent
	client.Shutdown()
	require.ErrorIs(t, receive(t, result), ErrServerShutdown)
}

func TestTransportErrorShutsDownWithoutErrorReader(t *testing.T) {
	client, transport := newTestClient(t)
	_, notifications := client.SubscribeScripthash()
	transport.errors <- errors.New("connection lost")
	awaitClosed(t, client.quit)
	awaitClosed(t, notifications)
	require.EqualError(t, receive(t, client.Error), "connection lost")
}

func TestDuplicateResponsesDoNotBlockShutdown(t *testing.T) {
	client, transport := newTestClient(t)
	client.handlersLock.Lock()
	client.handlers[1] = make(chan *container, 1)
	client.handlersLock.Unlock()
	for i := 0; i < 3; i++ {
		push(t, transport, `{"id":1,"result":null}`)
	}
	client.Shutdown()
}

func TestClosedTransportPreservesTerminalError(t *testing.T) {
	for i := 0; i < 100; i++ {
		transport := &stubTransport{
			responses: make(chan []byte),
			errors:    make(chan error, 1),
			done:      make(chan struct{}),
		}
		transport.errors <- errors.New("connection lost")
		close(transport.errors)
		close(transport.responses)
		client := &Client{
			transport: transport,
			Error:     make(chan error, 1),
			quit:      make(chan struct{}),
		}
		// Both channels are ready before listen starts. Whichever select case
		// wins, the terminal transport error must remain visible to the caller.
		client.listen()
		select {
		case err := <-client.Error:
			require.EqualError(t, err, "connection lost")
		default:
			t.Fatal("transport error was lost when the response channel closed")
		}
	}
}
