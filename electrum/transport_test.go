package electrum

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// writeGate holds a real WebSocket frame inside the underlying socket write.
// This makes overlap with another SendMessage or Close reproducible locally.
type writeGate struct {
	net.Conn
	lock        sync.Mutex
	enabled     bool
	entered     chan struct{}
	release     chan struct{}
	closed      chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	closeOnce   sync.Once
}

func (g *writeGate) Write(body []byte) (int, error) {
	g.lock.Lock()
	enabled := g.enabled
	g.lock.Unlock()
	if enabled {
		g.enterOnce.Do(func() { close(g.entered) })
		select {
		case <-g.release:
		case <-g.closed:
			return 0, net.ErrClosed
		}
	}
	return g.Conn.Write(body)
}

func (g *writeGate) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })
	return g.Conn.Close()
}

func (g *writeGate) block() {
	g.lock.Lock()
	g.enabled = true
	g.lock.Unlock()
}

func (g *writeGate) unblock() { g.releaseOnce.Do(func() { close(g.release) }) }

func newTestWebSocketTransport(t *testing.T) (*WebSocketTransport, *writeGate, *websocket.Conn, <-chan string) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	messages := make(chan string, 32)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(serverDone)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()
		accepted <- conn
		for {
			_, body, err := conn.ReadMessage()
			if err != nil {
				return
			}
			messages <- string(body)
		}
	}))
	t.Cleanup(server.Close)
	gate := &writeGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	dialer := websocket.Dialer{NetDialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, network, address)
		gate.Conn = conn
		return gate, err
	}}
	conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	peer := receive(t, accepted)
	transport := &WebSocketTransport{
		conn:      conn,
		responses: make(chan []byte),
		errors:    make(chan error, 1),
		close:     make(chan struct{}),
		quit:      make(chan struct{}),
	}
	go transport.listen()
	t.Cleanup(func() {
		gate.unblock()
		_ = transport.Close()
		_ = peer.Close()
		awaitClosed(t, transport.close)
		awaitClosed(t, serverDone)
	})
	return transport, gate, peer, messages
}

func asyncCall(call func() error) <-chan error {
	result := make(chan error, 1)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				result <- fmt.Errorf("panic: %v", err)
			}
		}()
		result <- call()
	}()
	return result
}

func TestWebSocketTransportSerializesWrites(t *testing.T) {
	transport, gate, _, messages := newTestWebSocketTransport(t)
	gate.block()
	first := asyncCall(func() error { return transport.SendMessage([]byte("first")) })
	awaitClosed(t, gate.entered)
	second := asyncCall(func() error { return transport.SendMessage([]byte("second")) })
	select {
	case err := <-second:
		t.Fatalf("second write ran while the first frame was blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	gate.unblock()
	require.NoError(t, receive(t, first))
	require.NoError(t, receive(t, second))
	require.Equal(t, "first", receive(t, messages))
	require.Equal(t, "second", receive(t, messages))
}

func TestWebSocketTransportCloseDuringBlockedWrite(t *testing.T) {
	transport, gate, _, _ := newTestWebSocketTransport(t)
	gate.block()
	written := asyncCall(func() error { return transport.SendMessage([]byte("blocked")) })
	awaitClosed(t, gate.entered)
	// Keep the data write blocked throughout Close. The control-write deadline
	// must force the socket closed rather than panic or wait for the write lock.
	require.NoError(t, receive(t, asyncCall(transport.Close)))
	require.Error(t, receive(t, written))
	awaitClosed(t, transport.close)
	require.ErrorIs(t, transport.SendMessage([]byte("late")), ErrServerShutdown)
	require.NoError(t, transport.Close())
}

func TestWebSocketTransportConcurrentClose(t *testing.T) {
	transport, _, _, _ := newTestWebSocketTransport(t)
	results := make([]<-chan error, 32)
	for i := range results {
		results[i] = asyncCall(transport.Close)
	}
	for _, result := range results {
		require.NoError(t, receive(t, result))
	}
	awaitClosed(t, transport.close)
}

func TestWebSocketTransportClosesWithoutResponseReader(t *testing.T) {
	transport, _, peer, _ := newTestWebSocketTransport(t)
	require.NoError(t, peer.WriteMessage(websocket.TextMessage, []byte("unread")))
	_ = transport.Close()
	awaitClosed(t, transport.close)
	awaitClosed(t, transport.Responses())
}

func TestWebSocketTransportClosesWithoutErrorReader(t *testing.T) {
	transport, _, peer, _ := newTestWebSocketTransport(t)
	require.NoError(t, peer.Close())
	awaitClosed(t, transport.close)
	require.Error(t, receive(t, transport.Errors()))
}

func TestTCPTransportStopsWithoutReaders(t *testing.T) {
	for _, failure := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "error"}[failure], func(t *testing.T) {
			conn, peer := net.Pipe()
			transport := &TCPTransport{
				conn:      conn,
				responses: make(chan []byte),
				errors:    make(chan error, 1),
				quit:      make(chan struct{}),
				done:      make(chan struct{}),
			}
			go transport.listen()
			t.Cleanup(func() { _ = transport.Close(); _ = peer.Close() })
			if failure {
				require.NoError(t, peer.Close())
			} else {
				written := asyncCall(func() error { _, err := peer.Write([]byte("unread\n")); return err })
				require.NoError(t, receive(t, written))
				require.NoError(t, transport.Close())
			}
			awaitClosed(t, transport.done)
			awaitClosed(t, transport.Responses())
		})
	}
}

func TestWebSocketClientConcurrentRequestsAndShutdown(t *testing.T) {
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(serverDone)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade failed: %v", err)
			return
		}
		defer conn.Close()
		for {
			var req request
			if err := conn.ReadJSON(&req); err != nil {
				return
			}
			body, _ := json.Marshal(map[string]interface{}{"id": req.ID, "result": nil})
			if err := conn.WriteMessage(websocket.TextMessage, body); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 2*testTimeout)
	defer cancel()
	client, err := NewClientWebSocket(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { client.Shutdown(); awaitClosed(t, serverDone) })
	for _, shutdown := range []bool{false, true} {
		start := make(chan struct{})
		results := make([]<-chan error, 32)
		for i := range results {
			results[i] = asyncCall(func() error {
				<-start
				return client.Ping(ctx)
			})
		}
		close(start)
		if shutdown {
			client.Shutdown()
		}
		for _, result := range results {
			err := receive(t, result)
			if shutdown {
				require.NotErrorIs(t, err, ErrTimeout)
			} else {
				require.NoError(t, err)
			}
		}
	}
}
