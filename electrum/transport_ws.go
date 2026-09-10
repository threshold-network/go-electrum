package electrum

import (
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type WebSocketTransport struct {
	conn      *websocket.Conn
	responses chan []byte
	errors    chan error
	writeLock sync.Mutex
	closeOnce sync.Once
	closeErr  error
	quit      chan struct{}
	// close is a channel used for graceful connection closure
	close chan struct{}
}

const webSocketClosingTimeout = 2 * time.Second

// NewWebSocketTransport initializes new WebSocket transport.
func NewWebSocketTransport(
	ctx context.Context,
	url string,
	tlsConfig *tls.Config,
) (*WebSocketTransport, error) {
	dialer := websocket.Dialer{
		TLSClientConfig: tlsConfig,
	}

	conn, response, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		if DebugMode {
			log.Printf(
				"%s [debug] connect -> status: %v, error: %v",
				time.Now().Format("2006-01-02 15:04:05"),
				response.Status,
				err,
			)
		}
		return nil, err
	}

	ws := &WebSocketTransport{
		conn:      conn,
		responses: make(chan []byte),
		errors:    make(chan error, 1),
		close:     make(chan struct{}),
		quit:      make(chan struct{}),
	}

	go ws.listen()

	return ws, nil
}

func (t *WebSocketTransport) listen() {
	defer close(t.close)
	defer t.conn.Close()
	defer close(t.responses)
	defer close(t.errors)

	for {
		_, msg, err := t.conn.ReadMessage()
		if DebugMode {
			log.Printf(
				"%s [debug] %s -> msg: %s, err: %v",
				time.Now().Format("2006-01-02 15:04:05"),
				t.conn.RemoteAddr(),
				msg,
				err,
			)
		}
		if err != nil {
			isNormalClose := websocket.IsCloseError(err, websocket.CloseNormalClosure)
			if !isNormalClose {
				select {
				case t.errors <- err:
				case <-t.quit:
				}
			}

			break
		}

		select {
		case t.responses <- msg:
		case <-t.quit:
			return
		}
	}
}

// SendMessage sends a message to the remote server through the WebSocket transport.
func (t *WebSocketTransport) SendMessage(body []byte) error {
	// Gorilla permits only one data writer per connection.
	t.writeLock.Lock()
	defer t.writeLock.Unlock()

	select {
	case <-t.quit:
		return ErrServerShutdown
	case <-t.close:
		return ErrServerShutdown
	default:
	}

	if DebugMode {
		log.Printf("%s [debug] %s <- %s", time.Now().Format("2006-01-02 15:04:05"), t.conn.RemoteAddr(), body)
	}

	return t.conn.WriteMessage(websocket.TextMessage, body)
}

// Responses returns chan to WebSocket transport responses.
func (t *WebSocketTransport) Responses() <-chan []byte {
	return t.responses
}

// Errors returns chan to WebSocket transport errors.
func (t *WebSocketTransport) Errors() <-chan error {
	return t.errors
}

// Close closes WebSocket transport.
func (t *WebSocketTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.quit)
		deadline := time.Now().Add(webSocketClosingTimeout)
		// WriteControl is safe alongside WriteMessage. Its deadline also bounds
		// waiting for an in-flight data write, so shutdown can unblock it.
		err := t.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
		if err != nil {
			t.closeErr = t.conn.Close()
			return
		}

		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-t.close:
		case <-timer.C:
			t.closeErr = t.conn.Close()
		}
	})

	return t.closeErr
}
