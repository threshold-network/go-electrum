package electrum

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

// TestWebSocketTransportConcurrentSendMessage checks that many goroutines can
// send on one transport at the same time. Without write serialization the
// gorilla/websocket connection panics with "concurrent write to websocket
// connection".
func TestWebSocketTransportConcurrentSendMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	url := "ws" + strings.TrimPrefix(server.URL, "http")
	transport, err := NewWebSocketTransport(context.Background(), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	const senders = 32
	const messagesPerSender = 200
	payload := []byte(strings.Repeat("x", 4096))

	var wg sync.WaitGroup
	errs := make(chan error, senders*messagesPerSender)
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < messagesPerSender; j++ {
				if err := transport.SendMessage(payload); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("send failed: %v", err)
	}
}
