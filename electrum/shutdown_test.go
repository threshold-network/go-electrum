package electrum

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// startSilentServer accepts TCP connections, reads requests and never
// answers. It returns the server address.
func startSilentServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var connsMutex sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		_ = listener.Close()
		connsMutex.Lock()
		defer connsMutex.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connsMutex.Lock()
			conns = append(conns, conn)
			connsMutex.Unlock()
			go func() {
				reader := bufio.NewReader(conn)
				for {
					if _, err := reader.ReadBytes(nl); err != nil {
						return
					}
				}
			}()
		}
	}()

	return listener.Addr().String()
}

// Callers reconnect by calling Shutdown and creating a new client, so
// Shutdown must be safe while the client's goroutines run and while other
// goroutines use the client. Run with -race.
func TestShutdownWhileInUse(t *testing.T) {
	addr := startSilentServer(t)

	for i := 0; i < 20; i++ {
		client, err := NewClientTCP(context.Background(), addr)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				_ = client.Ping(ctx)
			}()
		}
		wg.Add(2)
		go func() { defer wg.Done(); client.Shutdown() }()
		go func() { defer wg.Done(); client.Shutdown() }()
		wg.Wait()

		if !client.IsShutdown() {
			t.Fatal("expected the client to be shut down")
		}
		if err := client.Ping(context.Background()); err != ErrServerShutdown {
			t.Fatalf("expected [%v] after shutdown, got [%v]", ErrServerShutdown, err)
		}
	}
}

// Every goroutine a client starts must end after Shutdown. A caller that
// reconnects on each failure would otherwise leak goroutines for as long as
// the server misbehaves.
func TestShutdownReleasesGoroutines(t *testing.T) {
	addr := startSilentServer(t)

	settle := func() int {
		deadline := time.Now().Add(2 * time.Second)
		n := runtime.NumGoroutine()
		for time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			if m := runtime.NumGoroutine(); m == n {
				return m
			} else {
				n = m
			}
		}
		return n
	}

	// One connection first, so the server's own goroutines are in the
	// baseline.
	warmup, err := NewClientTCP(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	warmup.Shutdown()
	before := settle()

	const clients = 20
	for i := 0; i < clients; i++ {
		client, err := NewClientTCP(context.Background(), addr)
		if err != nil {
			t.Fatal(err)
		}
		client.Shutdown()
	}

	// Each closed server-side reader ends once its client closes; allow a
	// small slack for scheduling.
	if leaked := settle() - before; leaked > 2 {
		t.Fatalf("expected the goroutines of [%d] shut-down clients to end, [%d] still running", clients, leaked)
	}
}
