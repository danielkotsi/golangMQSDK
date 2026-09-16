package gomqSDK

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

type fakeBroker struct {
	ln     net.Listener
	conn   net.Conn
	wmu    sync.Mutex
	r      *bufio.Reader
	w      *bufio.Writer
	startC chan struct{}
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{ln: ln, startC: make(chan struct{}, 1)}
	go fb.accept(t)
	return fb
}

func (fb *fakeBroker) accept(t *testing.T) {
	conn, err := fb.ln.Accept()
	if err != nil {
		return
	}
	fb.conn = conn
	fb.r = bufio.NewReader(conn)
	fb.w = bufio.NewWriter(conn)

	line, _ := fb.r.ReadBytes('\n')
	if string(line) != protocol.ProtocolHeader+"\n" {
		panic("bad header: " + string(line))
	}
	fb.send(protocol.NewConnectionStart())
	readJSON(t, fb.r, &protocol.ConnectionStartOK{})
	fb.send(protocol.NewConnectionTune(0, 0, 0))
	readJSON(t, fb.r, &protocol.ConnectionTuneOK{})
	readJSON(t, fb.r, &protocol.ConnectionOpen{})
	fb.send(protocol.NewConnectionOpenOK())
	fb.startC <- struct{}{}
}

func (fb *fakeBroker) send(data any) {
	b, _ := json.Marshal(data)
	fb.wmu.Lock()
	defer fb.wmu.Unlock()
	_, err := fb.w.Write(append(b, '\n'))
	if err != nil {
		panic(err)
	}
	fb.w.Flush()
}

func (fb *fakeBroker) addr() string { return fb.ln.Addr().String() }

func (fb *fakeBroker) close() {
	fb.ln.Close()
	if fb.conn != nil {
		fb.conn.Close()
	}
}

func readJSON(t *testing.T, r *bufio.Reader, dst any) {
	t.Helper()
	line, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := json.Unmarshal(line, dst); err != nil {
		t.Fatalf("unmarshal %q: %v", line, err)
	}
}

func connectTestClient(t *testing.T, fb *fakeBroker) *Client {
	t.Helper()
	c, err := Connect(fb.addr(), Config{ClientName: "smoke"})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestClientCloseIdempotentAndPendingDrain(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	c := connectTestClient(t, fb)

	// Open a channel and let the broker reply.
	go func() {
		fb.send(protocol.Envelope{
			ChannelID: 1, RequestID: 1, Type: protocol.ChannelOpenOKType,
			Payload: json.RawMessage(`{"id":1}`),
		})
	}()
	ctx := context.Background()
	ch, err := c.OpenChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Block a DeclareQueue that the broker never answers.
	errCh := make(chan error, 1)
	go func() {
		_, err := ch.DeclareQueue("q", ctx, "", "")
		errCh <- err
	}()

	// Close the client, then close it again.
	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	// The blocked DeclareQueue must unblock with an error, not hang.
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error from blocked DeclareQueue")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked DeclareQueue hung after Close")
	}
}

func TestClientConcurrentClose(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	c := connectTestClient(t, fb)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = c.Close()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close #%d: %v", i, err)
		}
	}
}

func TestClientChannelClose(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	c := connectTestClient(t, fb)
	ctx := context.Background()

	// Open a channel; broker replies channel.open-ok {id:1}
	fb.send(prepareEnvelope(t, 0, 1, protocol.ChannelOpenOKType, protocol.ChannelOpenOK{ID: 1}))
	ch, err := c.OpenChannel(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := c.channels[ch.id]; !ok {
		t.Fatal("channel not registered")
	}

	// The channel.open envelope is already in the broker buffer (OpenChannel
	// completed), so drain it before watching for channel.close.
	if _, err := fb.r.ReadBytes('\n'); err != nil {
		t.Fatalf("drain channel.open: %v", err)
	}

	// Broker acknowledges channel.close once it receives it. A read deadline
	// guarantees the goroutine cannot block forever if the connection is torn
	// down before the envelope arrives.
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		fb.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := fb.r.ReadBytes('\n')
		if err != nil {
			return
		}
		var env protocol.Envelope
		if err := json.Unmarshal(line, &env); err != nil {
			return
		}
		fb.send(prepareEnvelope(t, ch.id, env.RequestID, protocol.ChannelCloseOKType, protocol.ChannelCloseOK{ID: 1}))
	}()

	if err := ch.Close(ctx); err == nil {
		t.Fatal("ch.Close: expected channel.close-ok to surface a 'channel closed' error, got nil")
	}

	select {
	case <-ackDone:
	case <-time.After(3 * time.Second):
		t.Fatal("broker ack goroutine did not finish")
	}

	c.mu.Lock()
	_, stillThere := c.channels[ch.id]
	c.mu.Unlock()
	if stillThere {
		t.Fatal("channel still registered after Close")
	}
}

// TestOpenChannelExpiredContextReturnsDeadlineExceeded verifies that an
// unanswered channel.open with a timed-out context returns
// context.DeadlineExceeded without a nil-pointer panic (review:
// docs/reviews/BUG_OpenChannel_timeout_nil_pointer.md).
func TestOpenChannelExpiredContextReturnsDeadlineExceeded(t *testing.T) {
	fb := newFakeBroker(t)
	defer fb.close()

	c := connectTestClient(t, fb)
	defer c.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	ch, err := c.OpenChannel(ctx)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if ch != nil {
		t.Fatal("expected nil channel on timeout")
	}

	c.mu.Lock()
	n := len(c.channels)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("expected no leftover channels after timeout, got %d", n)
	}
}

func prepareEnvelope(t *testing.T, channelID, reqID uint16, typ protocol.Method, payload any) protocol.Envelope {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{ChannelID: channelID, RequestID: reqID, Type: typ, Payload: b}
}
