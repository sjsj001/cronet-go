//go:build with_purego

package cronet

import (
	"context"
	"testing"

	"github.com/sagernet/sing/common/logger"
)

type countingLogger struct {
	logger.ContextLogger
	warn, debug int
}

func (l *countingLogger) WarnContext(context.Context, ...any)  { l.warn++ }
func (l *countingLogger) DebugContext(context.Context, ...any) { l.debug++ }

// newWaitingConn is a stream that has not answered yet. None of the paths
// under test touch the native stream.
func newWaitingConn(l logger.ContextLogger) (*BidirectionalConn, *naiveConn) {
	conn := &BidirectionalConn{
		logger:    l,
		close:     make(chan struct{}),
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
		handshake: make(chan struct{}),
		readDone:  make(chan struct{}),
		writeDone: make(chan struct{}),
	}
	return conn, &naiveConn{Conn: conn, ctx: context.Background(), conn: conn, logger: l}
}

// Every dial is measured twice, so the same answer must not be reported twice.
func TestAHandshakeOutcomeIsReportedOncePerConnection(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		l := &countingLogger{ContextLogger: logger.NOP()}
		conn, naive := newWaitingConn(l)
		conn.headers = map[string]string{":status": "502"}
		close(conn.handshake)
		for range 2 {
			if err := naive.HandshakeContext(context.Background()); err == nil {
				t.Fatal("a 502 handshake succeeded")
			}
		}
		if l.warn != 1 {
			t.Fatalf("got %d warnings for one 502, want 1", l.warn)
		}
	})
	t.Run("stream failed", func(t *testing.T) {
		l := &countingLogger{ContextLogger: logger.NOP()}
		conn, naive := newWaitingConn(l)
		conn.err = NetErrorConnectionReset
		close(conn.done)
		for range 2 {
			if err := naive.Handshake(); err != NetErrorConnectionReset {
				t.Fatalf("got %v, want %v", err, NetErrorConnectionReset)
			}
		}
		if l.warn != 1 {
			t.Fatalf("got %d warnings for one failed stream, want 1", l.warn)
		}
	})
	t.Run("succeeded", func(t *testing.T) {
		l := &countingLogger{ContextLogger: logger.NOP()}
		conn, naive := newWaitingConn(l)
		conn.headers = map[string]string{":status": "200"}
		close(conn.handshake)
		for range 2 {
			if err := naive.Handshake(); err != nil {
				t.Fatal(err)
			}
		}
		if l.debug != 1 {
			t.Fatalf("got %d lines for one success, want 1", l.debug)
		}
	})
}

// A caller that stops waiting has not seen the stream's answer, so it must not
// use up the one report of it.
func TestAnAbandonedWaitLeavesTheAnswerToBeReported(t *testing.T) {
	l := &countingLogger{ContextLogger: logger.NOP()}
	conn, naive := newWaitingConn(l)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := naive.HandshakeContext(cancelled); err != context.Canceled {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	conn.headers = map[string]string{":status": "502"}
	close(conn.handshake)
	if err := naive.HandshakeContext(context.Background()); err == nil {
		t.Fatal("a 502 handshake succeeded")
	}
	if l.debug != 1 || l.warn != 1 {
		t.Fatalf("got debug=%d warn=%d, want 1 and 1", l.debug, l.warn)
	}
}

// Only this end produces these two: a closing engine and Chromium's own
// network monitor.
func TestALocalAbortIsNoHandshakeFailure(t *testing.T) {
	for _, code := range []NetError{NetErrorAborted, NetErrorNetworkChanged} {
		l := &countingLogger{ContextLogger: logger.NOP()}
		conn, naive := newWaitingConn(l)
		conn.err = code
		close(conn.done)
		if err := naive.Handshake(); err != code {
			t.Fatalf("got %v, want %v", err, code)
		}
		if l.warn != 0 {
			t.Fatalf("%s: got %d warnings, want 0", code.Name(), l.warn)
		}
	}
}

// The stream's error reaches whoever waits on it, which reports it.
func TestAFailedStreamIsNotAWarningOfItsOwn(t *testing.T) {
	l := &countingLogger{ContextLogger: logger.NOP()}
	conn, _ := newWaitingConn(l)
	// Already terminated, so terminate below leaves the null native stream
	// alone.
	conn.doneOnce.Do(func() {})
	(&bidirectionalHandler{BidirectionalConn: conn}).OnFailed(BidirectionalStream{}, int(NetErrorConnectionReset))
	if l.warn != 0 {
		t.Fatalf("got %d warnings, want 0", l.warn)
	}
}

// Once the tunnel is up, the copy loop reading it logs a reset at Trace, so
// OnFailed is the only report — except for the two errors only this end makes.
func TestAFailedTunnelIsAWarningUnlessThisEndClosedIt(t *testing.T) {
	for code, want := range map[NetError]int{
		NetErrorConnectionReset: 1,
		NetErrorAborted:         0,
		NetErrorNetworkChanged:  0,
		// The copy loop logs these at ERROR already.
		NetErrorHTTP2PingFailed:    0,
		NetErrorHTTP2ProtocolError: 0,
	} {
		l := &countingLogger{ContextLogger: logger.NOP()}
		conn, _ := newWaitingConn(l)
		close(conn.handshake)
		conn.doneOnce.Do(func() {})
		(&bidirectionalHandler{BidirectionalConn: conn}).OnFailed(BidirectionalStream{}, int(code))
		if l.warn != want {
			t.Fatalf("%s: got %d warnings, want %d", code.Name(), l.warn, want)
		}
	}
}
