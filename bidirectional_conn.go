package cronet

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/pipe"
)

// ErrClosedLocally is what a caller waiting on this stream is told when this
// end closed it, as opposed to anything the network or the far side did.
//
// The distinction has to be carried in the error because it cannot be recovered
// from one. net.ErrClosed will not serve: NetError reports four genuine network
// failures as matching it — see its Is method in net_error.go — and those are
// precisely the ones a caller ranking this proxy must not excuse.
//
// It still wraps net.ErrClosed, because sing tests for that sentinel to keep a
// closed connection quiet everywhere else, and a value that did not match would
// turn every local close into new log lines further out.
//
// Only the three paths that wait on the proxy's answer report it —
// WaitForHeaders, WaitForHeadersContext and WaitReady — because they are the
// only ones a caller weighing this proxy consults. Read and Write keep the
// plain sentinel: nothing ranks on them, and their error reaches operators as
// text, where a suffix nobody needed would break every alert matching the
// message it has always had.
var ErrClosedLocally = fmt.Errorf("%w (closed by this end)", net.ErrClosed)

type BidirectionalConn struct {
	ctx              context.Context
	stream           BidirectionalStream
	logger           logger.ContextLogger
	cancelled        atomic.Bool
	readWaitHeaders  bool
	writeWaitHeaders bool
	access           sync.Mutex
	close            chan struct{}
	done             chan struct{}
	err              error
	ready            chan struct{}
	handshake        chan struct{}
	read             chan int
	write            chan struct{}
	headers          map[string]string
	readSemaphore    chan struct{}
	writeSemaphore   chan struct{}
	readDone         chan struct{}
	writeDone        chan struct{}
	doneOnce         sync.Once
	readDoneOnce     sync.Once
	writeDoneOnce    sync.Once
	onTerminate      func()
	onTraffic        func(int)
	readDeadline     pipe.Deadline
	writeDeadline    pipe.Deadline
	startAt          atomic.Int64
	readyAt          atomic.Int64
	headersAt        atomic.Int64
	payloadSent      atomic.Bool
}

// ConnTiming breaks a stream's latency down into connection establishment and
// the request/response round trip.
//
// Setup covers everything from Start until the request headers reach the wire:
// TCP connect, TLS and the HTTP/2 session handshake. It is near zero when the
// stream lands on a connection that is already established, so it doubles as a
// cold/warm indicator for pooled connections.
//
// RoundTrip runs from the request headers being sent to the response headers
// arriving, and therefore excludes connection establishment entirely.
type ConnTiming struct {
	Setup     time.Duration
	RoundTrip time.Duration
}

func (e StreamEngine) CreateConn(ctx context.Context, l logger.ContextLogger, readWaitHeaders bool, writeWaitHeaders bool) *BidirectionalConn {
	conn := &BidirectionalConn{
		ctx:              ctx,
		logger:           l,
		readWaitHeaders:  readWaitHeaders,
		writeWaitHeaders: writeWaitHeaders,
		close:            make(chan struct{}),
		done:             make(chan struct{}),
		ready:            make(chan struct{}),
		handshake:        make(chan struct{}),
		read:             make(chan int),
		write:            make(chan struct{}),
		readSemaphore:    make(chan struct{}, 1),
		writeSemaphore:   make(chan struct{}, 1),
		readDone:         make(chan struct{}),
		writeDone:        make(chan struct{}),
		readDeadline:     pipe.MakeDeadline(),
		writeDeadline:    pipe.MakeDeadline(),
	}
	conn.readSemaphore <- struct{}{}
	conn.writeSemaphore <- struct{}{}
	conn.stream = e.CreateStream(&bidirectionalHandler{BidirectionalConn: conn})
	return conn
}

func (c *BidirectionalConn) waitReady(waitHeaders bool, deadline <-chan struct{}) error {
	var gate <-chan struct{}
	if waitHeaders {
		gate = c.handshake
	} else {
		gate = c.ready
	}
	select {
	case <-gate:
		return nil
	case <-c.done:
		return c.err
	case <-c.close:
		return net.ErrClosed
	case <-deadline:
		return os.ErrDeadlineExceeded
	}
}

func (c *BidirectionalConn) Start(method string, url string, headers map[string]string, priority int, endOfStream bool) error {
	c.startAt.Store(time.Now().UnixNano())
	c.access.Lock()
	if !c.stream.Start(method, url, headers, priority, endOfStream) {
		c.access.Unlock()
		c.terminate(os.ErrInvalid)
		return os.ErrInvalid
	}
	c.access.Unlock()
	return nil
}

func (c *BidirectionalConn) markTerminatedLocked(err error) (onTerminate func(), marked bool) {
	c.readDoneOnce.Do(func() { close(c.readDone) })
	c.writeDoneOnce.Do(func() { close(c.writeDone) })
	c.cancelled.Store(true)
	c.doneOnce.Do(func() {
		c.err = err
		close(c.done)
		onTerminate = c.onTerminate
		marked = true
	})
	return
}

func (c *BidirectionalConn) terminate(err error) {
	c.access.Lock()
	onTerminate, marked := c.markTerminatedLocked(err)
	c.access.Unlock()

	if onTerminate != nil {
		onTerminate()
	}
	if marked {
		c.stream.Destroy()
		cleanupBidirectionalStream(c.stream.ptr)
	}
}

func (c *BidirectionalConn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	select {
	case <-c.close:
		return 0, net.ErrClosed
	case <-c.done:
		return 0, c.err
	case <-c.readSemaphore:
	}
	defer func() { c.readSemaphore <- struct{}{} }()

	if err := c.waitReady(c.readWaitHeaders, c.readDeadline.Wait()); err != nil {
		return 0, err
	}

	c.access.Lock()
	select {
	case <-c.close:
		c.access.Unlock()
		return 0, net.ErrClosed
	case <-c.done:
		c.access.Unlock()
		return 0, c.err
	default:
	}
	c.stream.Read(p)
	c.access.Unlock()

	select {
	case bytesRead := <-c.read:
		if c.onTraffic != nil {
			c.onTraffic(bytesRead)
		}
		return bytesRead, nil
	case <-c.readDeadline.Wait():
		if c.cancelled.CompareAndSwap(false, true) {
			c.stream.Cancel()
		}
		for {
			select {
			case <-c.read:
			case <-c.done:
				return 0, os.ErrDeadlineExceeded
			}
		}
	case <-c.done:
		<-c.readDone
		return 0, c.err
	case <-c.close:
		<-c.readDone
		return 0, net.ErrClosed
	}
}

func (c *BidirectionalConn) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	select {
	case <-c.close:
		return 0, net.ErrClosed
	case <-c.done:
		return 0, c.err
	case <-c.writeSemaphore:
	}
	defer func() { c.writeSemaphore <- struct{}{} }()

	if err := c.waitReady(c.writeWaitHeaders, c.writeDeadline.Wait()); err != nil {
		return 0, err
	}

	c.access.Lock()
	select {
	case <-c.close:
		c.access.Unlock()
		return 0, net.ErrClosed
	case <-c.done:
		c.access.Unlock()
		return 0, c.err
	default:
	}
	// Recorded under the same lock Close takes, and before the stream sees the
	// bytes: once Close has returned, CarriedPayload is final.
	c.payloadSent.Store(true)
	c.stream.Write(p, false)
	c.access.Unlock()

	select {
	case <-c.write:
		if c.onTraffic != nil {
			c.onTraffic(len(p))
		}
		return len(p), nil
	case <-c.writeDeadline.Wait():
		if c.cancelled.CompareAndSwap(false, true) {
			c.stream.Cancel()
		}
		for {
			select {
			case <-c.write:
			case <-c.done:
				return 0, os.ErrDeadlineExceeded
			}
		}
	case <-c.done:
		<-c.writeDone
		return 0, c.err
	case <-c.close:
		<-c.writeDone
		return 0, net.ErrClosed
	}
}

func (c *BidirectionalConn) Done() <-chan struct{} {
	return c.done
}

// setOnTraffic registers the byte-count hook. This is the terminal conn every
// wrapper unwraps to, so counting here survives the zero-copy paths that
// bypass the wrappers. Set once before Start; the reader and writer goroutines
// read it without a lock afterwards.
func (c *BidirectionalConn) setOnTraffic(fn func(int)) {
	c.onTraffic = fn
}

func (c *BidirectionalConn) setOnTerminate(fn func()) {
	c.access.Lock()
	select {
	case <-c.done:
		c.access.Unlock()
		fn()
		return
	default:
	}
	c.onTerminate = fn
	c.access.Unlock()
}

func (c *BidirectionalConn) Err() error {
	return c.err
}

func (c *BidirectionalConn) Close() error {
	c.access.Lock()

	select {
	case <-c.close:
		c.access.Unlock()
		return net.ErrClosed
	case <-c.done:
		c.access.Unlock()
		return nil
	default:
	}

	close(c.close)
	c.access.Unlock()

	if c.cancelled.CompareAndSwap(false, true) {
		c.stream.Cancel()
	}
	return nil
}

func (c *BidirectionalConn) LocalAddr() net.Addr {
	return nil
}

func (c *BidirectionalConn) RemoteAddr() net.Addr {
	return nil
}

func (c *BidirectionalConn) SetDeadline(t time.Time) error {
	c.SetReadDeadline(t)
	c.SetWriteDeadline(t)
	return nil
}

func (c *BidirectionalConn) SetReadDeadline(t time.Time) error {
	c.readDeadline.Set(t)
	return nil
}

func (c *BidirectionalConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.Set(t)
	return nil
}

// closed reports, without blocking, whether ch has already been closed.
func closed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// A select over channels that are both ready picks at random, and a stream that
// answered and then ended — a probe response followed immediately by the server
// closing the stream — has handshake and done both closed by the time a caller
// descheduled for a moment gets here. The handshake happening is a fact however
// the stream ended afterwards, so it is checked first, non-blocking, before the
// racing select; otherwise a successful answer is reported as the stream's
// terminal error about half the time it arrives late.
func (c *BidirectionalConn) WaitForHeaders() (map[string]string, error) {
	if closed(c.handshake) {
		return c.headers, nil
	}
	select {
	case <-c.handshake:
		return c.headers, nil
	case <-c.done:
		return c.headersIfHandshaken()
	case <-c.close:
		return nil, ErrClosedLocally
	}
}

func (c *BidirectionalConn) WaitForHeadersContext(ctx context.Context) (map[string]string, error) {
	if closed(c.handshake) {
		return c.headers, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.handshake:
		return c.headers, nil
	case <-c.done:
		return c.headersIfHandshaken()
	case <-c.close:
		return nil, ErrClosedLocally
	}
}

// headersIfHandshaken resolves the done branch: the callbacks close handshake
// strictly before the stream terminates, so with done drawn, one non-blocking
// look at handshake says definitively whether an answer arrived first.
func (c *BidirectionalConn) headersIfHandshaken() (map[string]string, error) {
	if closed(c.handshake) {
		return c.headers, nil
	}
	return nil, c.err
}

// WaitReady blocks until the stream is established and its request headers have
// reached the wire, which is after TCP, TLS and the HTTP/2 session are up. It is
// separate from waiting for the response because only this part is bounded by
// the connection to the proxy — the response additionally waits on whatever the
// proxy is doing on the far side.
func (c *BidirectionalConn) WaitReady(ctx context.Context) error {
	// Same non-blocking pre-check as WaitForHeaders: ready having happened is a
	// fact however the stream ended afterwards.
	if closed(c.ready) {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ready:
		return nil
	case <-c.done:
		if closed(c.ready) {
			return nil
		}
		return c.err
	case <-c.close:
		return ErrClosedLocally
	}
}

// CarriedPayload reports whether any payload byte has been handed to the
// stream — the point past which this end can no longer prove the far side saw
// nothing. The request headers alone do not count: a CONNECT that carried no
// payload can be repeated freely.
//
// Write hands bytes to the stream under the same lock Close takes, so once
// Close has returned the answer here is final: no write still blocked on the
// ready gate can slip through afterwards and invalidate it.
func (c *BidirectionalConn) CarriedPayload() bool {
	return c.payloadSent.Load()
}

// Timing reports the stream's setup/round-trip breakdown. It is only meaningful
// once the response headers have arrived, and reports false before that.
func (c *BidirectionalConn) Timing() (ConnTiming, bool) {
	startAt := c.startAt.Load()
	headersAt := c.headersAt.Load()
	if startAt == 0 || headersAt == 0 {
		return ConnTiming{}, false
	}
	readyAt := c.readyAt.Load()
	if readyAt == 0 || readyAt > headersAt {
		// OnStreamReady never fired (or raced past the headers): attribute
		// everything to the round trip rather than reporting a bogus split.
		return ConnTiming{RoundTrip: time.Duration(headersAt - startAt)}, true
	}
	return ConnTiming{
		Setup:     time.Duration(readyAt - startAt),
		RoundTrip: time.Duration(headersAt - readyAt),
	}, true
}

type bidirectionalHandler struct {
	*BidirectionalConn
	readyOnce     sync.Once
	handshakeOnce sync.Once
}

func (c *bidirectionalHandler) OnStreamReady(stream BidirectionalStream) {
	c.readyOnce.Do(func() {
		c.readyAt.Store(time.Now().UnixNano())
		close(c.ready)
	})
}

func (c *bidirectionalHandler) OnResponseHeadersReceived(stream BidirectionalStream, headers map[string]string, negotiatedProtocol string) {
	c.headersAt.Store(time.Now().UnixNano())
	c.headers = headers
	c.logger.DebugContext(c.ctx, "response received, protocol: ", negotiatedProtocol, ", status: ", headers[":status"])
	c.handshakeOnce.Do(func() { close(c.handshake) })
}

func (c *bidirectionalHandler) OnReadCompleted(stream BidirectionalStream, bytesRead int) {
	if bytesRead == 0 {
		c.terminate(io.EOF)
		return
	}

	select {
	case <-c.close:
		c.readDoneOnce.Do(func() { close(c.readDone) })
	case <-c.done:
		c.readDoneOnce.Do(func() { close(c.readDone) })
	case c.read <- bytesRead:
	}
}

func (c *bidirectionalHandler) OnWriteCompleted(stream BidirectionalStream) {
	select {
	case <-c.close:
		c.writeDoneOnce.Do(func() { close(c.writeDone) })
	case <-c.done:
		c.writeDoneOnce.Do(func() { close(c.writeDone) })
	case c.write <- struct{}{}:
	}
}

func (c *bidirectionalHandler) OnResponseTrailersReceived(stream BidirectionalStream, trailers map[string]string) {
}

func (c *bidirectionalHandler) OnSucceeded(stream BidirectionalStream) {
	c.terminate(io.EOF)
}

func (c *bidirectionalHandler) OnFailed(stream BidirectionalStream, netError int) {
	c.logger.WarnContext(c.ctx, "stream failed: ", NetError(netError))
	c.terminate(NetError(netError))
}

func (c *bidirectionalHandler) OnCanceled(stream BidirectionalStream) {
	c.logger.DebugContext(c.ctx, "stream canceled")
	c.terminate(context.Canceled)
}
