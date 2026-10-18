package cronet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/pipe"
)

// ErrClosedLocally is what a caller waiting on this stream is told when this
// end closed it, as opposed to anything the network or the far side did.
//
// The distinction has to be carried in the error because it cannot be recovered
// from one. net.ErrClosed will not serve: NetError reports six errors as
// matching it — see its Is method in net_error.go — and while two of them only
// this end produces, the other four are genuine network failures, precisely
// the ones a caller ranking this proxy must not excuse.
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

// errWriteClosed is what a write after CloseWrite gets. It never reaches the
// stream: data after end of stream makes the native side fail the whole
// stream, taking the read direction down with it.
var errWriteClosed = fmt.Errorf("%w: write side closed", net.ErrClosed)

// bulkBufferSize is the read-ahead buffer and the write staging limit, and the
// largest size sing's buffer pool serves.
//
// Every Read and Write is a synchronous round trip through Cronet's network
// thread: a C call, a posted task, then a completion callback that blocks that
// thread until a goroutine takes it. A caller doing 64 KiB I/O pays about 2.2 to
// 2.5 CPU seconds per GB for that; behind a snell inbound, whose writer MTU is
// 16383, every Read and Write is one 16 KiB record and the same tunnel cost 5.25
// s/GB down and 9.3 s/GB up (2-core box, loopback). Reading ahead into 64 KiB and
// coalescing writes up to 64 KiB measured 2.68 s/GB down and 3.05 s/GB up, 88 to
// 197 and 49 to 139 MB/s.
const bulkBufferSize = 64 * 1024

// smallReadSize: a read this small is a header, such as a UoT frame's address
// and length, and what follows it has usually arrived with it. Read straight
// into the caller's buffer, every sparse datagram took two round trips.
const smallReadSize = 512

// smallReadAheadSize is the read-ahead a small read takes on a stream that is
// not bulk. The buffer is taken when the read is posted, so an idle UoT
// session holds it for as long as it waits: 2 KiB holds a whole QUIC datagram
// with its frame, and 200 idle sessions held 0.4 MB more than without the
// small read-ahead, against 3.3 MB at 16 KiB.
const smallReadAheadSize = 2 * 1024

// nativeStream is what a BidirectionalConn drives of its BidirectionalStream.
// It is an interface only so that tests can stand in for the network thread.
type nativeStream interface {
	Start(method string, url string, headers map[string]string, priority int, endOfStream bool) bool
	Read(buffer []byte) int
	Write(buffer []byte, endOfStream bool) int
	WriteEndOfStream() int
	Cancel()
	destroy()
}

type BidirectionalConn struct {
	ctx              context.Context
	stream           nativeStream
	logger           logger.ContextLogger
	cancelled        bool
	writeClosed      bool
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
	onReady          func()
	onTraffic        func(int)
	readDeadline     pipe.Deadline
	writeDeadline    pipe.Deadline
	startAt          atomic.Int64
	readyAt          atomic.Int64
	headersAt        atomic.Int64
	payloadSent      atomic.Bool

	// Read-ahead. readBulk belongs to the Read holding readSemaphore; the rest
	// is under access, because Close and terminate return the buffer too. While
	// readPending a stream read targets readBuffer and only that Read may touch
	// it: Chromium writes into it until OnReadCompleted.
	readBulk    bool
	readPending bool
	readBuffer  []byte
	readStart   int
	readEnd     int

	// Write-behind, under access. writeBuffer holds bytes Write accepted and no
	// stream write has carried yet; flushing says a flusher goroutine is running,
	// which it always is while writeBuffer is non-nil. writeWake is how the
	// flusher tells a waiting Write or CloseWrite to look again.
	writeBuffer []byte
	flushing    bool
	writeWake   chan struct{}
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
	conn := newBidirectionalConn(ctx, l, readWaitHeaders, writeWaitHeaders)
	conn.stream = e.CreateStream(&bidirectionalHandler{BidirectionalConn: conn})
	return conn
}

func newBidirectionalConn(ctx context.Context, l logger.ContextLogger, readWaitHeaders bool, writeWaitHeaders bool) *BidirectionalConn {
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
		writeWake:        make(chan struct{}, 1),
	}
	conn.readSemaphore <- struct{}{}
	conn.writeSemaphore <- struct{}{}
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

// monoBase anchors the timing stamps to the monotonic clock. They were wall
// clock UnixNano, and a step back while a stream awaited its answer (chrony
// at boot, when every member is cold and warming) shrank the measured local
// leg to near nothing, for as many samples as the window holds. The stamps
// are private and only ever subtracted, so nothing needs them as dates.
var monoBase = time.Now()

// monoStamp is nanoseconds since monoBase, plus one: zero means not set.
func monoStamp() int64 {
	return int64(time.Since(monoBase)) + 1
}

func (c *BidirectionalConn) Start(method string, url string, headers map[string]string, priority int, endOfStream bool) error {
	c.startAt.Store(monoStamp())
	c.access.Lock()
	if !c.stream.Start(method, url, headers, priority, endOfStream) {
		c.access.Unlock()
		c.terminate(os.ErrInvalid)
		return os.ErrInvalid
	}
	c.access.Unlock()
	return nil
}

func (c *BidirectionalConn) cancelLocked() {
	if c.cancelled {
		return
	}
	c.cancelled = true
	c.stream.Cancel()
}

func (c *BidirectionalConn) terminate(err error) {
	var onTerminate func()
	c.access.Lock()
	c.readDoneOnce.Do(func() { close(c.readDone) })
	c.writeDoneOnce.Do(func() { close(c.writeDone) })
	c.cancelled = true
	c.doneOnce.Do(func() {
		c.err = err
		close(c.done)
		onTerminate = c.onTerminate
		c.stream.destroy()
	})
	// Unread bytes stay for the reader: they arrived before the stream ended.
	if !c.readPending && c.readStart == c.readEnd {
		c.releaseReadAheadLocked()
	}
	c.access.Unlock()

	if onTerminate != nil {
		onTerminate()
	}
}

func (c *BidirectionalConn) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Not on done: bytes read ahead before the stream ended are still the
	// caller's, as they would have been had they gone straight into p.
	select {
	case <-c.close:
		return 0, net.ErrClosed
	case <-c.readSemaphore:
	}
	defer func() { c.readSemaphore <- struct{}{} }()

	c.access.Lock()
	if c.readStart < c.readEnd && !closed(c.close) {
		n = c.takeReadAheadLocked(p)
		c.access.Unlock()
		return n, nil
	}
	c.access.Unlock()

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
	// A bulk stream reads ahead into its own buffer when the caller's is
	// smaller, so one round trip serves several Reads. So does a small read,
	// into a smallReadAheadSize buffer unless bulk: an idle UoT session waits
	// in one.
	ahead := len(p) < smallReadSize || c.readBulk && len(p) < bulkBufferSize
	target := p
	if ahead {
		size := smallReadAheadSize
		if c.readBulk {
			size = bulkBufferSize
		}
		// A small read-ahead that filled its buffer is still held, drained:
		// keeping it would pin a bulk stream to small reads.
		if cap(c.readBuffer) < size {
			c.releaseReadAheadLocked()
			c.readBuffer = buf.Get(size)
		}
		// A whole number of the caller's reads: behind a snell inbound, 65536
		// bytes left a 4-byte tail that went out as a record of its own.
		target = c.readBuffer[:len(c.readBuffer)/len(p)*len(p)]
		c.readPending = true
	} else {
		c.releaseReadAheadLocked()
	}
	c.stream.Read(target)
	c.access.Unlock()

	select {
	case bytesRead := <-c.read:
		if c.onTraffic != nil {
			c.onTraffic(bytesRead)
		}
		// Bulk while every round trip fills the buffer it was given: the
		// caller's for a direct read, the whole target for a read-ahead. One
		// that comes up short means the stream has caught up with the network,
		// as the last read of every burst does: it goes back to reading straight
		// into the caller's buffer, or into a small read-ahead, while it
		// waits.
		// Counting a read-ahead as bulk once it covered the caller's buffer
		// left most streams idle after a burst parked on 64 KiB: on the
		// 2-core box, 200 connections idle after a 2 MB download each kept
		// the core's live heap at 27 MB, against 15 MB with this rule or
		// without read-ahead, at the same throughput and CPU (snell to naive,
		// 1.68-1.71 vs 1.69-1.73 s/GB down at about 370 MB/s, 2.06-2.08 vs
		// 2.03-2.10 s/GB up).
		c.readBulk = bytesRead == len(target)
		if !ahead {
			return bytesRead, nil
		}
		c.access.Lock()
		c.readPending = false
		c.readStart, c.readEnd = 0, bytesRead
		n = c.takeReadAheadLocked(p)
		c.access.Unlock()
		return n, nil
	case <-c.readDeadline.Wait():
		c.access.Lock()
		c.cancelLocked()
		c.access.Unlock()
		for {
			select {
			case <-c.read:
			case <-c.done:
				c.abandonReadAhead(ahead)
				return 0, os.ErrDeadlineExceeded
			}
		}
	case <-c.done:
		<-c.readDone
		c.abandonReadAhead(ahead)
		return 0, c.err
	case <-c.close:
		<-c.readDone
		c.abandonReadAhead(ahead)
		return 0, net.ErrClosed
	}
}

// takeReadAheadLocked serves p from the read-ahead buffer. The buffer goes back
// to the pool once drained unless the stream is still bulk and open, and at
// once after Close, whose own release a read in flight held off.
func (c *BidirectionalConn) takeReadAheadLocked(p []byte) int {
	n := copy(p, c.readBuffer[c.readStart:c.readEnd])
	c.readStart += n
	drained := c.readStart == c.readEnd
	if closed(c.close) || drained && (!c.readBulk || closed(c.done)) {
		c.releaseReadAheadLocked()
	}
	return n
}

// releaseReadAheadLocked returns the read-ahead buffer to the pool, unread
// bytes included. No stream read may target it.
func (c *BidirectionalConn) releaseReadAheadLocked() {
	if c.readBuffer != nil {
		buf.Put(c.readBuffer)
		c.readBuffer = nil
	}
	c.readStart, c.readEnd = 0, 0
}

// abandonReadAhead drops the read-ahead buffer after a Read gave up waiting on
// its stream read. If that read targeted the buffer, it does not go back to the
// pool but to the GC: the stream ended without completing the read, and when
// it ends by failing Chromium deletes the native stream in a posted task, so
// nothing here proves it is done with the buffer. Handing that memory to
// another stream could leak one tunnel's bytes into another; leaving it to the
// GC costs one allocation per stream that ends mid-read.
func (c *BidirectionalConn) abandonReadAhead(pending bool) {
	c.access.Lock()
	if pending {
		c.readPending = false
		c.readBuffer = nil
	}
	c.releaseReadAheadLocked()
	c.access.Unlock()
}

// Write stages p for the flusher and returns, unless p is larger than the
// staging buffer. Writes that arrive while a stream write is in flight are
// coalesced into the next one, so a 16 KiB writer makes a quarter of the round
// trips. The staging limit is the back-pressure: a Write that does not fit
// waits for the flusher to take what is staged, so at most two buffers are
// held, one staged and one in flight.
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

	for {
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
		if c.writeClosed {
			c.access.Unlock()
			return 0, errWriteClosed
		}
		if len(p) > bulkBufferSize {
			// Too large to stage: it goes to the stream uncopied, once
			// everything staged before it has.
			if !c.flushing {
				break
			}
		} else if len(c.writeBuffer)+len(p) <= bulkBufferSize {
			if c.writeBuffer == nil {
				c.writeBuffer = buf.Get(bulkBufferSize)[:0]
			}
			c.writeBuffer = append(c.writeBuffer, p...)
			if !c.flushing {
				c.flushing = true
				go c.flush()
			}
			c.access.Unlock()
			return len(p), nil
		}
		c.access.Unlock()
		if err := c.waitFlusher(); err != nil {
			return 0, err
		}
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
		c.access.Lock()
		c.cancelLocked()
		c.access.Unlock()
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

// flush carries staged bytes to the stream, one write at a time, while Write
// stages the next. The Write that stages into an idle conn starts it, and it
// exits when a write completes with nothing new staged, so an idle stream has
// neither this goroutine nor a buffer. Only it takes c.write while it runs:
// direct writes and CloseWrite wait for it to exit first.
//
// The checks and the payloadSent store happen under access right before the
// stream write, as for a direct write. Bytes still staged at Close or
// termination are dropped unwritten, so CarriedPayload stays a statement about
// what the stream was given rather than what Write accepted.
func (c *BidirectionalConn) flush() {
	for {
		c.access.Lock()
		buffer := c.writeBuffer
		c.writeBuffer = nil
		if buffer == nil || c.writeClosed || closed(c.close) || closed(c.done) {
			c.flushing = false
			c.access.Unlock()
			if buffer != nil {
				// Never handed to the stream, so nothing else holds it.
				buf.Put(buffer)
			}
			c.wakeWriter()
			return
		}
		c.payloadSent.Store(true)
		c.stream.Write(buffer, false)
		c.access.Unlock()
		c.wakeWriter()

		select {
		case <-c.write:
			if c.onTraffic != nil {
				c.onTraffic(len(buffer))
			}
			buf.Put(buffer)
		// Not back to the pool on these two, for the reason in
		// abandonReadAhead: the write never completed.
		case <-c.done:
			<-c.writeDone
		case <-c.close:
			<-c.writeDone
		}
	}
}

// waitFlusher waits until the flusher has taken what is staged or exited. The
// write deadline bounds it like any other wait in Write: it cancels the stream,
// so nothing staged is left half written.
func (c *BidirectionalConn) waitFlusher() error {
	select {
	case <-c.writeWake:
		return nil
	case <-c.writeDeadline.Wait():
		c.access.Lock()
		c.cancelLocked()
		c.access.Unlock()
		<-c.done
		return os.ErrDeadlineExceeded
	case <-c.done:
		return c.err
	case <-c.close:
		return net.ErrClosed
	}
}

// wakeWriter tells a Write or CloseWrite in waitFlusher to look again. One
// pending signal is enough: only the holder of writeSemaphore waits, and it
// rechecks under access after every wake.
func (c *BidirectionalConn) wakeWriter() {
	select {
	case c.writeWake <- struct{}{}:
	default:
	}
}

// CloseWrite half-closes the stream: end of stream goes out after everything
// already written, and the response keeps arriving until the far side ends it.
// Writes after it fail without reaching the stream. Calling it again is a no-op.
func (c *BidirectionalConn) CloseWrite() error {
	select {
	case <-c.close:
		return net.ErrClosed
	case <-c.done:
		return c.err
	case <-c.writeSemaphore:
	}
	defer func() { c.writeSemaphore <- struct{}{} }()

	if err := c.waitReady(c.writeWaitHeaders, c.writeDeadline.Wait()); err != nil {
		return err
	}

	for {
		c.access.Lock()
		if c.writeClosed {
			c.access.Unlock()
			return nil
		}
		select {
		case <-c.close:
			c.access.Unlock()
			return net.ErrClosed
		case <-c.done:
			c.access.Unlock()
			return c.err
		default:
		}
		// Everything staged goes first, and its last write must have
		// completed: end of stream is a write of its own.
		if !c.flushing {
			break
		}
		c.access.Unlock()
		if err := c.waitFlusher(); err != nil {
			return err
		}
	}
	c.writeClosed = true
	c.stream.WriteEndOfStream()
	c.access.Unlock()

	// The empty write completes like any other, and its completion has to be
	// taken here: the callback blocks the network thread until someone does.
	select {
	case <-c.write:
		return nil
	case <-c.writeDeadline.Wait():
		c.access.Lock()
		c.cancelLocked()
		c.access.Unlock()
		for {
			select {
			case <-c.write:
			case <-c.done:
				return os.ErrDeadlineExceeded
			}
		}
	case <-c.done:
		<-c.writeDone
		return c.err
	case <-c.close:
		<-c.writeDone
		return net.ErrClosed
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

// setOnReady registers fn to run once the request reaches the wire. Set once
// before Start.
func (c *BidirectionalConn) setOnReady(fn func()) {
	c.onReady = fn
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

// failedToConnect reports whether the stream ended, before its request
// reached the wire, on the error of a failed connection attempt: it never had
// a session to ride.
func (c *BidirectionalConn) failedToConnect() bool {
	select {
	case <-c.done:
	default:
		return false
	}
	if closed(c.ready) {
		return false
	}
	return errors.Is(c.err, NetErrorConnectionTimedOut) ||
		errors.Is(c.err, NetErrorTimedOut) ||
		errors.Is(c.err, NetErrorConnectionRefused) ||
		errors.Is(c.err, NetErrorConnectionFailed) ||
		errors.Is(c.err, NetErrorAddressUnreachable)
}

func (c *BidirectionalConn) Err() error {
	return c.err
}

// Close drops whatever Write staged and the stream has not been given yet: see
// CarriedPayload. A caller that wants its last bytes delivered half-closes
// first; CloseWrite waits for them.
func (c *BidirectionalConn) Close() error {
	c.access.Lock()
	// Bytes read ahead are nobody's once the caller has closed. A read in
	// flight holds the buffer; takeReadAheadLocked releases it after.
	if !c.readPending {
		c.releaseReadAheadLocked()
	}

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
	c.cancelLocked()
	c.access.Unlock()
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
// Bytes reach the stream — from Write for a large write, from the flusher for
// staged ones — under the same lock Close takes, after checking it, so once
// Close has returned the answer here is final: no write still blocked on the
// ready gate can slip through afterwards and invalidate it, and bytes Write
// staged but the stream was never given are dropped and do not count.
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
		c.readyAt.Store(monoStamp())
		close(c.ready)
		if c.onReady != nil {
			c.onReady()
		}
	})
}

func (c *bidirectionalHandler) OnResponseHeadersReceived(stream BidirectionalStream, headers map[string]string, negotiatedProtocol string) {
	c.headersAt.Store(monoStamp())
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
	err := NetError(netError)
	// Before the answer, whoever waits on the handshake is told this error
	// and reports it once, so Debug. After it the reader is a copy loop,
	// which logs anything matching net.ErrClosed or a deadline — a reset or
	// closed tunnel among them — at Trace, so for those this line is the only
	// report; the rest it logs at ERROR itself. ERR_ABORTED and
	// ERR_NETWORK_CHANGED are this end closing the stream and stay Debug.
	quiet := errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded)
	if closed(c.handshake) && quiet && err != NetErrorAborted && err != NetErrorNetworkChanged {
		c.logger.WarnContext(c.ctx, "stream failed: ", err)
	} else {
		c.logger.DebugContext(c.ctx, "stream failed: ", err)
	}
	c.terminate(err)
}

func (c *bidirectionalHandler) OnCanceled(stream BidirectionalStream) {
	c.logger.DebugContext(c.ctx, "stream canceled")
	c.terminate(context.Canceled)
}
