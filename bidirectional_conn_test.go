//go:build with_purego

package cronet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/logger"
	"github.com/sagernet/sing/common/pipe"
)

// The default build cannot link a test binary in this package, so this runs
// with -tags with_purego. Neither path under test may reach the stream, which
// here is a null one: touching it fails the test.
func TestWriteAfterCloseWriteNeverReachesTheStream(t *testing.T) {
	conn := &BidirectionalConn{
		close:          make(chan struct{}),
		done:           make(chan struct{}),
		ready:          make(chan struct{}),
		handshake:      make(chan struct{}),
		writeSemaphore: make(chan struct{}, 1),
		writeDeadline:  pipe.MakeDeadline(),
		writeClosed:    true,
	}
	close(conn.ready)
	close(conn.handshake)
	conn.writeSemaphore <- struct{}{}

	if _, err := conn.Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write after CloseWrite: got %v, want net.ErrClosed", err)
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatalf("second CloseWrite: %v", err)
	}
}

// fakeStream stands in for the native stream and Cronet's network thread:
// every call is a task on one goroutine, and its callbacks block that goroutine
// until the conn takes them, as the real ones block the network thread.
type fakeStream struct {
	t       *testing.T
	handler *bidirectionalHandler
	tasks   chan func()
	quit    chan struct{}
	exited  chan struct{}

	// Network thread only.
	incoming   []byte
	eof        bool
	readBuffer []byte
	hold       bool
	pending    []fakeWrite

	mu         sync.Mutex
	destroyed  bool
	readSizes  []int
	writeSizes []int
	writeAddrs []*byte
	written    bytes.Buffer
	eosAt      int
}

type fakeWrite struct {
	buffer   []byte
	snapshot []byte
	eos      bool
}

func newFakeConn(t *testing.T) (*BidirectionalConn, *fakeStream) {
	base := runtime.NumGoroutine()
	conn := newBidirectionalConn(context.Background(), logger.NOP(), false, false)
	s := &fakeStream{
		t:       t,
		handler: &bidirectionalHandler{BidirectionalConn: conn},
		tasks:   make(chan func(), 1024),
		quit:    make(chan struct{}),
		exited:  make(chan struct{}),
		eosAt:   -1,
	}
	conn.stream = s
	go s.run()
	s.handler.OnStreamReady(BidirectionalStream{})
	s.handler.OnResponseHeadersReceived(BidirectionalStream{}, map[string]string{":status": "200"}, "h2")
	t.Cleanup(func() {
		conn.Close()
		select {
		case <-s.exited:
		case <-time.After(5 * time.Second):
			t.Fatal("stream never terminated")
		}
		eventually(t, "the conn's goroutines to exit", func() bool { return runtime.NumGoroutine() <= base })
	})
	return conn, s
}

func (s *fakeStream) run() {
	defer close(s.exited)
	for {
		select {
		case task := <-s.tasks:
			task()
		case <-s.quit:
			return
		}
	}
}

func (s *fakeStream) post(task func()) {
	s.mu.Lock()
	destroyed := s.destroyed
	s.mu.Unlock()
	if !destroyed {
		s.tasks <- task
	}
}

// alive is checked before every callback: a destroyed stream's are dropped.
func (s *fakeStream) alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.destroyed
}

func (s *fakeStream) Start(string, string, map[string]string, int, bool) bool { return true }

func (s *fakeStream) Read(buffer []byte) int {
	s.mu.Lock()
	s.readSizes = append(s.readSizes, len(buffer))
	s.mu.Unlock()
	s.post(func() {
		s.readBuffer = buffer
		s.completeRead()
	})
	return 0
}

func (s *fakeStream) completeRead() {
	if s.readBuffer == nil || len(s.incoming) == 0 && !s.eof || !s.alive() {
		return
	}
	n := copy(s.readBuffer, s.incoming)
	s.incoming = s.incoming[n:]
	s.readBuffer = nil
	s.handler.OnReadCompleted(BidirectionalStream{}, n)
}

func (s *fakeStream) Write(buffer []byte, endOfStream bool) int {
	s.mu.Lock()
	s.writeSizes = append(s.writeSizes, len(buffer))
	s.writeAddrs = append(s.writeAddrs, &buffer[0])
	s.mu.Unlock()
	w := fakeWrite{buffer: buffer, snapshot: bytes.Clone(buffer), eos: endOfStream}
	s.post(func() {
		s.pending = append(s.pending, w)
		s.completeWrites()
	})
	return 0
}

func (s *fakeStream) WriteEndOfStream() int {
	s.post(func() {
		s.pending = append(s.pending, fakeWrite{eos: true})
		s.completeWrites()
	})
	return 0
}

func (s *fakeStream) completeWrites() {
	for !s.hold && len(s.pending) > 0 && s.alive() {
		w := s.pending[0]
		s.pending = s.pending[1:]
		if !bytes.Equal(w.buffer, w.snapshot) {
			s.t.Error("a write's buffer changed while the stream held it")
		}
		s.mu.Lock()
		if s.eosAt >= 0 {
			s.t.Error("data after end of stream")
		}
		s.written.Write(w.snapshot)
		if w.eos {
			s.eosAt = s.written.Len()
		}
		s.mu.Unlock()
		s.handler.OnWriteCompleted(BidirectionalStream{})
	}
}

func (s *fakeStream) Cancel() {
	s.post(func() {
		if s.alive() {
			s.pending, s.readBuffer = nil, nil
			s.handler.OnCanceled(BidirectionalStream{})
		}
	})
}

func (s *fakeStream) destroy() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.destroyed {
		s.destroyed = true
		close(s.quit)
	}
}

// feed is the far side sending data, or with nil, ending its side.
func (s *fakeStream) feed(data []byte) {
	s.post(func() {
		if data == nil {
			s.eof = true
		}
		s.incoming = append(s.incoming, data...)
		s.completeRead()
	})
}

// holdWrites stands in for an exhausted send window.
func (s *fakeStream) holdWrites(hold bool) {
	s.post(func() {
		s.hold = hold
		s.completeWrites()
	})
}

func (s *fakeStream) fail(code NetError) {
	s.post(func() {
		if s.alive() {
			s.pending, s.readBuffer = nil, nil
			s.handler.OnFailed(BidirectionalStream{}, int(code))
		}
	})
}

func (s *fakeStream) log() (readSizes, writeSizes []int, written []byte, eosAt int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.readSizes), slices.Clone(s.writeSizes), bytes.Clone(s.written.Bytes()), s.eosAt
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func connState(c *BidirectionalConn) (staged int, flushing bool, readBuffer bool) {
	c.access.Lock()
	defer c.access.Unlock()
	return len(c.writeBuffer), c.flushing, c.readBuffer != nil
}

func pattern(n int, seed int64) []byte {
	data := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(data)
	return data
}

func countTraffic(c *BidirectionalConn) *atomic.Int64 {
	var total atomic.Int64
	c.setOnTraffic(func(n int) { total.Add(int64(n)) })
	return &total
}

// Whatever the caller's buffer sizes, and however the far side chunks the
// stream, the bytes come out exactly as sent and are counted once.
func TestReadAheadReassemblesTheStream(t *testing.T) {
	for _, size := range []int{1, 3, 1000, 16383, 65535, 65536, 100000, 0} {
		name := strconv.Itoa(size)
		if size == 0 {
			name = "varying"
		}
		t.Run(name, func(t *testing.T) {
			conn, s := newFakeConn(t)
			traffic := countTraffic(conn)
			data := pattern(1<<20, int64(size))
			go func() {
				random := rand.New(rand.NewSource(int64(size) + 1))
				for rest := data; len(rest) > 0; {
					n := min(len(rest), 1+random.Intn(200<<10))
					s.feed(rest[:n])
					rest = rest[n:]
					if random.Intn(4) == 0 {
						time.Sleep(time.Millisecond)
					}
				}
				s.feed(nil)
			}()
			random := rand.New(rand.NewSource(int64(size) + 2))
			var got []byte
			for {
				n := size
				if n == 0 {
					// Sizes change from one Read to the next.
					n = 1 + random.Intn(80<<10)
				}
				p := make([]byte, n)
				read, err := conn.Read(p)
				got = append(got, p[:read]...)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("got %d bytes, not the %d sent", len(got), len(data))
			}
			if traffic.Load() != int64(len(data)) {
				t.Fatalf("counted %d bytes, read %d", traffic.Load(), len(data))
			}
			readSizes, _, _, _ := s.log()
			if size > 0 && size < bulkBufferSize && !slices.Contains(readSizes, bulkBufferSize/size*size) {
				t.Fatalf("never read ahead: %v", readSizes)
			}
		})
	}
}

// A read that fills its buffer turns read-ahead on or keeps it on; one that
// does not turns it off and the buffer goes back once drained, even when it
// still covered the caller's buffer, as the tail of a burst usually does.
func TestReadAheadReturnsToDirectReads(t *testing.T) {
	conn, s := newFakeConn(t)
	const caller, tail = 16383, 30000
	const ahead = bulkBufferSize / caller * caller
	data := pattern(caller+ahead+tail, 1)
	s.feed(data)
	p := make([]byte, caller)
	var got []byte
	read := func(until int) {
		for len(got) < until {
			n, err := conn.Read(p)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, p[:n]...)
		}
	}
	// A direct read fills p, then a read-ahead fills the four p it covers.
	read(caller + ahead)
	if _, _, held := connState(conn); !held {
		t.Fatal("a bulk stream gave up its buffer between reads")
	}
	// The next read-ahead gets the 30000 byte tail: more than p, less than
	// the buffer.
	read(len(data))
	if _, _, held := connState(conn); held {
		t.Fatal("the buffer outlived bulk mode")
	}
	s.feed(nil)
	if _, err := conn.Read(p); err != io.EOF {
		t.Fatalf("got %v, want EOF", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes differ")
	}
	readSizes, _, _, _ := s.log()
	if want := []int{caller, ahead, ahead, caller}; !slices.Equal(readSizes, want) {
		t.Fatalf("stream reads %v, want %v", readSizes, want)
	}
}

// Datagrams that arrive one at a time and are read a header at a time, as uot
// reads them, take one stream read each, and a header read waiting on an idle
// stream holds the small read-ahead, not 64 KiB.
func TestSparseDatagramsReadAheadIntoASmallBuffer(t *testing.T) {
	conn, s := newFakeConn(t)
	const header, body = 3, 1200
	for i := range 3 {
		data := pattern(header+body, int64(i))
		s.feed(data)
		got := make([]byte, len(data))
		if _, err := io.ReadFull(conn, got[:header]); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(conn, got[header:]); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, data) {
			t.Fatal("bytes differ")
		}
	}
	if _, _, held := connState(conn); held {
		t.Fatal("a short read-ahead kept its buffer once drained")
	}
	readSizes, _, _, _ := s.log()
	small := smallReadAheadSize / header * header
	if want := []int{small, small, small}; !slices.Equal(readSizes, want) {
		t.Fatalf("stream reads %v, want %v", readSizes, want)
	}

	read := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(conn, make([]byte, header))
		read <- err
	}()
	eventually(t, "the idle header read", func() bool {
		readSizes, _, _, _ := s.log()
		return len(readSizes) == 4
	})
	conn.access.Lock()
	held := cap(conn.readBuffer)
	conn.access.Unlock()
	if held != smallReadAheadSize {
		t.Fatalf("an idle header read holds %d bytes, want %d", held, smallReadAheadSize)
	}
	s.feed(pattern(header, 9))
	if err := <-read; err != nil {
		t.Fatal(err)
	}
}

// A small read that fills its read-ahead turns bulk on, and the next
// read-ahead is a full one, not another small one.
func TestSmallReadAheadGrowsOnceBulk(t *testing.T) {
	conn, s := newFakeConn(t)
	const caller = 16383
	const ahead = bulkBufferSize / caller * caller
	data := pattern(smallReadAheadSize+ahead, 1)
	s.feed(data)
	p := make([]byte, caller)
	var got []byte
	for len(got) < len(data) {
		n := caller
		if got == nil {
			n = 2
		}
		read, err := conn.Read(p[:n])
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, p[:read]...)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("bytes differ")
	}
	readSizes, _, _, _ := s.log()
	if want := []int{smallReadAheadSize, ahead}; !slices.Equal(readSizes, want) {
		t.Fatalf("stream reads %v, want %v", readSizes, want)
	}
}

// The read-ahead buffer is let go when the stream ends, whether or not a read
// was waiting on it.
func TestReadAheadBufferIsReleased(t *testing.T) {
	bulk := func(t *testing.T) (*BidirectionalConn, *fakeStream) {
		conn, s := newFakeConn(t)
		s.feed(pattern(16383+100, 1))
		p := make([]byte, 16383)
		conn.Read(p)      // fills: bulk
		conn.Read(p[:50]) // reads ahead 100 bytes, serves 50 of them
		return conn, s
	}
	t.Run("close with bytes unread", func(t *testing.T) {
		conn, _ := bulk(t)
		if _, _, held := connState(conn); !held {
			t.Fatal("nothing read ahead")
		}
		conn.Close()
		if _, _, held := connState(conn); held {
			t.Fatal("Close kept the buffer")
		}
		if _, err := conn.Read(make([]byte, 10)); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("got %v, want net.ErrClosed", err)
		}
	})
	waiting := func(t *testing.T, stop func(*BidirectionalConn), want error) {
		conn, s := newFakeConn(t)
		s.feed(pattern(16383, 1))
		p := make([]byte, 16383)
		conn.Read(p)
		go func() {
			eventually(t, "the read-ahead read", func() bool {
				readSizes, _, _, _ := s.log()
				return len(readSizes) == 2
			})
			stop(conn)
		}()
		if _, err := conn.Read(p); !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
		if _, _, held := connState(conn); held {
			t.Fatal("the buffer outlived the stream")
		}
	}
	t.Run("close while reading ahead", func(t *testing.T) {
		waiting(t, func(c *BidirectionalConn) { c.Close() }, net.ErrClosed)
	})
	t.Run("deadline while reading ahead", func(t *testing.T) {
		waiting(t, func(c *BidirectionalConn) { c.SetReadDeadline(time.Now()) }, os.ErrDeadlineExceeded)
	})
	t.Run("stream failure keeps unread bytes", func(t *testing.T) {
		conn, s := bulk(t)
		p := make([]byte, 16383)
		s.fail(NetErrorConnectionReset)
		<-conn.Done()
		if n, err := conn.Read(p); n != 50 || err != nil {
			t.Fatalf("got %d, %v; want the 50 bytes read before the failure", n, err)
		}
		if _, _, held := connState(conn); held {
			t.Fatal("the buffer outlived the stream")
		}
		if _, err := conn.Read(p); err != NetErrorConnectionReset {
			t.Fatalf("got %v, want %v", err, NetErrorConnectionReset)
		}
	})
}

// Writes reach the stream in order, coalesced while one is in flight, and
// CloseWrite sends end of stream only after all of them.
func TestWriteBehindKeepsOrderAndDrainsOnCloseWrite(t *testing.T) {
	conn, s := newFakeConn(t)
	traffic := countTraffic(conn)
	const record, records = 16383, 64
	data := pattern(record*records, 1)
	s.holdWrites(true)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < records; i++ {
			if _, err := conn.Write(data[i*record : (i+1)*record]); err != nil {
				done <- err
				return
			}
		}
		done <- conn.CloseWrite()
	}()
	// One write in flight, four records staged behind it, the writer blocked.
	eventually(t, "the staging buffer to fill", func() bool {
		staged, _, _ := connState(conn)
		return staged == 4*record
	})
	s.holdWrites(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, writeSizes, written, eosAt := s.log()
	if !bytes.Equal(written, data) {
		t.Fatal("bytes reached the stream out of order")
	}
	if eosAt != len(data) {
		t.Fatalf("end of stream after %d bytes, want %d", eosAt, len(data))
	}
	// The four staged while the writer waited go out as one write.
	if !slices.Contains(writeSizes, 4*record) || len(writeSizes) > records-3 {
		t.Fatalf("stream writes %v, want %d staged records coalesced", writeSizes, 4)
	}
	for _, size := range writeSizes {
		if size > bulkBufferSize {
			t.Fatalf("a %d byte stream write", size)
		}
	}
	if traffic.Load() != int64(len(data)) {
		t.Fatalf("counted %d bytes, wrote %d", traffic.Load(), len(data))
	}
	if _, flushing, _ := connState(conn); flushing {
		t.Fatal("the flusher outlived CloseWrite")
	}
}

// A write larger than the staging buffer goes to the stream as is, after what
// was staged before it.
func TestLargeWriteIsNotCopied(t *testing.T) {
	conn, s := newFakeConn(t)
	small, large := pattern(100, 1), pattern(100000, 2)
	s.holdWrites(true)
	if _, err := conn.Write(small); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := conn.Write(large)
		done <- err
	}()
	time.Sleep(10 * time.Millisecond)
	s.holdWrites(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_, _, written, _ := s.log()
	if !bytes.Equal(written, append(bytes.Clone(small), large...)) {
		t.Fatal("bytes differ")
	}
	s.mu.Lock()
	addr := s.writeAddrs[len(s.writeAddrs)-1]
	s.mu.Unlock()
	if addr != &large[0] {
		t.Fatal("the large write was copied")
	}
}

// A background write that fails breaks the conn for whoever comes next.
func TestFailedBackgroundWriteSurfaces(t *testing.T) {
	conn, s := newFakeConn(t)
	s.holdWrites(true)
	if _, err := conn.Write(pattern(100, 1)); err != nil {
		t.Fatal(err)
	}
	s.fail(NetErrorConnectionReset)
	<-conn.Done()
	if _, err := conn.Write(pattern(100, 2)); err != NetErrorConnectionReset {
		t.Fatalf("Write: got %v, want %v", err, NetErrorConnectionReset)
	}
	if err := conn.CloseWrite(); err != NetErrorConnectionReset {
		t.Fatalf("CloseWrite: got %v, want %v", err, NetErrorConnectionReset)
	}
	if _, err := conn.Read(make([]byte, 10)); err != NetErrorConnectionReset {
		t.Fatalf("Read: got %v, want %v", err, NetErrorConnectionReset)
	}
}

// The write deadline bounds a Write waiting for staging space and CloseWrite
// waiting for the drain, and cancels the stream when it fires.
func TestWriteDeadlineWhileWaitingForTheFlusher(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		conn, s := newFakeConn(t)
		s.holdWrites(true)
		conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		record := pattern(16383, 1)
		var err error
		for i := 0; i < 10 && err == nil; i++ {
			_, err = conn.Write(record)
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("got %v, want a deadline", err)
		}
		if conn.Err() != context.Canceled {
			t.Fatalf("stream ended with %v, want cancelled", conn.Err())
		}
	})
	t.Run("close write", func(t *testing.T) {
		conn, s := newFakeConn(t)
		s.holdWrites(true)
		if _, err := conn.Write(pattern(100, 1)); err != nil {
			t.Fatal(err)
		}
		conn.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		if err := conn.CloseWrite(); !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("got %v, want a deadline", err)
		}
		if _, _, _, eosAt := s.log(); eosAt >= 0 {
			t.Fatal("end of stream went out")
		}
	})
}

// CarriedPayload is true exactly when bytes reached the stream, and final once
// Close has returned. Staged bytes Close finds are dropped and do not count.
func TestCarriedPayloadCountsOnlyWhatReachedTheStream(t *testing.T) {
	t.Run("staged then closed", func(t *testing.T) {
		conn, s := newFakeConn(t)
		// What Write leaves behind when Close comes before the flusher has
		// run, which the scheduler decides.
		conn.access.Lock()
		conn.writeBuffer = append(buf.Get(bulkBufferSize)[:0], "payload"...)
		conn.flushing = true
		conn.access.Unlock()
		conn.Close()
		conn.flush()
		// Once the stream has ended too, Write picks between the two
		// answers as it always has.
		if _, err := conn.Write([]byte("late")); !errors.Is(err, net.ErrClosed) && err != context.Canceled {
			t.Fatalf("got %v, want net.ErrClosed", err)
		}
		if conn.CarriedPayload() {
			t.Fatal("staged bytes counted as carried")
		}
		if _, writeSizes, _, _ := s.log(); len(writeSizes) != 0 {
			t.Fatalf("stream writes %v after Close", writeSizes)
		}
	})
	t.Run("flushed", func(t *testing.T) {
		conn, s := newFakeConn(t)
		if _, err := conn.Write([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the write", func() bool {
			_, _, written, _ := s.log()
			return len(written) > 0
		})
		conn.Close()
		if !conn.CarriedPayload() {
			t.Fatal("a delivered write not counted")
		}
	})
	t.Run("racing close", func(t *testing.T) {
		var carried, dropped int
		for i := 0; i < 200; i++ {
			conn, s := newFakeConn(t)
			conn.Write([]byte("payload"))
			if i%2 == 1 {
				// Lets the flusher win now and then.
				runtime.Gosched()
			}
			conn.Close()
			answer := conn.CarriedPayload()
			<-s.exited
			eventually(t, "the flusher to exit", func() bool {
				_, flushing, _ := connState(conn)
				return !flushing
			})
			_, writeSizes, _, _ := s.log()
			if answer != (len(writeSizes) > 0) || conn.CarriedPayload() != answer {
				t.Fatalf("CarriedPayload %v after Close, stream writes %v", answer, writeSizes)
			}
			if answer {
				carried++
			} else {
				dropped++
			}
		}
		t.Logf("%d carried, %d dropped", carried, dropped)
	})
}
