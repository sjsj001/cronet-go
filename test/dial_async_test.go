package test

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cronet "github.com/sagernet/cronet-go"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/stretchr/testify/require"
)

// slowDialer holds back the next armed TCP dials until hold returns, and
// delays every other TCP dial by up to jitter. UDP dials pass straight
// through: Cronet still makes them on its network thread, the IPv6 probe at
// engine start among them.
type slowDialer struct {
	underlying N.Dialer
	armed      atomic.Int32
	hold       func()
	jitter     time.Duration
	redirect   M.Socksaddr   // dial here instead, if valid
	wrap       bool          // hand back a conn that is not a *net.TCPConn
	closed     chan struct{} // closed when a wrapped conn is
}

// trackedConn is a wrappedConn that reports its Close.
type trackedConn struct {
	wrappedConn
	closed chan struct{}
	once   sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.wrappedConn.Close()
}

func (d *slowDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	if N.NetworkName(network) != N.NetworkTCP {
		return d.underlying.DialContext(ctx, network, destination)
	}
	if d.armed.Add(-1) >= 0 {
		d.hold()
		// Dial as if still wanted: what is under test is a connection that
		// arrives after nobody is waiting for it.
		ctx = context.Background()
	} else if d.jitter > 0 {
		time.Sleep(rand.N(d.jitter))
	}
	if d.redirect.IsValid() {
		destination = d.redirect
	}
	conn, err := d.underlying.DialContext(ctx, network, destination)
	if err != nil || !d.wrap {
		return conn, err
	}
	return &trackedConn{wrappedConn: wrappedConn{Conn: conn}, closed: d.closed}, nil
}

func (d *slowDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	return d.underlying.ListenPacket(ctx, destination)
}

// Cronet called the custom dialer on its network thread, and every stream of
// the engine waited out each new session's TCP handshake with it: a black-holed
// proxy stalled them all for the dialer's timeout.
func TestNaiveSlowDialStallsNoOtherStream(t *testing.T) {
	env := setupTestEnv(t)
	startEchoServer(t, 17100)
	dialer := &slowDialer{underlying: N.SystemDialer}
	// Two fixed sessions: the second stream opens the second session.
	client := env.newNaiveClient(t, cronet.NaiveClientOptions{
		Dialer:              dialer,
		DNSResolver:         localhostDNSResolver(t),
		InsecureConcurrency: 2,
	})
	target := M.ParseSocksaddrHostPort("127.0.0.1", 17100)
	a, err := client.DialContext(context.Background(), N.NetworkTCP, target)
	require.NoError(t, err)
	defer a.Close()

	var worst atomic.Int64
	stop := make(chan struct{})
	echoed := make(chan error, 1)
	go func() {
		ping := []byte("12345678")
		pong := make([]byte, len(ping))
		for {
			select {
			case <-stop:
				echoed <- nil
				return
			default:
			}
			start := time.Now()
			if _, err := a.Write(ping); err != nil {
				echoed <- err
				return
			}
			if _, err := io.ReadFull(a, pong); err != nil {
				echoed <- err
				return
			}
			if took := int64(time.Since(start)); took > worst.Load() {
				worst.Store(took)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	time.Sleep(200 * time.Millisecond)

	held := make(chan struct{})
	release := make(chan struct{})
	dialer.hold = func() {
		close(held)
		<-release
	}
	dialer.armed.Store(1)
	go func() {
		b, err := client.DialContext(context.Background(), N.NetworkTCP, target)
		if err == nil {
			b.Close()
		}
	}()
	<-held
	worst.Store(0)
	time.Sleep(2 * time.Second)
	close(release)
	// Let an echo stalled behind the dial come back before reading the worst.
	time.Sleep(200 * time.Millisecond)
	close(stop)
	require.NoError(t, <-echoed)
	// The dial is held for 2s; an echo a busy runner delays is far short of
	// that.
	require.Less(t, time.Duration(worst.Load()), time.Second,
		"a stream on a live session stalled while another session dialed")
}

// A dial can outlive the request and the engine that asked for it, and what it
// made must then be closed rather than left open. On the direct path that is
// Cronet's part: the descriptor it was handed has no socket to adopt it. On the
// relayed one it is the client's: Close has stopped counting relays, and the
// connection is closed instead of relayed.
func TestNaiveDialFinishingAfterCloseLeavesNothingOpen(t *testing.T) {
	for _, wrap := range []bool{false, true} {
		name := "direct"
		if wrap {
			name = "relayed"
		}
		t.Run(name, func(t *testing.T) {
			env := setupTestEnv(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer listener.Close()
			held := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseDial := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseDial()
			dialer := &slowDialer{
				underlying: N.SystemDialer,
				hold: func() {
					close(held)
					<-release
				},
				redirect: M.SocksaddrFromNet(listener.Addr()),
				wrap:     wrap,
				closed:   make(chan struct{}),
			}
			dialer.armed.Store(1)
			client := env.newNaiveClient(t, cronet.NaiveClientOptions{
				Dialer:      dialer,
				DNSResolver: localhostDNSResolver(t),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			go func() {
				conn, err := client.DialContext(ctx, N.NetworkTCP, M.ParseSocksaddrHostPort("127.0.0.1", 17101))
				if err == nil {
					conn.Close()
				}
			}()
			<-held
			closed := make(chan error, 1)
			go func() { closed <- client.Close() }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				releaseDial()
				t.Fatal("Close waited for a dial in progress")
			}

			releaseDial()
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			var conn net.Conn
			select {
			case conn = <-accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("the held dial never connected")
			}
			defer conn.Close()
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
			_, err = conn.Read(make([]byte, 1))
			var netErr net.Error
			require.False(t, errors.As(err, &netErr) && netErr.Timeout(),
				"the connection a dial produced after Close is still open")
			if wrap {
				select {
				case <-dialer.closed:
				case <-time.After(5 * time.Second):
					t.Fatal("the client never closed the connection it dialed after Close")
				}
			}
		})
	}
}

// Dials now run side by side on the thread pool rather than one by one on the
// network thread.
func TestNaiveManyDialsAtOnce(t *testing.T) {
	env := setupTestEnv(t)
	startEchoServer(t, 17102)
	client := env.newNaiveClient(t, cronet.NaiveClientOptions{
		Dialer:              &slowDialer{underlying: N.SystemDialer, jitter: 200 * time.Millisecond},
		DNSResolver:         localhostDNSResolver(t),
		InsecureConcurrency: 16,
	})
	target := M.ParseSocksaddrHostPort("127.0.0.1", 17102)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := client.DialContext(ctx, N.NetworkTCP, target)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close()
			data := []byte("many dials at once")
			if _, err := conn.Write(data); err != nil {
				errs <- err
				return
			}
			if _, err := io.ReadFull(conn, make([]byte, len(data))); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
