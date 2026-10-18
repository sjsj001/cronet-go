package test

import (
	"context"
	"io"
	"net"
	"sort"
	"testing"
	"time"

	cronet "github.com/sagernet/cronet-go"
	M "github.com/sagernet/sing/common/metadata"

	"github.com/stretchr/testify/require"
)

// Chromium held downstream data for 1 ms before handing it to the reader, on
// every response, echo or handshake reply: a millisecond per proxy hop.
// patches/naiveproxy/0004 hands over data that follows a quiet spell at once.
func TestNaiveDataAfterAQuietSpellIsNotHeld(t *testing.T) {
	env := setupTestEnv(t)
	client := env.newNaiveClient(t, cronet.NaiveClientOptions{})

	listener, err := net.Listen("tcp", "127.0.0.1:18102")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	echoed := make(chan time.Time, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		b := make([]byte, 1)
		for {
			if _, err := conn.Read(b); err != nil {
				return
			}
			echoed <- time.Now()
			if _, err := conn.Write(b); err != nil {
				return
			}
		}
	}()

	conn, err := client.DialEarly(context.Background(), M.ParseSocksaddrHostPort("127.0.0.1", 18102))
	require.NoError(t, err)
	defer conn.Close()
	b := []byte{'x'}
	var down []time.Duration
	for i := 0; i < 60; i++ {
		// Longer than the quiet spell, so every echo follows one.
		time.Sleep(10 * time.Millisecond)
		_, err = conn.Write(b)
		require.NoError(t, err)
		_, err = io.ReadFull(conn, b)
		require.NoError(t, err)
		arrived := time.Now()
		if i >= 10 {
			down = append(down, arrived.Sub(<-echoed))
		} else {
			<-echoed
		}
	}
	sort.Slice(down, func(i, j int) bool { return down[i] < down[j] })
	require.Less(t, down[len(down)/2], 700*time.Microsecond,
		"an echo after a quiet spell waited %v (median) between the server's write and the reader; the 1 ms batching wait is back", down[len(down)/2])
}
