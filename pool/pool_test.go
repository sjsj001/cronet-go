package pool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// clock is the test's time authority; the allocator never calls time.Now.
type clock struct {
	mu sync.Mutex
	at time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
}

func newTestAllocator(cap int) (*Allocator, *clock) {
	c := &clock{at: time.Unix(1_000_000, 0)}
	a := NewAdaptive(cap)
	a.now = c.now
	return a, c
}

// A cold single stream lands on pool 0 and the client stays at
// one pool.
func TestColdSingleStream(t *testing.T) {
	a, _ := newTestAllocator(16)
	s := a.Allocate("hub:443")
	if s.Pool() != 0 {
		t.Fatalf("first stream on pool %d, want 0", s.Pool())
	}
	if a.InUse() != 1 {
		t.Fatalf("InUse = %d, want 1", a.InUse())
	}
}

// Sixteen simultaneous streams to one destination — a downloader's segment
// storm — spread two per connection in the first wave. They have moved no
// bytes when they are placed, so the spread keys on the destination, the one
// signal a zero-byte burst carries.
func TestColdBurstSpreadsFirstWave(t *testing.T) {
	a, _ := newTestAllocator(16)
	perPool := map[int]int{}
	for i := 0; i < 16; i++ {
		perPool[a.Allocate("hub:443").Pool()]++
	}
	if len(perPool) != 8 {
		t.Fatalf("burst spread over %d pools, want 8: %v", len(perPool), perPool)
	}
	for pool, n := range perPool {
		if n != sameDestinationCap {
			t.Fatalf("pool %d carries %d streams, want %d", pool, n, sameDestinationCap)
		}
	}
}

// Daily browsing — mid-sized transfers that never stack four deep, with a
// millisecond-lived heartbeat probe threading between them — rides exactly
// two pools: the second pipe is deliberate redundancy, and ordinary traffic
// must never grow past it.
func TestBrowsingRidesTwoPools(t *testing.T) {
	a, c := newTestAllocator(16)
	var open []*Stream
	for i := 0; i < 20; i++ {
		s := a.Allocate(fmt.Sprintf("site%d:443", i))
		s.Note(2 << 20) // a 2MB page resource
		open = append(open, s)
		if len(open) == 3 { // never more than three in flight
			open[0].Release()
			open = open[1:]
		}
		// A heartbeat rides alongside and is gone before the next arrival.
		probe := a.Allocate("probe:443")
		probe.Note(2048)
		if probe.Pool() >= minSpreadPools {
			t.Fatalf("heartbeat %d placed on pool %d, want within the two daily pipes", i, probe.Pool())
		}
		probe.Release()
		c.advance(500 * time.Millisecond)
		if got := s.Pool(); got >= minSpreadPools {
			t.Fatalf("browse stream %d placed on pool %d, want within the two daily pipes", i, got)
		}
	}
	if a.InUse() > minSpreadPools {
		t.Fatalf("InUse = %d after browsing, want at most %d", a.InUse(), minSpreadPools)
	}
}

// The second concurrent stream opens the second pipe even though the first
// pool has plenty of room — one connection is a single point of failure. A
// lone stream still means one pool.
func TestSecondStreamOpensSecondPipe(t *testing.T) {
	a, _ := newTestAllocator(16)
	first := a.Allocate("a:443")
	if first.Pool() != 0 {
		t.Fatalf("lone stream on pool %d, want 0", first.Pool())
	}
	second := a.Allocate("b:443")
	if second.Pool() != 1 {
		t.Fatalf("second concurrent stream on pool %d, want the second pipe 1", second.Pool())
	}
	third := a.Allocate("c:443")
	if third.Pool() >= minSpreadPools {
		t.Fatalf("third stream on pool %d, want packed within the two pipes", third.Pool())
	}
}

// A sustained transfer becomes an elephant and new
// streams — probes included — route around its pool; ten quiet seconds
// demote it and the pool is a candidate again.
func TestElephantAvoidedThenDemoted(t *testing.T) {
	a, c := newTestAllocator(16)
	video := a.Allocate("video:443")
	if video.Pool() != 0 {
		t.Fatalf("video on pool %d, want 0", video.Pool())
	}
	video.Note(5 << 20) // over the 4MB/10s bar

	// A companion keeps a second pool in use so placement decisions below are
	// about the elephant, not about the minimum spread.
	companion := a.Allocate("companion:443")
	probe := a.Allocate("probe:443")
	if probe.Pool() == video.Pool() {
		t.Fatal("probe landed on the elephant's pool")
	}
	probe.Release()

	c.advance(11 * time.Second) // playback stops; window drains to zero
	after := a.Allocate("after:443")
	if after.Pool() != 0 {
		t.Fatalf("stream after demotion on pool %d, want the reclaimed pool 0", after.Pool())
	}
	companion.Release()
}

// A long-lived quiet session (UoT-shaped) demotes without ever
// closing — quiet, not closed, is the demotion trigger.
func TestQuietLongSessionDemotes(t *testing.T) {
	a, c := newTestAllocator(16)
	session := a.Allocate("uot:443")
	session.Note(6 << 20)
	companion := a.Allocate("companion:443") // keeps minimum spread satisfied
	if got := a.Allocate("beside:443"); got.Pool() == session.Pool() {
		t.Fatal("stream landed beside a busy long session")
	}
	c.advance(10*time.Second + time.Second)
	if got := a.Allocate("later:443"); got.Pool() != session.Pool() {
		t.Fatalf("after 10 quiet seconds stream went to pool %d, want %d back in rotation",
			got.Pool(), session.Pool())
	}
	companion.Release()
}

// Every pool loaded and elephant-ridden at the ceiling — the
// allocator degrades to least-busy instead of refusing or growing past cap.
func TestAllElephantsAtCapDegrade(t *testing.T) {
	a, _ := newTestAllocator(2)
	first := a.Allocate("a:443")
	first.Note(5 << 20) // elephant on pool 0
	second := a.Allocate("b:443")
	if second.Pool() == first.Pool() {
		t.Fatal("second stream should have avoided the elephant's pool")
	}
	second.Note(5 << 20) // both pools elephant-ridden, cap reached
	third := a.Allocate("c:443")
	if third.Pool() >= 2 {
		t.Fatalf("allocator grew to pool %d past its cap of 2", third.Pool())
	}
	fourth := a.Allocate("d:443")
	if fourth.Pool() == third.Pool() {
		t.Fatal("degraded allocation should still balance across the loaded pools")
	}
}

// At the cap with the other pools full, a new stream still goes to a pool
// without an elephant. The elephant's pool looked least busy only because the
// scan had left it out.
func TestAtCapNewStreamsStillAvoidTheElephant(t *testing.T) {
	a, _ := newTestAllocator(3)
	bulk := a.Allocate("bulk:443")
	bulk.Note(5 << 20)
	for i := range 2 * maxActivesPerPool {
		if a.Allocate(fmt.Sprint("light", i, ":443")).Pool() == bulk.Pool() {
			t.Fatal("a light stream joined the elephant's pool before the cap")
		}
	}
	for i := range 3 {
		if pool := a.Allocate(fmt.Sprint("late", i, ":443")).Pool(); pool == bulk.Pool() {
			t.Fatalf("stream %d placed on the elephant's pool %d at the cap", i, pool)
		}
	}
}

// The cap follows memory ÷ window, clamped to [2, 16], and an
// unreadable memory total is treated as the smallest production box.
func TestCapDerivation(t *testing.T) {
	cases := []struct {
		memory, window uint64
		want           int
	}{
		{2 << 30, 20 << 20, 16},    // 512MB budget / 20MB — clamped at the top
		{2 << 30, 96 << 20, 5},     // the spec's long-leg example
		{1961 << 20, 128 << 20, 3}, // a real 2GB Linux box: MemTotal is short of 2GiB by the kernel's reserve, so three, not four
		{1 << 30, 96 << 20, 2},     // small box, big window — floor keeps two
		{0, 128 << 20, 4},          // unknown host: 512MB assumed budget
		{64 << 20, 4 << 20, 4},     // iOS: four 4MB windows inside the extension's limit
		{64 << 30, 1 << 20, 16},    // absurd inputs still clamp
		{256 << 20, 128 << 20, 2},
	}
	for _, c := range cases {
		if got := Cap(c.memory, c.window); got != c.want {
			t.Fatalf("Cap(%d, %d) = %d, want %d", c.memory, c.window, got, c.want)
		}
	}
}

// Explicit concurrency stays a fixed set of pools with least-busy
// placement — the escape hatch the thirteen production instances run on.
func TestFixedModeNeverGrows(t *testing.T) {
	a := NewFixed(4)
	perPool := map[int]int{}
	for i := 0; i < 8; i++ {
		s := a.Allocate("x:443")
		if s.Pool() >= 4 {
			t.Fatalf("fixed-4 allocator used pool %d", s.Pool())
		}
		perPool[s.Pool()]++
	}
	for pool, n := range perPool {
		if n != 2 {
			t.Fatalf("pool %d carries %d, want an even 2", pool, n)
		}
	}
	single := NewFixed(1)
	for i := 0; i < 5; i++ {
		if single.Allocate("x:443").Pool() != 0 {
			t.Fatal("fixed-1 allocator left pool 0")
		}
	}
}

// Fixed mode spreads least-busy, but not onto the pipe a download is sitting
// on: the guard has to outrank the count, or an explicit concurrency setting
// keeps feeding new streams to the elephant every time it drops to least busy.
func TestFixedModeAvoidsElephant(t *testing.T) {
	a := NewFixed(4)
	c := &clock{at: time.Unix(1_000_000, 0)}
	a.now = c.now

	streams := make([]*Stream, 8)
	for i := range streams {
		streams[i] = a.Allocate(fmt.Sprintf("dst-%d:443", i))
	}
	// Two per pool, then drain one from pool 0 so plain least-busy would send
	// the next stream straight back to it.
	streams[4].Release()
	if streams[0].Pool() != 0 {
		t.Fatalf("first stream on pool %d, want 0", streams[0].Pool())
	}
	streams[0].Note(5 << 20)

	next := a.Allocate("probe:443")
	if next.Pool() == 0 {
		t.Fatal("probe landed on the elephant's pool")
	}

	c.advance(11 * time.Second) // download ends; the window drains to zero
	after := a.Allocate("after:443")
	if after.Pool() != 0 {
		t.Fatalf("stream after demotion on pool %d, want the emptiest pool 0", after.Pool())
	}
}

// A thousand concurrent placements count exactly — placement is
// registration, so nothing is double-booked or leaked. Run under -race.
func TestConcurrentAllocateCountsExactly(t *testing.T) {
	a, _ := newTestAllocator(16)
	const streams = 1000
	var wg sync.WaitGroup
	kept := make(chan *Stream, streams)
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int, release bool) {
			defer wg.Done()
			s := a.Allocate(fmt.Sprintf("d%d:443", i%5))
			s.Note(1024)
			if release {
				s.Release()
				s.Release() // idempotent under contention
			} else {
				kept <- s
			}
		}(i, i%2 == 0)
	}
	wg.Wait()
	close(kept)
	held := 0
	for range kept {
		held++
	}
	total := 0
	a.mu.Lock()
	for i, state := range a.pools {
		if i >= a.ceiling {
			t.Fatalf("pool index %d beyond ceiling %d", i, a.ceiling)
		}
		total += len(state.streams)
	}
	a.mu.Unlock()
	if total != held {
		t.Fatalf("%d streams held but %d counted active", held, total)
	}
}

// Heartbeat probes — one or two short-lived streams at a time — never grow
// the pool set past the two daily pipes, round after round.
func TestHeartbeatsDoNotGrow(t *testing.T) {
	a, c := newTestAllocator(16)
	for i := 0; i < 50; i++ {
		first := a.Allocate("probe:443")
		second := a.Allocate("probe:443")
		if first.Pool() >= minSpreadPools || second.Pool() >= minSpreadPools {
			t.Fatalf("probe round %d left the two daily pipes: %d/%d", i, first.Pool(), second.Pool())
		}
		first.Release()
		second.Release()
		c.advance(time.Second)
	}
	if a.InUse() > minSpreadPools {
		t.Fatalf("InUse = %d after heartbeats, want at most %d", a.InUse(), minSpreadPools)
	}
}

// Growth is demand-shaped and the low bias reclaims high pools: after a burst
// drains, new load fills pool 0 first, so idle timers can take the tail.
func TestDrainedBurstRefillsLowFirst(t *testing.T) {
	a, _ := newTestAllocator(16)
	var burst []*Stream
	for i := 0; i < 16; i++ {
		burst = append(burst, a.Allocate("hub:443"))
	}
	for _, s := range burst {
		s.Release()
	}
	if a.InUse() != 1 {
		t.Fatalf("InUse = %d after drain, want 1", a.InUse())
	}
	if got := a.Allocate("next:443").Pool(); got != 0 {
		t.Fatalf("post-drain stream on pool %d, want 0", got)
	}
}

// Four parallel segments to one destination ride two connections; four
// resources from four different sites pack onto one. The destination is what
// separates a downloader from a page burst.
func TestSameDestinationSpreadsDistinctPack(t *testing.T) {
	a, _ := newTestAllocator(16)
	perPool := map[int]int{}
	for i := 0; i < 4; i++ {
		perPool[a.Allocate("bigfile:443").Pool()]++
	}
	if len(perPool) != 2 || perPool[0] != 2 || perPool[1] != 2 {
		t.Fatalf("same-destination burst placed %v, want 2 pools x 2", perPool)
	}

	b, _ := newTestAllocator(16)
	for i := 0; i < 4; i++ {
		if got := b.Allocate(fmt.Sprintf("site%d:443", i)).Pool(); got >= minSpreadPools {
			t.Fatalf("distinct-destination stream %d on pool %d, want packed within the two daily pipes", i, got)
		}
	}
}

// Expansion prefers a recently-drained slot — its session is likely still
// alive, so the spread is free — over a slot that went cold long ago.
func TestWarmSlotPreferredOverCold(t *testing.T) {
	a, c := newTestAllocator(16)
	var pool0, pool1 []*Stream
	for _, s := range connectedBurst(a, "seed:443", 4) {
		if s.Pool() == 0 {
			pool0 = append(pool0, s)
		} else {
			pool1 = append(pool1, s)
		}
	}
	if len(pool0) != 2 || len(pool1) != 2 {
		t.Fatalf("seed burst placed %d/%d, want 2/2", len(pool0), len(pool1))
	}
	for _, s := range pool0 {
		s.Release()
	}
	c.advance(200 * time.Second) // slot 0 goes cold
	for _, s := range pool1 {
		s.Release()
	}
	if got := a.Allocate("fresh:443").Pool(); got != 1 {
		t.Fatalf("expansion picked pool %d, want the warm slot 1 over the cold slot 0", got)
	}
}

// A finished download leaves a wide set of pools behind. The browsing that
// follows must not fan out across them — it settles back onto the lowest two,
// leaving the rest untouched so the server's idle timer can reclaim them.
func TestBrowsingAfterBurstDoesNotFanOut(t *testing.T) {
	a, c := newTestAllocator(16)

	var burst []*Stream
	for i := 0; i < 16; i++ {
		s := a.Allocate("bigfile:443")
		s.Note(8 << 20) // every one of them an elephant while it runs
		burst = append(burst, s)
	}
	// Segments that are already moving bytes when their siblings arrive read
	// as elephants, so a real download fans out to the ceiling rather than to
	// the two-per-connection floor. That is the state this test starts from.
	opened := a.InUse()
	if opened < 8 {
		t.Fatalf("same-destination burst opened only %d pools; the scenario needs a wide fan-out", opened)
	}
	for _, s := range burst {
		s.Release()
	}
	c.advance(11 * time.Second) // downloads over, elephants demoted

	// Ordinary browsing: two or three concurrent fetches to varied sites.
	highest := 0
	for round := 0; round < 40; round++ {
		var open []*Stream
		for i := 0; i < 3; i++ {
			s := a.Allocate(fmt.Sprintf("site%d-%d:443", round, i))
			s.Note(512 << 10)
			if s.Pool() > highest {
				highest = s.Pool()
			}
			open = append(open, s)
		}
		for _, s := range open {
			s.Release()
		}
		c.advance(2 * time.Second)
		if a.InUse() > minSpreadPools {
			t.Fatalf("round %d: browsing spread to %d pools, want at most %d",
				round, a.InUse(), minSpreadPools)
		}
	}
	if highest >= minSpreadPools {
		t.Fatalf("browsing reached pool %d; the %d-pool burst should have left everything above %d idle",
			highest, opened, minSpreadPools-1)
	}
}

// connectedStream places a stream and marks it connected, as a stream whose
// request reached the wire.
func connectedStream(a *Allocator, destination string) *Stream {
	s := a.Allocate(destination)
	s.Connected()
	return s
}

// connectedBurst places n streams to destination at once and only then marks
// them connected: none had a session when placed, so they spread as a cold
// burst does.
func connectedBurst(a *Allocator, destination string, n int) []*Stream {
	streams := make([]*Stream, n)
	for i := range streams {
		streams[i] = a.Allocate(destination)
	}
	for _, s := range streams {
		s.Connected()
	}
	return streams
}

// failTwice records two failed dials far enough apart to count as two.
func failTwice(a *Allocator, c *clock) {
	a.DialFailed()
	c.advance(2 * dialFailureGap)
	a.DialFailed()
}

// A second failed dial within the window holds off dials: while the hold
// lasts a new stream rides a pool that has a session, even one it would
// otherwise avoid, and nothing dials the fresh pool whose dial just timed out
// behind a saturated bottleneck. One failure alone does not hold dials off.
// When the hold ends the allocator dials again, and the failed slot is not
// mistaken for a warm one.
func TestFailedDialsHoldOffNewPools(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedBurst(a, "hub:443", 8)
	dial := a.Allocate("hub:443").Warmup()
	if dial == nil || dial.Pool() != 4 {
		t.Fatalf("ninth stream to the same destination dialed %v, want fresh pool 4", dialedPool(dial))
	}
	a.DialFailed()
	dial.ReleaseUnconnected()
	dial = a.Allocate("hub:443").Warmup()
	if dial == nil || dial.Pool() != 4 {
		t.Fatalf("stream after one failed dial dialed %v, want pool 4 again", dialedPool(dial))
	}
	c.advance(5 * time.Second)
	a.DialFailed()
	dial.ReleaseUnconnected()
	for i := 0; i < 3; i++ {
		s := a.Allocate("hub:443")
		if s.Pool() > 3 || s.Warmup() != nil {
			t.Fatalf("stream during the hold on pool %d dialing %v, want one of the four in use and no dial",
				s.Pool(), dialedPool(s.Warmup()))
		}
		s.Release()
	}
	c.advance(dialHoldOff + time.Second)
	if dial := a.Allocate("hub:443").Warmup(); dial == nil || dial.Pool() != 4 {
		t.Fatalf("stream after the hold dialed %v, want pool 4 again", dialedPool(dial))
	}
}

// dialedPool names the pool a warm-up dials, for failure messages.
func dialedPool(warmup *Stream) any {
	if warmup == nil {
		return "nothing"
	}
	return warmup.Pool()
}

// A first connection and the backup Chromium races against it time out
// together: two failures that close are one, and hold nothing off.
func TestBackupDialFailureCountsOnce(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedBurst(a, "hub:443", 8)
	a.DialFailed()
	c.advance(250 * time.Millisecond)
	a.DialFailed()
	if dial := a.Allocate("hub:443").Warmup(); dial == nil || dial.Pool() != 4 {
		t.Fatalf("stream after one failed connect (two dials) dialed %v, want fresh pool 4", dialedPool(dial))
	}
}

// During a hold a slot whose streams are all still dialing needs a dial as
// much as an empty one: the next stream goes to a slot with a connected
// stream, however busier.
func TestHoldSkipsSlotsStillDialing(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedBurst(a, "a:443", 4) // pools 0 and 1, two each
	dialing := a.Allocate("a:443").Warmup()
	if dialing == nil || dialing.Pool() != 2 {
		t.Fatalf("setup: fifth stream to a:443 dialed %v, want 2", dialedPool(dialing))
	}
	failTwice(a, c)
	for i := 0; i < 4; i++ {
		if s := a.Allocate("e:443"); s.Pool() > 1 {
			t.Fatalf("stream during the hold on pool %d, a slot without a session", s.Pool())
		}
	}
}

// A stream that fails to connect on a slot a connected stream drained from
// moments ago leaves it cold: that session is gone.
func TestUnconnectedReleaseColdsWarmSlot(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedBurst(a, "a:443", 4) // pools 0 and 1, two each
	drained := a.Allocate("a:443").Warmup()
	if drained == nil || drained.Pool() != 2 {
		t.Fatalf("setup: fifth stream to a:443 dialed %v, want 2", dialedPool(drained))
	}
	drained.Connected()
	drained.Release()
	failed := a.Allocate("a:443")
	if failed.Pool() != 2 {
		t.Fatalf("setup: stream after the drain on pool %d, want the warm slot 2", failed.Pool())
	}
	failed.ReleaseUnconnected()
	failTwice(a, c)
	for i := 0; i < 4; i++ {
		if s := a.Allocate("e:443"); s.Pool() > 1 {
			t.Fatalf("stream during the hold on pool %d, whose session failed", s.Pool())
		}
	}
}

// A stream dropped before it connected leaves no warm slot behind: expansion
// takes the slot a connected stream drained from over the one an abandoned
// dial drained from later.
func TestAbandonedDialLeavesSlotCold(t *testing.T) {
	a, c := newTestAllocator(16)
	for i := 0; i < 4; i++ {
		a.Allocate("a:443") // pools 0 and 1, two each, all still dialing
	}
	abandoned := []*Stream{a.Allocate("a:443"), a.Allocate("a:443")}
	warm := connectedStream(a, "a:443")
	if abandoned[0].Pool() != 2 || abandoned[1].Pool() != 2 || warm.Pool() != 3 {
		t.Fatalf("setup: placed %d %d %d, want 2 2 3", abandoned[0].Pool(), abandoned[1].Pool(), warm.Pool())
	}
	warm.Release()
	c.advance(time.Second)
	abandoned[0].Release()
	abandoned[1].Release()
	if s := a.Allocate("a:443"); s.Pool() != 3 {
		t.Fatalf("expansion picked pool %d, want slot 3 that had a session over slot 2 that never did", s.Pool())
	}
}

// Fixed pools hold off the same way: a slot never connected is skipped while
// dials are held off.
func TestFixedModeHoldSkipsUnconnectedSlots(t *testing.T) {
	c := &clock{at: time.Unix(1_000_000, 0)}
	a := NewFixed(3)
	a.now = c.now
	first, second := connectedStream(a, "a:443"), connectedStream(a, "b:443")
	failTwice(a, c)
	for i := 0; i < 4; i++ {
		s := a.Allocate("d:443")
		if s.Pool() != first.Pool() && s.Pool() != second.Pool() {
			t.Fatalf("fixed-mode stream during the hold on pool %d, want %d or %d", s.Pool(), first.Pool(), second.Pool())
		}
	}
}

// With no pool up there is nothing to ride: failed dials do not stop the next
// stream from dialing.
func TestFailedDialWithNothingUpStillDials(t *testing.T) {
	a, c := newTestAllocator(16)
	a.Allocate("hub:443").ReleaseUnconnected()
	failTwice(a, c)
	if s := a.Allocate("hub:443"); s.Pool() != 0 {
		t.Fatalf("stream after failed dials on pool %d, want 0", s.Pool())
	}
}

// After an idle spell only the heartbeat's session is up. A burst to one
// destination rides it while the one fresh pool it would have spread onto
// dials, instead of waiting a TCP and a TLS round trip on fresh pools. Once
// that dial connects the usual caps are back: the next streams go two to a
// destination onto the new session, none onto the pool the burst crowded.
func TestBurstAfterIdleRidesTheWarmSession(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedStream(a, "probe:443").Release()
	c.advance(60 * time.Second)

	var dials []*Stream
	for i := 0; i < 8; i++ {
		s := a.Allocate("hub:443")
		if s.Pool() != 0 {
			t.Fatalf("burst stream %d on pool %d, want the warm pool 0", i, s.Pool())
		}
		if s.Warmup() != nil {
			dials = append(dials, s.Warmup())
		}
	}
	if len(dials) != 1 || dials[0].Pool() != 1 {
		t.Fatalf("burst dialed %d times, want pool 1 once", len(dials))
	}
	dials[0].Connected()
	dials[0].Release()
	for i := 0; i < 2; i++ {
		if s := a.Allocate("hub:443"); s.Pool() != 1 || s.Warmup() != nil {
			t.Fatalf("stream %d after the dial connected on pool %d dialing %v, want pool 1 and no dial",
				i, s.Pool(), dialedPool(s.Warmup()))
		}
	}
}

// A bigger burst piles no more than maxActivesWhileDialing onto the warm
// session; the rest wait for dials as before, on no more connections than the
// burst opened before (eight, two streams each).
func TestBurstOverflowIsBounded(t *testing.T) {
	a, c := newTestAllocator(16)
	connectedStream(a, "probe:443").Release()
	c.advance(60 * time.Second)

	onWarm := 0
	for i := 0; i < 16; i++ {
		if a.Allocate("hub:443").Pool() == 0 {
			onWarm++
		}
	}
	if onWarm != maxActivesWhileDialing {
		t.Fatalf("%d of 16 streams rode the warm session, want %d", onWarm, maxActivesWhileDialing)
	}
	if a.InUse() > 16/sameDestinationCap {
		t.Fatalf("burst of 16 spread over %d connections, more than the %d it opened before", a.InUse(), 16/sameDestinationCap)
	}
}

// A pool carrying bulk takes no overflow: a stream that would wait for a dial
// still waits, rather than queue behind the elephant on the one session up.
func TestNoOverflowOntoAnElephantsPool(t *testing.T) {
	a, _ := newTestAllocator(16)
	bulk := connectedStream(a, "bulk:443")
	bulk.Note(5 << 20)
	if s := a.Allocate("page:443"); s.Pool() == bulk.Pool() || s.Warmup() != nil {
		t.Fatalf("stream beside a lone elephant on pool %d dialing %v, want its own pool and no warm-up",
			s.Pool(), dialedPool(s.Warmup()))
	}
}

// Fixed pools are an explicit setting: placement stays least busy across all
// of them, and no stream rides a session in another pool's stead.
func TestFixedModeDoesNotOverflow(t *testing.T) {
	a := NewFixed(4)
	first := connectedStream(a, "a:443")
	if s := a.Allocate("b:443"); s.Pool() == first.Pool() || s.Warmup() != nil {
		t.Fatalf("fixed-mode second stream on pool %d dialing %v, want another pool and no warm-up",
			s.Pool(), dialedPool(s.Warmup()))
	}
}
