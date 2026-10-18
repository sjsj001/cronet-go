// Package pool decides which isolated h2 connection a new naive stream rides,
// so a bulk transfer and the interactive traffic beside it stop sharing one
// pipe. It is pure bookkeeping: the caller turns the chosen index into an
// isolation key and cronet turns that into a connection — nothing here touches
// the network.
package pool

import (
	"sync"
	"time"
)

const (
	// maxActivesPerPool is the spread threshold: a pool already carrying this
	// many streams stops being a candidate while a fresh pool is still
	// allowed. Byte counts play no part in it — at allocation time a burst of
	// bulk streams and a burst of probes look identical (zero bytes either
	// way), and a byte-gated threshold would let a simultaneous burst pile
	// into pool 0 before the first 64KB ever moves. Four matches the worst
	// case of the static four-way rotation this replaces, so a burst is never
	// spread thinner than production spreads it today.
	maxActivesPerPool = 4

	// A stream that moved more than elephantBytes within the trailing
	// elephantWindow is an elephant: new streams avoid its pool. The window
	// is also the demotion: a stream quiet for the whole window sums to zero
	// and stops being one, so a finished download does not pin its pool
	// forever — a long-lived connection must be able to shed the label.
	elephantWindow = 10 * time.Second
	elephantBytes  = 4 << 20

	bucketCount = 10 // elephantWindow sliced into one-second buckets

	// A parallel downloader's segments all dial the same destination, and
	// that is the one workload where packing streams onto one connection
	// caps the aggregate at a single TCP flow's ceiling. Two per connection
	// spreads exactly those; a page burst dials many destinations and still
	// packs tight.
	sameDestinationCap = 2

	// Concurrent traffic rides at least this many connections before any
	// packing: one pipe is a single point of failure — a loss episode or a
	// dying session stalls everything on it — and a second live pipe is
	// cheap insurance. A lone stream still means one connection, and idle
	// still shrinks to none.
	minSpreadPools = 2

	// An empty slot whose last stream ended within this window almost
	// certainly still has a live session behind it — the server reclaims
	// idle connections at 120s — so expanding onto it costs nothing. Beyond
	// the window the slot is a fresh handshake like any other.
	warmSlotWindow = 110 * time.Second

	// While a fresh pool dials, a pool with a session takes the streams it
	// would get, up to this many, sparing them a TCP and a TLS round trip. The
	// bound is how much zero-byte bulk can pile onto one connection for good.
	maxActivesWhileDialing = 2 * maxActivesPerPool

	// After a second failed dial within dialFailureWindow, new streams take
	// only slots that need no dial for dialHoldOff; see DialFailed. Failures
	// closer together than dialFailureGap are one: Chromium races a backup
	// connection against a slow first one, and both time out together.
	dialHoldOff       = 30 * time.Second
	dialFailureWindow = time.Minute
	dialFailureGap    = time.Second

	minPoolCap = 2
	maxPoolCap = 16

	// unknownMemoryBudget stands in when the host's memory cannot be read:
	// the budget of a 2GB box, the smallest this runs on in production.
	unknownMemoryBudget = 512 << 20
)

// Cap derives the pool ceiling from the memory budget and the session
// receive window, keeping pools × window ≤ RAM/4 an invariant: the window is
// what one connection can force this client to buffer, so the ceiling shrinks
// as the window grows. Clamped to [2, 16]; 16 doubles as the fingerprint
// bound.
func Cap(totalMemory, sessionWindow uint64) int {
	if sessionWindow == 0 {
		return minPoolCap
	}
	budget := totalMemory / 4
	if totalMemory == 0 {
		budget = unknownMemoryBudget
	}
	n := budget / sessionWindow
	if n < minPoolCap {
		return minPoolCap
	}
	if n > maxPoolCap {
		return maxPoolCap
	}
	return int(n)
}

// Allocator places streams into pools. The zero value is not usable; construct
// with NewAdaptive or NewFixed.
type Allocator struct {
	mu       sync.Mutex
	adaptive bool
	ceiling  int
	pools    []*poolState
	now      func() time.Time // fixed in tests to travel time
	// Until then, place streams on slots with a session behind them.
	dialsHeldUntil  time.Time
	lastDialFailure time.Time
}

type poolState struct {
	streams      map[*Stream]struct{}
	lastActiveAt time.Time
}

// NewAdaptive grows and shrinks the pool set on demand, up to ceiling.
func NewAdaptive(ceiling int) *Allocator {
	if ceiling < 1 {
		ceiling = minPoolCap
	}
	return &Allocator{adaptive: true, ceiling: ceiling, now: time.Now}
}

// NewFixed keeps exactly n pools and never grows past them — the escape hatch
// that preserves the semantics of an explicit insecure_concurrency. Placement
// stays least-busy so all n stay warm, minus the elephant and
// same-destination guards.
func NewFixed(n int) *Allocator {
	if n < 1 {
		n = 1
	}
	return &Allocator{adaptive: false, ceiling: n, now: time.Now}
}

// Stream is one placed stream. Feed it Note as bytes move and Release it when
// the stream ends; both are safe from any goroutine.
type Stream struct {
	alloc       *Allocator
	pool        int
	destination string
	mu          sync.Mutex
	released    bool
	connected   bool // its request reached the wire: the slot has a session
	warmup      *Stream
	buckets     [bucketCount]uint64
	bucketAt    [bucketCount]int64
}

// Allocate places a new stream toward destination and never fails: worst case
// every pool is busy and it returns the least loaded one. Placement is
// registration — the chosen pool's count is up before Allocate returns, so a
// concurrent burst cannot all see the same empty pool.
func (a *Allocator) Allocate(destination string) *Stream {
	a.mu.Lock()
	defer a.mu.Unlock()
	index := a.chooseLocked(destination)
	for len(a.pools) <= index {
		a.pools = append(a.pools, &poolState{streams: make(map[*Stream]struct{})})
	}
	stream := &Stream{alloc: a, pool: index, destination: destination}
	if now := a.now(); a.adaptive && !dialFreeLocked(a.pools[index], now) {
		hot := -1
		for i, state := range a.pools {
			if len(state.streams) < maxActivesWhileDialing && dialFreeLocked(state, now) &&
				!a.poolHasElephantLocked(state, now) && (hot == -1 || len(state.streams) < len(a.pools[hot].streams)) {
				hot = i
			}
		}
		if hot != -1 {
			if len(a.pools[index].streams) == 0 {
				stream.warmup = &Stream{alloc: a, pool: index}
				a.pools[index].streams[stream.warmup] = struct{}{}
			}
			stream.pool = hot
		}
	}
	a.pools[stream.pool].streams[stream] = struct{}{}
	return stream
}

func (a *Allocator) chooseLocked(destination string) int {
	if now := a.now(); now.Before(a.dialsHeldUntil) {
		if index := a.chooseDialFreeLocked(destination, now); index != -1 {
			return index
		}
	}
	if !a.adaptive {
		// Fixed means N warm pipes, so spread over all of them before packing
		// — that is what an explicit setting buys. The elephant and
		// same-destination guards
		// ride along: they cost nothing while every pipe is quiet, and are
		// the whole point when one is not.
		if index := a.leastBusyAvoidingLocked(destination, a.now()); index != -1 {
			return index
		}
		return a.leastBusyLocked()
	}
	now := a.now()
	// An in-use pool without an elephant that is not already carrying this
	// destination in parallel, the emptiest one, lowest index on ties — the
	// low bias is what lets high pools drain and be reclaimed.
	best := -1
	inUse := 0
	for i, state := range a.pools {
		if len(state.streams) == 0 {
			continue
		}
		inUse++
		if a.poolHasElephantLocked(state, now) {
			continue
		}
		if destinationCountLocked(state, destination) >= sameDestinationCap {
			continue
		}
		if best == -1 || len(state.streams) < len(a.pools[best].streams) {
			best = i
		}
	}
	if best != -1 && len(a.pools[best].streams) < maxActivesPerPool && inUse >= minSpreadPools {
		return best
	}
	// Nothing acceptable in use: expand. A recently-drained slot very likely
	// still has a live session behind it, which makes it a free spread — take
	// the lowest of those before paying a fresh handshake on a cold slot.
	warm, cold := -1, -1
	for i := 0; i < a.ceiling; i++ {
		if i < len(a.pools) && len(a.pools[i].streams) > 0 {
			continue
		}
		if i < len(a.pools) && now.Sub(a.pools[i].lastActiveAt) < warmSlotWindow {
			if warm == -1 {
				warm = i
			}
		} else if cold == -1 {
			cold = i
		}
	}
	if warm != -1 {
		return warm
	}
	if cold != -1 {
		return cold
	}
	// At the ceiling. A loaded pool without an elephant still beats one with:
	// the elephant's pool is the least busy only because the scan above left
	// it out, and sending new streams there put probes and interactive
	// traffic behind bulk exactly when the pools were busiest.
	if best != -1 {
		return best
	}
	// Every pool elephant-ridden: degrade to the least busy pipe rather than
	// refusing. This is the documented worst case — full load shares with
	// bulk, exactly like today.
	return a.leastBusyLocked()
}

// chooseDialFreeLocked places a stream while dials are held off: on the least
// busy slot that needs none, one without an elephant or a parallel stream to
// this destination first. -1 when every slot would need a dial.
func (a *Allocator) chooseDialFreeLocked(destination string, now time.Time) int {
	best, fallback := -1, -1
	for i, state := range a.pools {
		if i >= a.ceiling || !dialFreeLocked(state, now) {
			continue
		}
		if fallback == -1 || len(state.streams) < len(a.pools[fallback].streams) {
			fallback = i
		}
		if a.poolHasElephantLocked(state, now) ||
			destinationCountLocked(state, destination) >= sameDestinationCap {
			continue
		}
		if best == -1 || len(state.streams) < len(a.pools[best].streams) {
			best = i
		}
	}
	if best != -1 {
		return best
	}
	return fallback
}

// dialFreeLocked reports whether a slot has a session behind it: a stream on
// it connected, or one that had drained from it within warmSlotWindow. A slot
// whose streams are all still dialing does not count.
func dialFreeLocked(state *poolState, now time.Time) bool {
	for stream := range state.streams {
		if stream.connected {
			return true
		}
	}
	return !state.lastActiveAt.IsZero() && now.Sub(state.lastActiveAt) < warmSlotWindow
}

func destinationCountLocked(state *poolState, destination string) int {
	count := 0
	for stream := range state.streams {
		if stream.destination == destination {
			count++
		}
	}
	return count
}

// leastBusyAvoidingLocked is leastBusyLocked restricted to pools carrying no
// elephant and with room for one more stream to this destination. Returns -1
// when every pool is excluded, so the caller degrades instead of refusing.
func (a *Allocator) leastBusyAvoidingLocked(destination string, now time.Time) int {
	best, bestActives := -1, -1
	for i := 0; i < a.ceiling; i++ {
		actives := 0
		if i < len(a.pools) {
			state := a.pools[i]
			if a.poolHasElephantLocked(state, now) {
				continue
			}
			if destinationCountLocked(state, destination) >= sameDestinationCap {
				continue
			}
			actives = len(state.streams)
		}
		if bestActives == -1 || actives < bestActives {
			best, bestActives = i, actives
		}
	}
	return best
}

func (a *Allocator) leastBusyLocked() int {
	best := 0
	bestActives := -1
	for i := 0; i < a.ceiling; i++ {
		actives := 0
		if i < len(a.pools) {
			actives = len(a.pools[i].streams)
		}
		if bestActives == -1 || actives < bestActives {
			best, bestActives = i, actives
		}
	}
	return best
}

func (a *Allocator) poolHasElephantLocked(state *poolState, now time.Time) bool {
	for stream := range state.streams {
		if stream.windowedBytes(now) > elephantBytes {
			return true
		}
	}
	return false
}

// InUse reports how many pools currently carry at least one stream, for
// integration tests and diagnostics. Never below one: an idle client is one
// dial away from pool 0.
func (a *Allocator) InUse() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	inUse := 0
	for _, state := range a.pools {
		if len(state.streams) > 0 {
			inUse++
		}
	}
	if inUse < 1 {
		return 1
	}
	return inUse
}

// Pool is the index this stream was placed on.
func (s *Stream) Pool() int {
	return s.pool
}

// Warmup, if not nil, is placed on the pool this stream would have dialed: the
// caller opens it to the server and drops it once connected.
func (s *Stream) Warmup() *Stream {
	return s.warmup
}

// Note records n bytes moving on this stream, in either direction.
func (s *Stream) Note(n int) {
	if n <= 0 {
		return
	}
	second := s.alloc.now().Unix()
	index := int(second % bucketCount)
	s.mu.Lock()
	if s.bucketAt[index] != second {
		s.bucketAt[index] = second
		s.buckets[index] = 0
	}
	s.buckets[index] += uint64(n)
	s.mu.Unlock()
}

func (s *Stream) windowedBytes(now time.Time) uint64 {
	oldest := now.Add(-elephantWindow).Unix()
	s.mu.Lock()
	defer s.mu.Unlock()
	var sum uint64
	for i := 0; i < bucketCount; i++ {
		if s.bucketAt[i] > oldest {
			sum += s.buckets[i]
		}
	}
	return sum
}

// Connected records that the stream's request reached the wire, so its slot
// has a session.
func (s *Stream) Connected() {
	s.alloc.mu.Lock()
	s.connected = true
	s.alloc.mu.Unlock()
}

// Release returns the stream's slot. Idempotent, because close paths overlap.
// Only a stream that connected leaves its slot warm: one dropped while its
// dial was still under way says nothing of a session there.
func (s *Stream) Release() {
	a := s.alloc
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	state := a.pools[s.pool]
	delete(state.streams, s)
	if s.connected {
		state.lastActiveAt = a.now()
	}
}

// ReleaseUnconnected is Release for a stream that ended on the error of a
// failed connection attempt: its slot is left cold, since no session came of
// it, even if one drained from it recently.
func (s *Stream) ReleaseUnconnected() {
	a := s.alloc
	a.mu.Lock()
	defer a.mu.Unlock()
	if s.released {
		return
	}
	s.released = true
	state := a.pools[s.pool]
	delete(state.streams, s)
	state.lastActiveAt = time.Time{}
}

// DialFailed records a failed dial to the server. Behind a bottleneck that
// queues for longer than a dial may take, a fresh connection's handshake
// waits in that queue and every dial times out the same way, while the
// connections already up still answer, slowly. A second failure within
// dialFailureWindow holds dials off for dialHoldOff: new streams ride the
// slots that have a session instead of dialing again. One failure alone does
// not, since it may be a passing one. It is counted where the dial fails, not
// where a stream does: a caller racing several paths drops a stream that
// waits on a slow dial long before the dial gives up.
func (a *Allocator) DialFailed() {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if !a.lastDialFailure.IsZero() && now.Sub(a.lastDialFailure) < dialFailureGap {
		return
	}
	if !a.lastDialFailure.IsZero() && now.Sub(a.lastDialFailure) < dialFailureWindow {
		a.dialsHeldUntil = now.Add(dialHoldOff)
	}
	a.lastDialFailure = now
}
