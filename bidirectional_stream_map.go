package cronet

import (
	"sync"
	"sync/atomic"
)

type bidirectionalStreamEntry struct {
	callback  BidirectionalStreamCallback
	destroyed atomic.Bool
}

var (
	bidirectionalStreamAccess sync.RWMutex
	bidirectionalStreamMap    map[uintptr]*bidirectionalStreamEntry
)

func init() {
	bidirectionalStreamMap = make(map[uintptr]*bidirectionalStreamEntry)
}

func instanceOfBidirectionalStreamCallback(ptr uintptr) BidirectionalStreamCallback {
	bidirectionalStreamAccess.RLock()
	defer bidirectionalStreamAccess.RUnlock()
	entry := bidirectionalStreamMap[ptr]
	if entry == nil || entry.destroyed.Load() {
		return nil
	}
	return entry.callback
}

func cleanupBidirectionalStream(ptr uintptr) {
	bidirectionalStreamAccess.Lock()
	delete(bidirectionalStreamMap, ptr)
	bidirectionalStreamAccess.Unlock()
}

// destroy is Destroy plus forgetting the stream's callback, which a
// BidirectionalConn always does together when its stream ends.
func (c BidirectionalStream) destroy() {
	c.Destroy()
	cleanupBidirectionalStream(c.ptr)
}
