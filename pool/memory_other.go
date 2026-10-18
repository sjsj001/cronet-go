//go:build !linux

package pool

import "runtime"

// TotalMemory is unknown off Linux — every deployment is a Linux box, and Cap
// treats zero as the smallest of them.
//
// Except on iOS, where the client runs inside a network extension the system
// kills at about 50MB. The 512MB assumed for an unknown host let the adaptive
// pools hold sixteen 4MB windows there. 64MB makes Cap allow four, 16MB of
// windows; the engine's own threads and arenas still count against the limit,
// so this bounds the part that grows with load rather than guaranteeing it.
func TotalMemory() uint64 {
	if runtime.GOOS == "ios" {
		return 64 << 20
	}
	return 0
}
