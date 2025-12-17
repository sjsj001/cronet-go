//go:build !linux

package pool

// TotalMemory is unknown off Linux — every deployment is a Linux box, and Cap
// treats zero as the smallest of them.
func TotalMemory() uint64 {
	return 0
}
