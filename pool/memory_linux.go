package pool

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// TotalMemory reports the memory actually available to this process: the
// cgroup limit when one is set — inside a container /proc/meminfo shows the
// host and would wreck the pools × window invariant — and the machine total
// otherwise. The fixed paths cover cgroup-namespaced processes (containers);
// a non-namespaced systemd MemoryMax is not seen and falls back to the
// machine total. Zero means unknown; Cap then assumes the smallest
// production box.
func TotalMemory() uint64 {
	if limit := cgroupLimit(); limit > 0 {
		return limit
	}
	return meminfoTotal()
}

// absurdLimit filters the "no limit" sentinels cgroup v1 reports as a number
// (PAGE_COUNTER_MAX order of magnitude) instead of a word.
const absurdLimit = 1 << 60

func cgroupLimit() uint64 {
	for _, path := range []string{
		"/sys/fs/cgroup/memory.max",                   // v2
		"/sys/fs/cgroup/memory/memory.limit_in_bytes", // v1
	} {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(raw))
		if text == "max" {
			continue
		}
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil || value == 0 || value >= absurdLimit {
			continue
		}
		return value
	}
	return 0
}

func meminfoTotal() uint64 {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0
			}
			return kb * 1024
		}
	}
	return 0
}
