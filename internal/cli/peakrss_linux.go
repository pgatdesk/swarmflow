package cli

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// peakRSSMiB returns this process's peak resident set size (VmHWM). Unlike the
// parent's rusage, it excludes memory a parent shared before exec.
func peakRSSMiB() float64 {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "VmHWM:"); ok {
			kb, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(rest), "kB")), 64)
			if err == nil {
				return kb / 1024
			}
		}
	}
	return 0
}
