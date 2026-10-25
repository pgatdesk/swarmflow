package lab

import (
	"os"
	"syscall"
)

// maxRSSMiB returns the peak resident set size (Linux reports ru_maxrss in KiB).
func maxRSSMiB(st *os.ProcessState) float64 {
	if ru, ok := st.SysUsage().(*syscall.Rusage); ok {
		return float64(ru.Maxrss) / 1024
	}
	return 0
}

// Supported reports whether the lab can run here.
func Supported() error {
	if os.Geteuid() != 0 {
		return errNotLinux
	}
	return nil
}
