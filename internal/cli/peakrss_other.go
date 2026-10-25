//go:build !linux

package cli

// peakRSSMiB is only implemented on Linux (where the lab runs).
func peakRSSMiB() float64 { return 0 }
