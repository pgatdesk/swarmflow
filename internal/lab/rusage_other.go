//go:build !linux

package lab

import "os"

func maxRSSMiB(*os.ProcessState) float64 { return 0 }

// Supported reports whether the lab can run here.
func Supported() error { return errNotLinux }
