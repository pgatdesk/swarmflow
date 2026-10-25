// Package netx holds small networking helpers shared by peers, tests and tools.
package netx

import (
	"math/rand/v2"
	"net"
	"strconv"
	"time"
)

// Listen binds a TCP address. For port 0 it retries on failure: on Windows,
// binding an ephemeral port intermittently fails with WSAEACCES ("access
// forbidden"), apparently when the OS picks a port inside a dynamically
// reserved range (Hyper-V/WSL). It retries port 0 a few times with back-off,
// then tries explicit random ports from the dynamic range (49152–65535).
// See docs/troubleshooting.md.
func Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return ln, nil
	}
	host, port, splitErr := net.SplitHostPort(addr)
	if splitErr != nil || port != "0" {
		return nil, err
	}
	delay := 5 * time.Millisecond
	for attempt := range 20 {
		time.Sleep(delay)
		delay = min(2*delay, 200*time.Millisecond)
		try := addr
		if attempt >= 3 {
			try = net.JoinHostPort(host, strconv.Itoa(49152+rand.IntN(16384)))
		}
		if ln, lerr := net.Listen("tcp", try); lerr == nil {
			return ln, nil
		} else {
			err = lerr
		}
	}
	return nil, err
}
