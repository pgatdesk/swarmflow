// Package netx holds small networking helpers shared by peers, tests and tools.
package netx

import "net"

// Listen binds a TCP address. For port 0 it retries a few times: on Windows,
// binding an ephemeral port intermittently fails with WSAEACCES when the OS
// picks a port inside an administered exclusion range (for example ranges
// reserved by Hyper-V or WSL).
func Listen(addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return ln, nil
	}
	if _, port, splitErr := net.SplitHostPort(addr); splitErr != nil || port != "0" {
		return nil, err
	}
	for range 4 {
		if ln, err = net.Listen("tcp", addr); err == nil {
			return ln, nil
		}
	}
	return nil, err
}
