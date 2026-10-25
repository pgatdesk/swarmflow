// Package faultnet wraps a listener so that every accepted connection's
// outgoing bytes are impaired: bandwidth-limited, stalled, or corrupted. It is
// an in-process, application-level fault injector for tests; it does not model
// packet loss or reordering (see docs/decisions/0001 #4).
package faultnet

import (
	"net"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/netx"
)

// Impairment applies to the bytes written on each accepted connection.
type Impairment struct {
	BytesPerSec int   // 0 = unlimited
	StallAfter  int64 // block all writes after this many bytes (0 = never)
	CorruptAt   int64 // flip the byte at this stream offset (0 = never)
}

type Listener struct {
	net.Listener
	imp   Impairment
	mu    sync.Mutex
	conns []net.Conn
}

// Listen binds addr and impairs every connection it accepts.
func Listen(addr string, imp Impairment) (*Listener, error) {
	ln, err := netx.Listen(addr)
	if err != nil {
		return nil, err
	}
	return &Listener{Listener: ln, imp: imp}, nil
}

func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.mu.Unlock()
	return &conn{Conn: c, imp: l.imp, closed: make(chan struct{})}, nil
}

// KillAll closes every accepted socket abruptly (RST, no protocol goodbye),
// as a crashed process would.
func (l *Listener) KillAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0)
		}
		c.Close()
	}
}

type conn struct {
	net.Conn
	imp       Impairment
	written   int64
	closeOnce sync.Once
	closed    chan struct{}
}

func (c *conn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *conn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), 4096)]
		if c.imp.StallAfter > 0 && c.written+int64(len(chunk)) > c.imp.StallAfter {
			<-c.closed
			return total, net.ErrClosed
		}
		if off := c.imp.CorruptAt; off > 0 && c.written <= off && off < c.written+int64(len(chunk)) {
			chunk = append([]byte(nil), chunk...)
			chunk[off-c.written] ^= 0xFF
		}
		if c.imp.BytesPerSec > 0 {
			time.Sleep(time.Duration(len(chunk)) * time.Second / time.Duration(c.imp.BytesPerSec))
		}
		n, err := c.Conn.Write(chunk)
		total += n
		c.written += int64(n)
		if err != nil {
			return total, err
		}
		p = p[len(chunk):]
	}
	return total, nil
}
