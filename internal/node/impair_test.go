package node

import (
	"net"
	"sync"
	"testing"
	"time"
)

// impairment shapes the bytes a peer writes on accepted connections. It is an
// in-process stand-in for the faultproxy used by the lab.
type impairment struct {
	bytesPerSec int   // 0 = unlimited
	stallAfter  int64 // block all writes after this many bytes (0 = never)
	corruptAt   int64 // flip the byte at this stream offset (0 = never)
}

type impairedListener struct {
	net.Listener
	imp   impairment
	mu    sync.Mutex
	conns []net.Conn
}

func newImpairedListener(t *testing.T, imp impairment) *impairedListener {
	ln, err := listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return &impairedListener{Listener: ln, imp: imp}
}

func (l *impairedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.conns = append(l.conns, c)
	l.mu.Unlock()
	return &impairedConn{Conn: c, imp: l.imp, closed: make(chan struct{})}, nil
}

// killAll closes every accepted socket without any protocol goodbye.
func (l *impairedListener) killAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		if tc, ok := c.(*net.TCPConn); ok {
			tc.SetLinger(0) // send RST rather than FIN
		}
		c.Close()
	}
}

type impairedConn struct {
	net.Conn
	imp       impairment
	written   int64
	closeOnce sync.Once
	closed    chan struct{}
}

func (c *impairedConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func (c *impairedConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		chunk := p[:min(len(p), 4096)]
		if c.imp.stallAfter > 0 && c.written+int64(len(chunk)) > c.imp.stallAfter {
			<-c.closed
			return total, net.ErrClosed
		}
		if off := c.imp.corruptAt; off > 0 && c.written <= off && off < c.written+int64(len(chunk)) {
			chunk = append([]byte(nil), chunk...)
			chunk[off-c.written] ^= 0xFF
		}
		if c.imp.bytesPerSec > 0 {
			time.Sleep(time.Duration(len(chunk)) * time.Second / time.Duration(c.imp.bytesPerSec))
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
