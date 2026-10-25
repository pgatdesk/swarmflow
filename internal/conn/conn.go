// Package conn wraps an established, handshaken TCP connection to a peer. Each
// connection has a reader goroutine that delivers decoded messages to its owner
// and a writer goroutine fed by two bounded queues: a control queue (small
// messages, never blocks the sender) and a data queue (PIECE payloads, applies
// backpressure to the uploader).
package conn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

var (
	ErrClosed     = errors.New("conn: closed")
	ErrOverloaded = errors.New("conn: control queue full; peer is not reading")
)

type Config struct {
	// ControlQueue bounds queued control messages. Overflowing it closes the
	// connection, because a peer that stops reading cannot be served anyway.
	ControlQueue int
	// DataQueue bounds queued PIECE messages.
	DataQueue int
	// IdleTimeout closes the connection if nothing at all is received for this long.
	IdleTimeout time.Duration
	// WriteTimeout bounds a single frame write.
	WriteTimeout time.Duration
	// KeepAlive is how long the writer may stay silent before sending KEEPALIVE.
	KeepAlive time.Duration
}

func DefaultConfig() Config {
	return Config{
		ControlQueue: 1024,
		DataQueue:    8,
		IdleTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		KeepAlive:    10 * time.Second,
	}
}

// Inbound is what a connection delivers to its owner: a message, or exactly
// once at the end, a close notification.
type Inbound struct {
	From   *Conn
	Msg    protocol.Message
	Closed bool
	Err    error
}

type Conn struct {
	nc       net.Conn
	remote   protocol.PeerID
	listen   netip.AddrPort
	outbound bool
	flags    uint16 // remote handshake flags
	cfg      Config
	since    time.Time

	ctrl    chan protocol.Message
	data    chan protocol.Message
	closing chan struct{}
	once    sync.Once

	mu          sync.Mutex
	err         error
	reason      protocol.ByeReason
	writeFailed bool

	bytesIn, bytesOut atomic.Int64
}

// New wraps nc. listen is the remote's advertised listening address (its IP as
// seen by us plus the port from its handshake).
func New(nc net.Conn, remote protocol.PeerID, listen netip.AddrPort, outbound bool, cfg Config) *Conn {
	return &Conn{
		nc: nc, remote: remote, listen: listen, outbound: outbound, cfg: cfg, since: time.Now(),
		ctrl:    make(chan protocol.Message, cfg.ControlQueue),
		data:    make(chan protocol.Message, cfg.DataQueue),
		closing: make(chan struct{}),
	}
}

// SetRemoteFlags records the remote's handshake flags; call before Start.
func (c *Conn) SetRemoteFlags(f uint16) { c.flags = f }

// RemoteFlags returns the remote's handshake flags.
func (c *Conn) RemoteFlags() uint16 { return c.flags }

func (c *Conn) Remote() protocol.PeerID    { return c.remote }
func (c *Conn) ListenAddr() netip.AddrPort { return c.listen }
func (c *Conn) Outbound() bool             { return c.outbound }
func (c *Conn) RemoteAddr() net.Addr       { return c.nc.RemoteAddr() }
func (c *Conn) ConnectedAt() time.Time     { return c.since }
func (c *Conn) BytesIn() int64             { return c.bytesIn.Load() }
func (c *Conn) BytesOut() int64            { return c.bytesOut.Load() }
func (c *Conn) Closing() <-chan struct{}   { return c.closing }
func (c *Conn) String() string             { return c.remote.Short() + "@" + c.nc.RemoteAddr().String() }

// Err returns the reason the connection closed, or nil while open or after a
// locally initiated close.
func (c *Conn) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// Start launches the reader and writer. Messages and the final close
// notification are sent to inbound; ctx cancellation closes the connection.
func (c *Conn) Start(ctx context.Context, inbound chan<- Inbound) {
	stop := context.AfterFunc(ctx, func() { c.Close(protocol.ByeShutdown) })
	var wg sync.WaitGroup
	wg.Go(func() { c.readLoop(inbound) })
	wg.Go(c.writeLoop)
	go func() {
		wg.Wait()
		stop()
		select {
		case inbound <- Inbound{From: c, Closed: true, Err: c.Err()}:
		case <-ctx.Done():
		}
	}()
}

// Abort closes a connection that was never started.
func (c *Conn) Abort() { c.nc.Close() }

// Close asks the writer to send BYE(reason) and close the socket. It is idempotent.
func (c *Conn) Close(reason protocol.ByeReason) { c.closeWith(nil, reason, false) }

func (c *Conn) closeWith(err error, reason protocol.ByeReason, writeFailed bool) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err, c.reason, c.writeFailed = err, reason, writeFailed
		c.mu.Unlock()
		close(c.closing)
	})
}

// SendControl queues a small message without blocking.
func (c *Conn) SendControl(m protocol.Message) error {
	select {
	case <-c.closing:
		return ErrClosed
	default:
	}
	select {
	case c.ctrl <- m:
		return nil
	default:
		c.closeWith(ErrOverloaded, protocol.ByeOverloaded, false)
		return ErrOverloaded
	}
}

// SendData queues a PIECE message, blocking while the data queue is full.
func (c *Conn) SendData(ctx context.Context, m protocol.Message) error {
	select {
	case c.data <- m:
		return nil
	case <-c.closing:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Conn) readLoop(inbound chan<- Inbound) {
	br := bufio.NewReaderSize(countingReader{c.nc, &c.bytesIn}, 64<<10)
	for {
		c.nc.SetReadDeadline(time.Now().Add(c.cfg.IdleTimeout))
		m, err := protocol.ReadMessage(br)
		if err != nil {
			reason := protocol.ByeProtocolError
			switch {
			case errors.Is(err, os.ErrDeadlineExceeded):
				reason, err = protocol.ByeTimeout, fmt.Errorf("nothing received for %s", c.cfg.IdleTimeout)
			case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
				reason, err = protocol.ByeNormal, fmt.Errorf("connection closed by peer: %w", err)
			}
			c.closeWith(err, reason, false)
			return
		}
		switch m := m.(type) {
		case protocol.KeepAlive:
			continue
		case protocol.Bye:
			c.closeWith(fmt.Errorf("peer sent BYE(%s)", m.Reason), protocol.ByeNormal, false)
			return
		}
		select {
		case inbound <- Inbound{From: c, Msg: m}:
		case <-c.closing:
			return
		}
	}
}

func (c *Conn) writeLoop() {
	defer c.nc.Close()
	bw := bufio.NewWriterSize(countingWriter{c.nc, &c.bytesOut}, 64<<10)
	ka := time.NewTimer(c.cfg.KeepAlive)
	defer ka.Stop()

	write := func(m protocol.Message) bool {
		c.nc.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout))
		err := protocol.WriteMessage(bw, m)
		if err == nil && len(c.ctrl) == 0 && len(c.data) == 0 {
			err = bw.Flush()
		}
		if err != nil {
			c.closeWith(fmt.Errorf("write %s: %w", m.Type(), err), protocol.ByeNormal, true)
			return false
		}
		ka.Reset(c.cfg.KeepAlive)
		return true
	}
	sayBye := func() {
		c.mu.Lock()
		reason, failed := c.reason, c.writeFailed
		c.mu.Unlock()
		if failed {
			return
		}
		c.nc.SetWriteDeadline(time.Now().Add(time.Second))
		if protocol.WriteMessage(bw, protocol.Bye{Reason: reason}) == nil {
			bw.Flush()
		}
	}

	for {
		select {
		case <-c.closing:
			sayBye()
			return
		default:
		}
		var m protocol.Message
		select {
		case m = <-c.ctrl: // control messages take priority over bulk data
		default:
			select {
			case m = <-c.ctrl:
			case m = <-c.data:
			case <-ka.C:
				m = protocol.KeepAlive{}
			case <-c.closing:
				sayBye()
				return
			}
		}
		if !write(m) {
			return
		}
	}
}

type countingReader struct {
	r io.Reader
	n *atomic.Int64
}

func (r countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n.Add(int64(n))
	return n, err
}

type countingWriter struct {
	w io.Writer
	n *atomic.Int64
}

func (w countingWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n.Add(int64(n))
	return n, err
}
