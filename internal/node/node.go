// Package node is a peer process's network edge: it owns the listening socket,
// performs handshakes in both directions, runs discovery, and hands established
// connections to the coordinator of the matching swarm.
package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/conn"
	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/discovery"
	"github.com/pgatdesk/swarmflow/internal/netx"
	"github.com/pgatdesk/swarmflow/internal/protocol"
)

type Config struct {
	ListenAddr string
	// Listener, if set, is used instead of listening on ListenAddr (tests inject
	// impaired listeners this way).
	Listener net.Listener
	// AdvertiseIP is sent to the tracker instead of the address it observes.
	AdvertiseIP string
	// AdvertisePort is sent in the handshake and to the tracker instead of the
	// listener's port (for example when peers must reach us through a proxy).
	AdvertisePort uint16
	APIPort       uint16
	TrackerURL    string
	StaticPeers   []string
	// HintPeers are addresses remembered from earlier runs; they are dialed
	// only while a swarm has no connected peers.
	HintPeers []string
	// AcceptReplicas sets FlagAcceptReplicas in our handshakes.
	AcceptReplicas   bool
	MaxPeers         int
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
	AnnounceInterval time.Duration
	// LonelyInterval is the shorter re-discovery interval used while a swarm has no peers.
	LonelyInterval time.Duration
	MinBackoff     time.Duration
	MaxBackoff     time.Duration
	MaxHandshakes  int
	Conn           conn.Config
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:       "127.0.0.1:0",
		MaxPeers:         50,
		DialTimeout:      5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		AnnounceInterval: 30 * time.Second,
		LonelyInterval:   5 * time.Second,
		MinBackoff:       time.Second,
		MaxBackoff:       60 * time.Second,
		MaxHandshakes:    64,
		Conn:             conn.DefaultConfig(),
	}
}

type Node struct {
	cfg  Config
	id   protocol.PeerID
	ln   net.Listener
	port uint16
	log  *slog.Logger
	wg   sync.WaitGroup
	hsem chan struct{}

	mu       sync.Mutex
	swarms   map[protocol.SwarmID]*coordinator.Coordinator
	pex      map[protocol.SwarmID]chan []netip.AddrPort
	dialing  map[string]bool
	addrPeer map[string]protocol.PeerID // learned from successful handshakes
	selfAddr map[string]bool
}

func New(cfg Config, log *slog.Logger) (*Node, error) {
	var id protocol.PeerID
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	ln := cfg.Listener
	if ln == nil {
		var err error
		if ln, err = netx.Listen(cfg.ListenAddr); err != nil {
			return nil, err
		}
	}
	ap, err := netip.ParseAddrPort(ln.Addr().String())
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("node: listener address: %w", err)
	}
	port := ap.Port()
	if cfg.AdvertisePort != 0 {
		port = cfg.AdvertisePort
	}
	return &Node{
		cfg: cfg, id: id, ln: ln, port: port,
		log:      log.With("self", id.Short()),
		hsem:     make(chan struct{}, max(cfg.MaxHandshakes, 1)),
		swarms:   make(map[protocol.SwarmID]*coordinator.Coordinator),
		pex:      make(map[protocol.SwarmID]chan []netip.AddrPort),
		dialing:  make(map[string]bool),
		addrPeer: make(map[string]protocol.PeerID),
		selfAddr: make(map[string]bool),
	}, nil
}

func (n *Node) ID() protocol.PeerID  { return n.id }
func (n *Node) Addr() net.Addr       { return n.ln.Addr() }
func (n *Node) Logger() *slog.Logger { return n.log }

// AddSwarm registers a coordinator. Call before Run.
func (n *Node) AddSwarm(co *coordinator.Coordinator) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.swarms[co.ID()] = co
	ch := make(chan []netip.AddrPort, 64)
	n.pex[co.ID()] = ch
	co.OnDiscovered(func(addrs []netip.AddrPort) {
		select {
		case ch <- addrs:
		default: // discovery is behind; drop rather than block the coordinator
		}
	})
}

func (n *Node) swarm(id protocol.SwarmID) *coordinator.Coordinator {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.swarms[id]
}

// Run accepts connections and runs discovery until ctx is cancelled.
func (n *Node) Run(ctx context.Context) error {
	n.log.Info("listening", "addr", n.ln.Addr().String(), "advertised_port", n.port)
	n.wg.Go(func() { n.acceptLoop(ctx) })
	n.mu.Lock()
	for id, co := range n.swarms {
		pex := n.pex[id]
		n.wg.Go(func() { n.discoverLoop(ctx, co) })
		n.wg.Go(func() { n.pexLoop(ctx, co, pex) })
	}
	n.mu.Unlock()
	<-ctx.Done()
	n.ln.Close()
	n.wg.Wait()
	return nil
}

func (n *Node) acceptLoop(ctx context.Context) {
	for {
		nc, err := n.ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			n.log.Warn("accept failed", "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		select {
		case n.hsem <- struct{}{}:
		default:
			n.log.Warn("too many concurrent handshakes; dropping connection", "remote", nc.RemoteAddr().String())
			nc.Close()
			continue
		}
		n.wg.Go(func() {
			defer func() { <-n.hsem }()
			if err := n.acceptHandshake(ctx, nc); err != nil {
				n.log.Info("inbound handshake failed", "remote", nc.RemoteAddr().String(), "err", err)
			}
		})
	}
}

func (n *Node) handshake(s protocol.SwarmID) protocol.Handshake {
	var flags uint16
	if n.cfg.AcceptReplicas {
		flags |= protocol.FlagAcceptReplicas
	}
	return protocol.Handshake{Major: protocol.VersionMajor, Minor: protocol.VersionMinor, Flags: flags,
		Swarm: s, Peer: n.id, ListenPort: n.port}
}

func (n *Node) newConn(nc net.Conn, h protocol.Handshake, outbound bool) *conn.Conn {
	c := conn.New(nc, h.Peer, listenAddr(nc, h.ListenPort), outbound, n.cfg.Conn)
	c.SetRemoteFlags(h.Flags)
	return c
}

func (n *Node) acceptHandshake(ctx context.Context, nc net.Conn) error {
	nc.SetDeadline(time.Now().Add(n.cfg.HandshakeTimeout))
	theirs, err := protocol.ReadHandshake(nc)
	if err != nil {
		nc.Close()
		return err
	}
	reject := func(reason protocol.ByeReason) error {
		protocol.WriteHandshake(nc, n.handshake(protocol.SwarmID{}))
		protocol.WriteMessage(nc, protocol.Bye{Reason: reason})
		nc.Close()
		return fmt.Errorf("rejected: %s", reason)
	}
	if theirs.Major != protocol.VersionMajor {
		return reject(protocol.ByeIncompatibleVersion)
	}
	if theirs.Peer == n.id {
		return reject(protocol.ByeSelf)
	}
	co := n.swarm(theirs.Swarm)
	if co == nil {
		return reject(protocol.ByeUnknownSwarm)
	}
	if err := protocol.WriteHandshake(nc, n.handshake(theirs.Swarm)); err != nil {
		nc.Close()
		return err
	}
	nc.SetDeadline(time.Time{})
	co.AddConn(ctx, n.newConn(nc, theirs, false))
	return nil
}

// Dial connects to addr for swarm co. It is exported for tests and tools.
func (n *Node) Dial(ctx context.Context, co *coordinator.Coordinator, addr string) error {
	d := net.Dialer{Timeout: n.cfg.DialTimeout}
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	nc.SetDeadline(time.Now().Add(n.cfg.HandshakeTimeout))
	if err := protocol.WriteHandshake(nc, n.handshake(co.ID())); err != nil {
		nc.Close()
		return err
	}
	theirs, err := protocol.ReadHandshake(nc)
	if err != nil {
		nc.Close()
		return fmt.Errorf("reading handshake: %w", err)
	}
	if theirs.Major != protocol.VersionMajor {
		nc.Close()
		return fmt.Errorf("incompatible protocol version %d.%d", theirs.Major, theirs.Minor)
	}
	if theirs.Swarm != co.ID() {
		reason := "unknown"
		if m, err := protocol.ReadMessage(nc); err == nil {
			if b, ok := m.(protocol.Bye); ok {
				reason = b.Reason.String()
			}
		}
		nc.Close()
		if reason == protocol.ByeSelf.String() {
			n.mu.Lock()
			n.selfAddr[addr] = true
			n.mu.Unlock()
		}
		return fmt.Errorf("handshake rejected by peer: %s", reason)
	}
	if theirs.Peer == n.id {
		nc.Close()
		n.mu.Lock()
		n.selfAddr[addr] = true
		n.mu.Unlock()
		return errors.New("connected to self")
	}
	nc.SetDeadline(time.Time{})
	n.mu.Lock()
	n.addrPeer[addr] = theirs.Peer
	n.mu.Unlock()
	co.AddConn(ctx, n.newConn(nc, theirs, true))
	return nil
}

func listenAddr(nc net.Conn, port uint16) netip.AddrPort {
	ap, err := netip.ParseAddrPort(nc.RemoteAddr().String())
	if err != nil {
		return netip.AddrPort{}
	}
	return netip.AddrPortFrom(ap.Addr().Unmap(), port)
}

// maybeDial dials addr in the background unless it is already connected,
// being dialed, known to be ourselves, or the swarm is at its peer limit.
func (n *Node) maybeDial(ctx context.Context, co *coordinator.Coordinator, addr string, id *protocol.PeerID) {
	if co.PeerCount() >= n.cfg.MaxPeers {
		return
	}
	if id != nil && (*id == n.id || co.Connected(*id)) {
		return
	}
	n.mu.Lock()
	if known, ok := n.addrPeer[addr]; n.dialing[addr] || n.selfAddr[addr] || (ok && co.Connected(known)) {
		n.mu.Unlock()
		return
	}
	n.dialing[addr] = true
	n.mu.Unlock()
	n.wg.Go(func() {
		defer func() {
			n.mu.Lock()
			delete(n.dialing, addr)
			n.mu.Unlock()
		}()
		if err := n.Dial(ctx, co, addr); err != nil && ctx.Err() == nil {
			n.log.Info("dial failed", "addr", addr, "err", err)
		}
	})
}

// pexLoop dials addresses learned from peers.
func (n *Node) pexLoop(ctx context.Context, co *coordinator.Coordinator, ch <-chan []netip.AddrPort) {
	for {
		select {
		case <-ctx.Done():
			return
		case addrs := <-ch:
			for _, a := range addrs {
				n.maybeDial(ctx, co, a.String(), nil)
			}
		}
	}
}

func (n *Node) discoverLoop(ctx context.Context, co *coordinator.Coordinator) {
	var client *discovery.Client
	if n.cfg.TrackerURL != "" {
		client = discovery.NewClient(n.cfg.TrackerURL)
	}
	req := discovery.AnnounceRequest{
		SwarmID: co.ID().String(), PeerID: n.id.String(), Port: n.port,
		IP: n.cfg.AdvertiseIP, APIPort: n.cfg.APIPort, Event: discovery.EventStarted,
	}
	backoff := n.cfg.MinBackoff
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			if client != nil {
				sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				req.Event = discovery.EventStopped
				client.Announce(sctx, req)
				cancel()
			}
			return
		case <-timer.C:
		}

		wait := n.cfg.AnnounceInterval
		for _, addr := range n.cfg.StaticPeers {
			n.maybeDial(ctx, co, addr, nil)
		}
		if co.PeerCount() == 0 {
			for _, addr := range n.cfg.HintPeers {
				n.maybeDial(ctx, co, addr, nil)
			}
		}
		trackerOK := true
		if client != nil {
			resp, err := client.Announce(ctx, req)
			switch {
			case err != nil && ctx.Err() == nil:
				trackerOK = false
				wait = jitter(backoff)
				n.log.Warn("tracker announce failed", "err", err, "retry_in", wait.Round(time.Millisecond))
				backoff = min(backoff*2, n.cfg.MaxBackoff)
			case err == nil:
				req.Event = ""
				backoff = n.cfg.MinBackoff
				if resp.IntervalSec > 0 {
					wait = time.Duration(resp.IntervalSec) * time.Second
				}
				for _, p := range resp.Peers {
					id, err := protocol.ParsePeerID(p.PeerID)
					if err != nil {
						continue
					}
					n.maybeDial(ctx, co, p.Addr, &id)
				}
			}
		}
		if trackerOK && co.PeerCount() == 0 {
			wait = min(wait, n.cfg.LonelyInterval)
		}
		timer.Reset(wait)
	}
}

// jitter spreads retries over [d/2, d) so restarted peers do not retry in lockstep.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(mrand.Int64N(int64(d/2)+1))
}
