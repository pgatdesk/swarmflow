// Package coordinator runs one swarm: it owns all piece and request state in a
// single goroutine, asks the scheduler what to fetch, serves uploads, and hands
// received pieces to the verify pool. Because exactly one goroutine mutates the
// state, duplicate arrivals, cancellations and disconnects can never race.
package coordinator

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/conn"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
	"github.com/pgatdesk/swarmflow/internal/store"
	"github.com/pgatdesk/swarmflow/internal/verify"
)

type Config struct {
	// MaxOutstanding bounds requests in flight plus pieces awaiting verification,
	// which also bounds buffered piece memory to MaxOutstanding × piece size.
	MaxOutstanding int
	// MaxPerPeer bounds requests in flight to a single peer.
	MaxPerPeer int
	// UploadSlots bounds queued uploads per remote peer; beyond it we REJECT busy.
	UploadSlots    int
	RequestTimeout time.Duration
	VerifyWorkers  int
	MaxPeers       int
	Picker         scheduler.PiecePicker
	Selector       scheduler.PeerSelector
}

func DefaultConfig() Config {
	return Config{
		MaxOutstanding: 32,
		MaxPerPeer:     4,
		UploadSlots:    4,
		RequestTimeout: 10 * time.Second,
		VerifyWorkers:  2,
		MaxPeers:       50,
		Picker:         scheduler.Sequential{},
		Selector:       scheduler.NewRandomPeer(1),
	}
}

type pieceState uint8

const (
	stMissing pieceState = iota
	stRequested
	stVerifying // hashing and durable write in progress
	stPersisted
)

type request struct {
	id       uint32
	piece    int
	peer     *peer
	sent     time.Time
	deadline time.Time
}

type peer struct {
	id          protocol.PeerID
	c           *conn.Conn
	has         bitfield.Bitfield
	gotBitfield bool
	closing     bool
	outstanding map[uint32]*request
	up          *uploader
	stopUpload  context.CancelFunc

	piecesFrom, invalid, timeouts, rejects, duplicates, unexpected int
	bytesFrom                                                      int64
}

type Coordinator struct {
	cfg  Config
	m    *manifest.Manifest
	st   *store.Store
	self protocol.PeerID
	log  *slog.Logger

	addCh    chan *conn.Conn
	inbound  chan conn.Inbound
	pool     *verify.Pool
	done     chan struct{}
	complete chan struct{}

	connected sync.Map // protocol.PeerID → netip.AddrPort; read by the node for dialing decisions
	peerCount atomic.Int32
	bytesUp   atomic.Int64
	status    atomic.Pointer[Status]

	// Everything below is owned by the Run goroutine.
	have      bitfield.Bitfield
	state     []pieceState
	inflight  []int
	avail     []int
	peers     map[protocol.PeerID]*peer
	peerList  []*peer // connection order, for deterministic candidate ordering
	reqs      map[uint32]*request
	nextReq   uint32
	verifying int
	// downloading is false for read-only stores (seed mode serves a fixed,
	// possibly partial, file and never fetches).
	downloading bool
	started     time.Time
	finished    time.Time
	bytesDown   int64
	rejected    int
}

// New creates a coordinator for manifest m backed by st. have is the set of
// pieces already verified on disk.
func New(m *manifest.Manifest, st *store.Store, have bitfield.Bitfield, self protocol.PeerID, cfg Config, log *slog.Logger) *Coordinator {
	n := m.NumPieces()
	co := &Coordinator{
		cfg: cfg, m: m, st: st, self: self,
		log:      log.With("swarm", m.ID().Short()),
		addCh:    make(chan *conn.Conn),
		inbound:  make(chan conn.Inbound, 256),
		done:     make(chan struct{}),
		complete: make(chan struct{}),
		have:     have,
		state:    make([]pieceState, n),
		inflight: make([]int, n),
		avail:    make([]int, n),
		peers:    make(map[protocol.PeerID]*peer),
		reqs:     make(map[uint32]*request),

		downloading: st.Writable(),
	}
	for i := range n {
		if have.Has(i) {
			co.state[i] = stPersisted
		}
	}
	co.pool = verify.NewPool(m, st.WritePiece, cfg.VerifyWorkers, cfg.MaxOutstanding)
	co.publishStatus()
	return co
}

func (co *Coordinator) ID() protocol.SwarmID         { return co.m.ID() }
func (co *Coordinator) Manifest() *manifest.Manifest { return co.m }
func (co *Coordinator) Complete() <-chan struct{}    { return co.complete }
func (co *Coordinator) Done() <-chan struct{}        { return co.done }
func (co *Coordinator) PeerCount() int               { return int(co.peerCount.Load()) }
func (co *Coordinator) Status() *Status              { return co.status.Load() }

func (co *Coordinator) Connected(id protocol.PeerID) bool {
	_, ok := co.connected.Load(id)
	return ok
}

// AddConn hands a handshaken connection to the coordinator.
func (co *Coordinator) AddConn(ctx context.Context, c *conn.Conn) {
	select {
	case co.addCh <- c:
	case <-co.done:
		c.Abort()
	case <-ctx.Done():
		c.Abort()
	}
}

// Run is the coordinator's event loop. It returns when ctx is cancelled.
func (co *Coordinator) Run(ctx context.Context) error {
	defer close(co.done)
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait() // verify workers must stop before the caller closes the store
	defer cancel()
	wg.Go(func() { co.pool.Run(ctx) })

	co.started = time.Now()
	if co.have.Complete() {
		co.finished = co.started
		close(co.complete)
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			for _, p := range co.peerList {
				p.c.Close(protocol.ByeShutdown)
			}
			co.publishStatus()
			return nil
		case c := <-co.addCh:
			co.onConn(ctx, c)
		case in := <-co.inbound:
			p := co.peers[in.From.Remote()]
			if p == nil || p.c != in.From {
				break // a connection we already dropped (duplicate, overload)
			}
			if in.Closed {
				co.removePeer(p, in.Err)
			} else if !p.closing {
				co.onMessage(p, in.Msg)
			}
		case r := <-co.pool.Results():
			co.onVerified(r)
		case now := <-tick.C:
			co.checkTimeouts(now)
			co.publishStatus()
		}
		co.schedule(time.Now())
	}
}

func (co *Coordinator) onConn(ctx context.Context, c *conn.Conn) {
	reject := func(reason protocol.ByeReason) {
		c.Start(ctx, co.inbound) // so BYE is delivered; the close notice is then ignored
		c.Close(reason)
	}
	id := c.Remote()
	if old, ok := co.peers[id]; ok {
		if !co.preferNew(old.c, c) {
			reject(protocol.ByeDuplicate)
			return
		}
		old.c.Close(protocol.ByeDuplicate)
		co.removePeer(old, nil)
	}
	if len(co.peers) >= co.cfg.MaxPeers {
		reject(protocol.ByeOverloaded)
		return
	}
	upCtx, stop := context.WithCancel(ctx)
	p := &peer{
		id: id, c: c, has: bitfield.New(co.m.NumPieces()),
		outstanding: make(map[uint32]*request),
		stopUpload:  stop,
	}
	p.up = newUploader(c, co.st, co.cfg.UploadSlots, &co.bytesUp, co.log)
	co.peers[id] = p
	co.peerList = append(co.peerList, p)
	co.connected.Store(id, c.ListenAddr())
	co.peerCount.Store(int32(len(co.peers)))
	go p.up.run(upCtx)
	c.Start(ctx, co.inbound)
	c.SendControl(protocol.Bitfield{Bits: co.have.Bytes()})
	co.log.Info("peer connected", "peer", id.Short(), "addr", c.RemoteAddr().String(), "outbound", c.Outbound())
}

// preferNew decides between two connections to the same peer. Both ends keep
// the connection initiated by the peer with the lower ID, so they agree.
func (co *Coordinator) preferNew(old, nw *conn.Conn) bool {
	if old.Outbound() == nw.Outbound() {
		return false
	}
	remote := nw.Remote()
	wantOutbound := bytes.Compare(co.self[:], remote[:]) < 0
	return nw.Outbound() == wantOutbound
}

func (co *Coordinator) removePeer(p *peer, err error) {
	if co.peers[p.id] != p {
		return
	}
	delete(co.peers, p.id)
	for i, q := range co.peerList {
		if q == p {
			co.peerList = append(co.peerList[:i], co.peerList[i+1:]...)
			break
		}
	}
	co.connected.Delete(p.id)
	co.peerCount.Store(int32(len(co.peers)))
	p.stopUpload()
	reassigned := len(p.outstanding)
	for _, r := range p.outstanding {
		co.finishRequest(r)
		co.releasePiece(r.piece)
	}
	if p.gotBitfield {
		for i := range co.avail {
			if p.has.Has(i) {
				co.avail[i]--
			}
		}
	}
	attrs := []any{"peer", p.id.Short(), "pieces_from", p.piecesFrom, "requests_reassigned", reassigned}
	if err != nil {
		attrs = append(attrs, "reason", err.Error())
	}
	co.log.Info("peer disconnected", attrs...)
}

func (co *Coordinator) violation(p *peer, what string) {
	co.log.Warn("protocol violation; closing connection", "peer", p.id.Short(), "violation", what)
	p.closing = true
	p.c.Close(protocol.ByeProtocolError)
}

func (co *Coordinator) onMessage(p *peer, m protocol.Message) {
	if _, isBF := m.(protocol.Bitfield); !p.gotBitfield && !isBF {
		co.violation(p, "first message is "+m.Type().String()+", not BITFIELD")
		return
	}
	n := co.m.NumPieces()
	switch m := m.(type) {
	case protocol.Bitfield:
		if p.gotBitfield {
			co.violation(p, "second BITFIELD")
			return
		}
		bf, err := bitfield.FromBytes(n, m.Bits)
		if err != nil {
			co.violation(p, err.Error())
			return
		}
		p.has, p.gotBitfield = bf, true
		for i := range n {
			if bf.Has(i) {
				co.avail[i]++
			}
		}
	case protocol.Have:
		i := int(m.Piece)
		if i >= n {
			co.violation(p, "HAVE for piece out of range")
			return
		}
		if !p.has.Has(i) {
			p.has.Set(i)
			co.avail[i]++
		}
	case protocol.Request:
		co.onRequest(p, m)
	case protocol.Cancel:
		p.up.cancel(m.ReqID)
	case protocol.Piece:
		co.onPiece(p, m)
	case protocol.Reject:
		co.onReject(p, m)
	case protocol.Ping:
		p.c.SendControl(protocol.Pong{Nonce: m.Nonce})
	default:
		// PONG, PEX, CHALLENGE, PROOF and replication messages are handled by
		// later milestones; until then they are accepted and ignored.
		co.log.Debug("ignoring message", "peer", p.id.Short(), "type", m.Type().String())
	}
}

func (co *Coordinator) onRequest(p *peer, m protocol.Request) {
	i := int(m.Piece)
	if i >= co.m.NumPieces() {
		co.violation(p, "REQUEST for piece out of range")
		return
	}
	if !co.have.Has(i) {
		p.c.SendControl(protocol.Reject{ReqID: m.ReqID, Reason: protocol.RejectNotHave})
		return
	}
	if !p.up.enqueue(m) {
		p.c.SendControl(protocol.Reject{ReqID: m.ReqID, Reason: protocol.RejectBusy})
	}
}

func (co *Coordinator) onPiece(p *peer, m protocol.Piece) {
	r, ok := co.reqs[m.ReqID]
	if !ok || r.peer != p || r.piece != int(m.Piece) {
		// Usually a response that crossed our CANCEL or arrived after a timeout.
		p.unexpected++
		return
	}
	co.finishRequest(r)
	now := time.Now()
	if len(m.Data) != co.m.PieceLen(r.piece) {
		p.invalid++
		co.rejected++
		co.log.Warn("piece rejected: wrong length", "peer", p.id.Short(), "piece", r.piece, "len", len(m.Data))
		co.releasePiece(r.piece)
		return
	}
	if s := co.state[r.piece]; s == stVerifying || s == stPersisted {
		p.duplicates++ // another copy already won; drop this one
		return
	}
	job := verify.Job{Piece: r.piece, Data: m.Data, From: p.id, Latency: now.Sub(r.sent)}
	if !co.pool.Submit(job) {
		// Unreachable while the outstanding budget holds; fail safe if it does not.
		co.log.Error("verify queue full; dropping piece", "piece", r.piece)
		co.releasePiece(r.piece)
		return
	}
	co.state[r.piece] = stVerifying
	co.verifying++
	p.piecesFrom++
	p.bytesFrom += int64(len(m.Data))
	co.bytesDown += int64(len(m.Data))
}

func (co *Coordinator) onReject(p *peer, m protocol.Reject) {
	r, ok := co.reqs[m.ReqID]
	if !ok || r.peer != p {
		return
	}
	co.finishRequest(r)
	p.rejects++
	if m.Reason == protocol.RejectNotHave && p.has.Has(r.piece) {
		p.has.Clear(r.piece)
		co.avail[r.piece]--
	}
	co.releasePiece(r.piece)
}

func (co *Coordinator) onVerified(r verify.Result) {
	co.verifying--
	i := r.Piece
	switch {
	case !r.Valid:
		co.rejected++
		if p := co.peers[r.From]; p != nil {
			p.invalid++
		}
		co.log.Warn("piece rejected: hash mismatch", "peer", r.From.Short(), "piece", i)
		co.state[i] = stRequested // back to Missing unless a duplicate request is still in flight
		co.releasePiece(i)
		return
	case r.Err != nil:
		co.log.Error("piece write failed", "piece", i, "err", r.Err)
		co.state[i] = stRequested
		co.releasePiece(i)
		return
	}
	co.state[i] = stPersisted
	co.have.Set(i)
	// Endgame duplicates for this piece are no longer needed.
	for _, rq := range co.reqs {
		if rq.piece == i {
			rq.peer.c.SendControl(protocol.Cancel{ReqID: rq.id})
			co.finishRequest(rq)
		}
	}
	for _, p := range co.peerList {
		if !p.closing {
			p.c.SendControl(protocol.Have{Piece: uint32(i)})
		}
	}
	if co.have.Complete() {
		co.onComplete()
	}
}

func (co *Coordinator) onComplete() {
	co.finished = time.Now()
	if err := co.st.Finalize(); err != nil {
		co.log.Error("finalizing download failed", "err", err)
	}
	co.log.Info("download complete", "path", co.st.Path(), "elapsed", co.finished.Sub(co.started).Round(time.Millisecond),
		"bytes", co.m.Size(), "rejected_pieces", co.rejected)
	co.publishStatus()
	close(co.complete)
}

// finishRequest removes a request from all indexes. It does not change the piece state.
func (co *Coordinator) finishRequest(r *request) {
	delete(co.reqs, r.id)
	delete(r.peer.outstanding, r.id)
	co.inflight[r.piece]--
}

// releasePiece returns a requested piece to Missing once no request for it remains.
func (co *Coordinator) releasePiece(i int) {
	if co.state[i] == stRequested && co.inflight[i] == 0 {
		co.state[i] = stMissing
	}
}

func (co *Coordinator) checkTimeouts(now time.Time) {
	for _, r := range co.reqs {
		if now.Before(r.deadline) {
			continue
		}
		r.peer.timeouts++
		r.peer.c.SendControl(protocol.Cancel{ReqID: r.id})
		co.finishRequest(r)
		co.releasePiece(r.piece)
		co.log.Info("request timed out; piece will be reassigned", "peer", r.peer.id.Short(), "piece", r.piece,
			"after", now.Sub(r.sent).Round(time.Millisecond))
	}
}

// NumPieces, Needed and Availability implement scheduler.View.
func (co *Coordinator) NumPieces() int         { return co.m.NumPieces() }
func (co *Coordinator) Needed(i int) bool      { return co.state[i] == stMissing }
func (co *Coordinator) Availability(i int) int { return co.avail[i] }

func (co *Coordinator) schedule(now time.Time) {
	if co.have.Complete() || len(co.peerList) == 0 || !co.downloading {
		return
	}
	budget := co.cfg.MaxOutstanding - len(co.reqs) - co.verifying
	if budget <= 0 {
		return
	}
	var cands []scheduler.Candidate
	var candPeers []*peer
	co.cfg.Picker.Pick(co, budget, func(piece int) bool {
		cands, candPeers = cands[:0], candPeers[:0]
		for _, p := range co.peerList {
			if p.closing || !p.gotBitfield || !p.has.Has(piece) || len(p.outstanding) >= co.cfg.MaxPerPeer {
				continue
			}
			cands = append(cands, scheduler.Candidate{Peer: p.id, Outstanding: len(p.outstanding)})
			candPeers = append(candPeers, p)
		}
		if len(cands) == 0 {
			return false
		}
		p := candPeers[co.cfg.Selector.Select(piece, cands)]
		co.nextReq++
		id := co.nextReq
		if err := p.c.SendControl(protocol.Request{ReqID: id, Piece: uint32(piece)}); err != nil {
			return false
		}
		r := &request{id: id, piece: piece, peer: p, sent: now, deadline: now.Add(co.cfg.RequestTimeout)}
		co.reqs[id] = r
		p.outstanding[id] = r
		co.inflight[piece]++
		co.state[piece] = stRequested
		return true
	})
}
