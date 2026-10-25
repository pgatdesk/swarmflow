// Package coordinator runs one swarm: it owns all piece and request state in a
// single goroutine, asks the scheduler what to fetch, serves uploads, and hands
// received pieces to the verify pool. Because exactly one goroutine mutates the
// state, duplicate arrivals, cancellations and disconnects can never race.
package coordinator

import (
	"bytes"
	"cmp"
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/conn"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/peerstats"
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
	UploadSlots int
	// UploadQueueCap bounds queued plus in-progress uploads across all remote
	// peers of the swarm; beyond it we REJECT busy (decision 0003). Short queues
	// keep requesters' piece choices fresh instead of committing them seconds
	// before they can be served.
	UploadQueueCap int
	// BusyBackoff is how long we avoid a peer after it rejects us as busy
	// (randomised ±25% to avoid synchronised retries).
	BusyBackoff time.Duration
	// RequestTimeout is the deadline for requests to a peer without latency
	// history. Once a peer has history its deadline is
	// clamp(TimeoutFactor × p95 latency, RequestTimeoutMin, RequestTimeoutMax).
	RequestTimeout    time.Duration
	RequestTimeoutMin time.Duration
	RequestTimeoutMax time.Duration
	TimeoutFactor     float64
	PingInterval      time.Duration
	VerifyWorkers     int
	MaxPeers          int
	Picker            scheduler.PiecePicker
	Selector          scheduler.PeerSelector
	Endgame           scheduler.EndgamePolicy
	Stats             peerstats.Config
	// Journal, if set, durably records progress (see Journal).
	Journal Journal
}

// Journal durably records download progress. PiecePersisted is called from a
// verify worker after the piece's data has been written and flushed; the piece
// only counts as persisted if it returns nil (otherwise it is fetched again).
// Completed is called after the file is finalized.
type Journal interface {
	PiecePersisted(piece int) error
	Completed() error
}

func DefaultConfig() Config {
	return Config{
		MaxOutstanding:    32,
		MaxPerPeer:        4,
		UploadSlots:       4,
		UploadQueueCap:    16,
		BusyBackoff:       250 * time.Millisecond,
		RequestTimeout:    10 * time.Second,
		RequestTimeoutMin: 2 * time.Second,
		RequestTimeoutMax: 30 * time.Second,
		TimeoutFactor:     4,
		PingInterval:      5 * time.Second,
		VerifyWorkers:     2,
		MaxPeers:          50,
		Picker:            scheduler.NewRarestFirst(1),
		Selector:          scheduler.NewAdaptivePeer(1, scheduler.DefaultExplore),
		Endgame:           scheduler.DefaultEndgame(),
		Stats:             peerstats.DefaultConfig(),
	}
}

const (
	tickInterval   = 100 * time.Millisecond
	statusInterval = 250 * time.Millisecond
	// Stats of disconnected peers are kept so a peer that reconnects keeps its
	// reputation; they are pruned after statsRetention without contact.
	statsRetention = 30 * time.Minute
	maxStats       = 1024
)

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
	// expected is the request's expected completion time when it was sent.
	expected time.Duration
	endgame  bool
}

type peer struct {
	id          protocol.PeerID
	c           *conn.Conn
	stats       *peerstats.Stats
	has         bitfield.Bitfield
	gotBitfield bool
	closing     bool
	outstanding map[uint32]*request
	up          *uploader
	stopUpload  context.CancelFunc

	pingNonce          uint64
	pingSent, lastPing time.Time
	busyUntil          time.Time

	piecesFrom, duplicates, unexpected int
	bytesFrom                          int64
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

	connected    sync.Map // protocol.PeerID → netip.AddrPort; read by the node for dialing decisions
	peerCount    atomic.Int32
	bytesUp      atomic.Int64
	uploadQueued atomic.Int32
	status       atomic.Pointer[Status]

	// Everything below is owned by the Run goroutine.
	have      bitfield.Bitfield
	state     []pieceState
	inflight  []int
	avail     []int
	peers     map[protocol.PeerID]*peer
	peerList  []*peer // connection order, for deterministic candidate ordering
	stats     map[protocol.PeerID]*peerstats.Stats
	banned    map[protocol.PeerID]time.Time
	reqs      map[uint32]*request
	nextReq   uint32
	verifying int
	// downloading is false for read-only stores (seed mode serves a fixed,
	// possibly partial, file and never fetches).
	downloading bool
	inEndgame   bool
	started     time.Time
	finished    time.Time
	lastStatus  time.Time
	counters    Counters
}

// New creates a coordinator for manifest m backed by st. have is the set of
// pieces already verified on disk.
func New(m *manifest.Manifest, st *store.Store, have bitfield.Bitfield, self protocol.PeerID, cfg Config, log *slog.Logger) *Coordinator {
	n := m.NumPieces()
	co := &Coordinator{
		cfg: cfg, m: m, st: st, self: self,
		log:         log.With("swarm", m.ID().Short()),
		addCh:       make(chan *conn.Conn),
		inbound:     make(chan conn.Inbound, 256),
		done:        make(chan struct{}),
		complete:    make(chan struct{}),
		have:        have,
		state:       make([]pieceState, n),
		inflight:    make([]int, n),
		avail:       make([]int, n),
		peers:       make(map[protocol.PeerID]*peer),
		stats:       make(map[protocol.PeerID]*peerstats.Stats),
		banned:      make(map[protocol.PeerID]time.Time),
		reqs:        make(map[uint32]*request),
		downloading: st.Writable(),
	}
	for i := range n {
		if have.Has(i) {
			co.state[i] = stPersisted
		}
	}
	commit := func(i int, data []byte) error {
		if err := st.WritePiece(i, data); err != nil { // flushed before returning
			return err
		}
		if cfg.Journal != nil {
			return cfg.Journal.PiecePersisted(i)
		}
		return nil
	}
	co.pool = verify.NewPool(m, commit, cfg.VerifyWorkers, cfg.MaxOutstanding)
	co.publishStatus(time.Now())
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
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			for _, p := range co.peerList {
				p.c.Close(protocol.ByeShutdown)
			}
			co.publishStatus(time.Now())
			return nil
		case c := <-co.addCh:
			co.onConn(ctx, c)
		case in := <-co.inbound:
			p := co.peers[in.From.Remote()]
			if p == nil || p.c != in.From {
				break // a connection we already dropped (duplicate, overload, ban)
			}
			if in.Closed {
				co.removePeer(p, in.Err)
			} else if !p.closing {
				co.onMessage(p, in.Msg, time.Now())
			}
		case r := <-co.pool.Results():
			co.onVerified(r, time.Now())
		case now := <-tick.C:
			co.checkTimeouts(now)
			co.pingPeers(now)
			if now.Sub(co.lastStatus) >= statusInterval {
				co.pruneStats(now)
				co.publishStatus(now)
			}
		}
		co.schedule(time.Now())
	}
}

func (co *Coordinator) statsFor(id protocol.PeerID) *peerstats.Stats {
	s := co.stats[id]
	if s == nil {
		s = peerstats.New(&co.cfg.Stats)
		co.stats[id] = s
	}
	return s
}

func (co *Coordinator) pruneStats(now time.Time) {
	if len(co.stats) <= len(co.peers) {
		return
	}
	for id, s := range co.stats {
		if _, live := co.peers[id]; !live && (now.Sub(s.LastSeen()) > statsRetention || len(co.stats) > maxStats) {
			delete(co.stats, id)
		}
	}
}

func (co *Coordinator) onConn(ctx context.Context, c *conn.Conn) {
	reject := func(reason protocol.ByeReason) {
		c.Start(ctx, co.inbound) // so BYE is delivered; the close notice is then ignored
		c.Close(reason)
	}
	id := c.Remote()
	if _, isBanned := co.banned[id]; isBanned {
		reject(protocol.ByeBanned)
		return
	}
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
		id: id, c: c, stats: co.statsFor(id), has: bitfield.New(co.m.NumPieces()),
		outstanding: make(map[uint32]*request),
		stopUpload:  stop,
	}
	p.up = newUploader(c, co.st, co.cfg.UploadSlots, &co.bytesUp, &co.uploadQueued, co.log)
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
	co.counters.Reassigned += reassigned
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

func (co *Coordinator) closePeer(p *peer, reason protocol.ByeReason) {
	p.closing = true
	p.c.Close(reason)
}

func (co *Coordinator) violation(p *peer, what string) {
	co.log.Warn("protocol violation; closing connection", "peer", p.id.Short(), "violation", what)
	co.closePeer(p, protocol.ByeProtocolError)
}

func (co *Coordinator) onMessage(p *peer, m protocol.Message, now time.Time) {
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
		co.onPiece(p, m, now)
	case protocol.Reject:
		co.onReject(p, m, now)
	case protocol.Ping:
		p.c.SendControl(protocol.Pong{Nonce: m.Nonce})
	case protocol.Pong:
		if p.pingNonce != 0 && m.Nonce == p.pingNonce {
			p.stats.OnRTT(now, now.Sub(p.pingSent))
			p.pingNonce = 0
		}
	default:
		// PEX, CHALLENGE, PROOF and replication messages are handled by later
		// milestones; until then they are accepted and ignored.
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
	if (co.cfg.UploadQueueCap > 0 && int(co.uploadQueued.Load()) >= co.cfg.UploadQueueCap) || !p.up.enqueue(m) {
		co.counters.BusySent++
		p.c.SendControl(protocol.Reject{ReqID: m.ReqID, Reason: protocol.RejectBusy})
	}
}

func (co *Coordinator) onPiece(p *peer, m protocol.Piece, now time.Time) {
	r, ok := co.reqs[m.ReqID]
	if !ok || r.peer != p || r.piece != int(m.Piece) {
		// Usually a response that crossed our CANCEL or arrived after a timeout.
		p.unexpected++
		return
	}
	co.finishRequest(r)
	if len(m.Data) != co.m.PieceLen(r.piece) {
		co.log.Warn("piece rejected: wrong length", "peer", p.id.Short(), "piece", r.piece, "len", len(m.Data))
		co.counters.Rejected++
		co.onInvalid(p, now)
		co.releasePiece(r.piece)
		return
	}
	p.stats.OnSuccess(now, r.sent, len(m.Data))
	if s := co.state[r.piece]; s == stVerifying || s == stPersisted {
		p.duplicates++ // another copy already won; drop this one
		co.counters.DuplicatePieces++
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
	co.counters.BytesDown += int64(len(m.Data))
}

func (co *Coordinator) onReject(p *peer, m protocol.Reject, now time.Time) {
	r, ok := co.reqs[m.ReqID]
	if !ok || r.peer != p {
		return
	}
	co.finishRequest(r)
	switch m.Reason {
	case protocol.RejectBusy:
		// Backpressure, not failure: try others now and this peer again shortly.
		p.stats.OnBusy(now)
		co.counters.BusyReceived++
		jitter := time.Duration(rand.Int64N(int64(co.cfg.BusyBackoff)/2+1)) - co.cfg.BusyBackoff/4
		p.busyUntil = now.Add(co.cfg.BusyBackoff + jitter)
	default:
		p.stats.OnReject(now)
		if m.Reason == protocol.RejectNotHave && p.has.Has(r.piece) {
			p.has.Clear(r.piece)
			co.avail[r.piece]--
		}
	}
	co.releasePiece(r.piece)
}

// onInvalid charges a peer for bad data and bans it past the threshold.
func (co *Coordinator) onInvalid(p *peer, now time.Time) {
	if p.stats.OnInvalid(now) && co.ban(p.id, now) && !p.closing {
		co.closePeer(p, protocol.ByeBanned)
	}
}

// ban records id as banned and reports whether it was newly banned.
func (co *Coordinator) ban(id protocol.PeerID, now time.Time) bool {
	if _, already := co.banned[id]; already {
		return false
	}
	co.banned[id] = now
	co.counters.Banned++
	co.log.Warn("peer banned for repeated invalid pieces", "peer", id.Short())
	return true
}

func (co *Coordinator) onVerified(r verify.Result, now time.Time) {
	co.verifying--
	i := r.Piece
	switch {
	case !r.Valid:
		co.counters.Rejected++
		co.log.Warn("piece rejected: hash mismatch", "peer", r.From.Short(), "piece", i)
		if p := co.peers[r.From]; p != nil {
			co.onInvalid(p, now)
		} else if s := co.stats[r.From]; s != nil && s.OnInvalid(now) {
			co.ban(r.From, now)
		}
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
			co.counters.Cancelled++
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
	} else if co.cfg.Journal != nil {
		if err := co.cfg.Journal.Completed(); err != nil {
			co.log.Error("recording completion failed", "err", err)
		}
	}
	co.log.Info("download complete", "path", co.st.Path(), "elapsed", co.finished.Sub(co.started).Round(time.Millisecond),
		"bytes", co.m.Size(), "rejected_pieces", co.counters.Rejected, "endgame_requests", co.counters.EndgameRequests)
	co.publishStatus(co.finished)
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
		r.peer.stats.OnTimeout(now)
		r.peer.c.SendControl(protocol.Cancel{ReqID: r.id})
		co.finishRequest(r)
		co.releasePiece(r.piece)
		co.counters.Timeouts++
		attrs := []any{"peer", r.peer.id.Short(), "piece", r.piece, "after", now.Sub(r.sent).Round(time.Millisecond)}
		if r.peer.stats.InCooldown(now) {
			attrs = append(attrs, "cooldown_until", r.peer.stats.CooldownUntil().Format(time.TimeOnly))
		}
		co.log.Info("request timed out; piece will be reassigned", attrs...)
	}
}

func (co *Coordinator) pingPeers(now time.Time) {
	for _, p := range co.peerList {
		if p.closing || !p.gotBitfield {
			continue
		}
		if p.pingNonce != 0 && now.Sub(p.pingSent) > 2*co.cfg.PingInterval {
			p.pingNonce = 0 // lost or very late; no RTT sample
		}
		if p.pingNonce == 0 && now.Sub(p.lastPing) >= co.cfg.PingInterval {
			p.pingNonce = rand.Uint64() | 1
			p.pingSent, p.lastPing = now, now
			p.c.SendControl(protocol.Ping{Nonce: p.pingNonce})
		}
	}
}

// requestTimeout returns the deadline for a new request to p.
func (co *Coordinator) requestTimeout(p *peer) time.Duration {
	_, p95, ok := p.stats.Latency()
	if !ok {
		return co.cfg.RequestTimeout
	}
	d := time.Duration(co.cfg.TimeoutFactor * float64(p95))
	return min(max(d, co.cfg.RequestTimeoutMin), co.cfg.RequestTimeoutMax)
}

func (co *Coordinator) candidate(p *peer) scheduler.Candidate {
	c := scheduler.Candidate{Peer: p.id, Outstanding: len(p.outstanding), Reliability: p.stats.Reliability(),
		Samples: p.stats.Successes}
	c.Throughput, c.HasThroughput = p.stats.Throughput()
	c.RTT, c.HasRTT = p.stats.RTT()
	return c
}

// NumPieces, Needed and Availability implement scheduler.View.
func (co *Coordinator) NumPieces() int         { return co.m.NumPieces() }
func (co *Coordinator) Needed(i int) bool      { return co.state[i] == stMissing }
func (co *Coordinator) Availability(i int) int { return co.avail[i] }

// usable reports whether p may receive a new request for piece.
func (co *Coordinator) usable(p *peer, piece int, now time.Time) bool {
	return !p.closing && p.gotBitfield && p.has.Has(piece) && len(p.outstanding) < co.cfg.MaxPerPeer &&
		!now.Before(p.busyUntil)
}

// sources returns the candidates for piece, excluding peers in skip. Peers in
// cooldown are only offered when no other source exists, and then only when
// idle, so a slow sole source is probed rather than abandoned.
func (co *Coordinator) sources(piece int, now time.Time, skip func(*peer) bool) ([]scheduler.Candidate, []*peer) {
	var cands []scheduler.Candidate
	var ps []*peer
	var cooling []*peer
	for _, p := range co.peerList {
		if !co.usable(p, piece, now) || (skip != nil && skip(p)) {
			continue
		}
		if p.stats.InCooldown(now) {
			cooling = append(cooling, p)
			continue
		}
		cands, ps = append(cands, co.candidate(p)), append(ps, p)
	}
	if len(ps) == 0 {
		for _, p := range cooling {
			if len(p.outstanding) == 0 {
				cands, ps = append(cands, co.candidate(p)), append(ps, p)
			}
		}
	}
	return cands, ps
}

func (co *Coordinator) send(p *peer, piece int, now time.Time, cands []scheduler.Candidate, endgame bool) bool {
	co.nextReq++
	id := co.nextReq
	if err := p.c.SendControl(protocol.Request{ReqID: id, Piece: uint32(piece)}); err != nil {
		return false
	}
	tp, rtt := scheduler.Priors(cands)
	c := co.candidate(p)
	if c.HasThroughput && c.Throughput > 0 {
		tp = c.Throughput
	}
	if c.HasRTT {
		rtt = c.RTT
	}
	expected := rtt + time.Duration(float64(len(p.outstanding)+1)*float64(co.m.PieceLen(piece))/tp*float64(time.Second))
	r := &request{id: id, piece: piece, peer: p, sent: now, deadline: now.Add(co.requestTimeout(p)),
		expected: expected, endgame: endgame}
	co.reqs[id] = r
	p.outstanding[id] = r
	co.inflight[piece]++
	co.state[piece] = stRequested
	co.counters.Requests++
	return true
}

func (co *Coordinator) schedule(now time.Time) {
	if co.have.Complete() || len(co.peerList) == 0 || !co.downloading {
		return
	}
	budget := co.cfg.MaxOutstanding - len(co.reqs) - co.verifying
	if budget <= 0 {
		return
	}
	budget -= co.cfg.Picker.Pick(co, budget, func(piece int) bool {
		cands, ps := co.sources(piece, now, nil)
		if len(cands) == 0 {
			return false
		}
		return co.send(ps[co.cfg.Selector.Select(piece, co.m.PieceLen(piece), cands)], piece, now, cands, false)
	})
	if budget > 0 && len(co.reqs) > 0 && co.allMissingInFlight() {
		co.endgame(now, budget)
	}
}

// allMissingInFlight reports whether no fetchable piece is waiting for a
// first request: the condition for entering endgame.
func (co *Coordinator) allMissingInFlight() bool {
	for i, s := range co.state {
		if s == stMissing && co.avail[i] > 0 {
			return false
		}
	}
	return true
}

func (co *Coordinator) endgame(now time.Time, budget int) {
	if !co.cfg.Endgame.Enabled {
		return
	}
	if !co.inEndgame {
		co.inEndgame = true
		co.log.Info("entering endgame", "remaining", co.m.NumPieces()-co.have.Count(), "in_flight", len(co.reqs))
	}
	oldest := make(map[int]*request)
	for _, r := range co.reqs {
		if o := oldest[r.piece]; o == nil || r.sent.Before(o.sent) || (r.sent.Equal(o.sent) && r.id < o.id) {
			oldest[r.piece] = r
		}
	}
	inflight := make([]scheduler.InFlight, 0, len(oldest))
	for piece, r := range oldest {
		inflight = append(inflight, scheduler.InFlight{Piece: piece, Requests: co.inflight[piece],
			Age: now.Sub(r.sent), Expected: r.expected})
	}
	slices.SortFunc(inflight, func(a, b scheduler.InFlight) int { return cmp.Compare(a.Piece, b.Piece) })
	for _, piece := range co.cfg.Endgame.Overdue(inflight) {
		if budget == 0 {
			return
		}
		cands, ps := co.sources(piece, now, func(p *peer) bool {
			for _, r := range p.outstanding {
				if r.piece == piece {
					return true
				}
			}
			return false
		})
		if len(cands) == 0 {
			continue
		}
		p := ps[co.cfg.Selector.Select(piece, co.m.PieceLen(piece), cands)]
		if co.send(p, piece, now, cands, true) {
			budget--
			co.counters.EndgameRequests++
			o := oldest[piece]
			co.log.Debug("endgame duplicate request", "piece", piece, "peer", p.id.Short(),
				"waiting_on", o.peer.id.Short(), "age", now.Sub(o.sent).Round(time.Millisecond),
				"expected", o.expected.Round(time.Millisecond))
		}
	}
}
