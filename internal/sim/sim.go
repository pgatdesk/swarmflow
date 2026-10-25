// Package sim is a deterministic discrete-event simulator of a swarm, used to
// compare scheduling policies on identical synthetic workloads. It runs the
// real scheduler (pickers, selectors, endgame) and peerstats code; only the
// network and the coordinator's bookkeeping are modelled.
//
// Model (and its simplifications):
//   - Full mesh: every online peer is connected to every other.
//   - Each peer's upload capacity is shared equally among its active transfers
//     (processor sharing), with at most UploadSlots active and the rest queued
//     FIFO. Download capacity is unlimited.
//   - Latency between two peers is the sum of their one-way access latencies;
//     REQUEST, CANCEL and piece delivery each take one such latency.
//   - HAVE propagation is instantaneous; there is no packet loss and no TCP
//     dynamics. A peer leaving is detected immediately by the others (as a
//     closed connection would be), and their requests to it are reassigned.
//   - RTT samples equal twice the path latency (no queueing).
package sim

import (
	"cmp"
	"container/heap"
	"math"
	"slices"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/peerstats"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
)

type PeerSpec struct {
	Name string
	// Seed peers start with the whole file and never download.
	Seed    bool
	Up      float64       // upload capacity, bytes/s
	Latency time.Duration // one-way access latency
	JoinAt  time.Duration
	LeaveAt time.Duration // 0 = never
	// LeaveOnComplete makes a downloader leave as soon as it finishes.
	LeaveOnComplete bool
}

type Config struct {
	FileSize       int64
	PieceSize      int
	Peers          []PeerSpec
	NewPicker      func(seed uint64) scheduler.PiecePicker
	NewSelector    func(seed uint64) scheduler.PeerSelector
	MaxOutstanding int
	MaxPerPeer     int
	UploadSlots    int
	RequestTimeout time.Duration
	Endgame        scheduler.EndgamePolicy
	Stats          peerstats.Config
	// Seed varies policy randomness between trials; peer i uses Seed*1000+i.
	Seed    uint64
	Horizon time.Duration
	// ServerQueueCap, if > 0, bounds each peer's total queued plus active
	// uploads; requests beyond it are rejected as busy, and the requester
	// avoids that peer for BusyBackoff before trying it again.
	ServerQueueCap int
	BusyBackoff    time.Duration
}

type Result struct {
	// Completion holds each downloader's completion time (time since it
	// joined), in spec order; -1 if it did not finish.
	Completion      []time.Duration
	Downloaders     int
	Completed       int
	Uploaded        []int64 // per peer, in spec order
	SeedUploaded    int64
	DeliveredBytes  int64
	WastedBytes     int64 // deliveries that were duplicates, cancelled or timed out
	Requests        int
	Timeouts        int
	Busy            int
	EndgameRequests int
	End             time.Duration
	// Coverage is the number of distinct pieces held by online peers at the end;
	// below the piece count, the file can no longer be completed by anyone.
	Coverage int
	Pieces   int
	// MaxQueue is the deepest upload queue any peer had (queued + active).
	MaxQueue int
}

type evKind uint8

const (
	evJoin evKind = iota
	evLeave
	evReqArrive
	evServerDone
	evDeliver
	evTimeout
	evCancelArrive
	evBusy
)

type event struct {
	at      time.Duration
	seq     uint64
	kind    evKind
	peer    *speer
	req     *sreq
	version int
}

type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQueue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQueue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}

type pstate uint8

const (
	psMissing pstate = iota
	psRequested
	psDone
)

type sreq struct {
	id       int
	from, to *speer
	piece    int
	sent     time.Duration
	expected time.Duration
	active   bool // still wanted by the requester
}

type xfer struct {
	req       *sreq
	remaining float64
}

type speer struct {
	idx    int
	spec   PeerSpec
	id     protocol.PeerID
	online bool
	done   bool
	joined time.Duration

	have        bitfield.Bitfield
	state       []pstate
	inflight    []int
	avail       []int
	active      map[int]*sreq // by request id
	perPeer     []int         // outstanding requests to each peer index
	busyUntil   []time.Duration
	stats       []*peerstats.Stats
	picker      scheduler.PiecePicker
	selector    scheduler.PeerSelector
	pollPending bool

	// upload side
	queue      []*sreq
	xfers      []*xfer
	lastUpdate time.Duration
	version    int
	uploaded   int64
}

type sim struct {
	cfg    Config
	n      int // pieces
	peers  []*speer
	q      eventQueue
	seq    uint64
	now    time.Duration
	nextID int
	res    Result
}

var epoch = time.Unix(0, 0)

func at(d time.Duration) time.Time { return epoch.Add(d) }

// Run simulates cfg to completion (or the horizon) and returns the result.
func Run(cfg Config) Result {
	s := &sim{cfg: cfg, n: int((cfg.FileSize + int64(cfg.PieceSize) - 1) / int64(cfg.PieceSize))}
	for i, spec := range cfg.Peers {
		p := &speer{idx: i, spec: spec, have: bitfield.New(s.n), state: make([]pstate, s.n),
			inflight: make([]int, s.n), avail: make([]int, s.n), active: map[int]*sreq{},
			perPeer: make([]int, len(cfg.Peers)), busyUntil: make([]time.Duration, len(cfg.Peers))}
		p.id[0], p.id[1] = byte(i>>8), byte(i)
		if spec.Seed {
			for k := range s.n {
				p.have.Set(k)
				p.state[k] = psDone
			}
			p.done = true
		} else {
			s.res.Downloaders++
			p.picker = cfg.NewPicker(cfg.Seed*1000 + uint64(i))
			p.selector = cfg.NewSelector(cfg.Seed*1000 + uint64(i))
		}
		p.stats = make([]*peerstats.Stats, len(cfg.Peers))
		for j := range p.stats {
			p.stats[j] = peerstats.New(&s.cfg.Stats)
		}
		s.peers = append(s.peers, p)
		s.push(&event{at: spec.JoinAt, kind: evJoin, peer: p})
		if spec.LeaveAt > 0 {
			s.push(&event{at: spec.LeaveAt, kind: evLeave, peer: p})
		}
	}
	s.res.Completion = make([]time.Duration, 0, s.res.Downloaders)
	completion := make([]time.Duration, len(cfg.Peers))
	for i := range completion {
		completion[i] = -1
	}

	for s.q.Len() > 0 {
		e := heap.Pop(&s.q).(*event)
		if e.at > cfg.Horizon {
			break
		}
		s.now = e.at
		switch e.kind {
		case evJoin:
			s.join(e.peer)
		case evLeave:
			s.leave(e.peer)
		case evReqArrive:
			s.reqArrive(e.req)
		case evServerDone:
			if e.version == e.peer.version {
				s.serverDone(e.peer)
			}
		case evDeliver:
			if s.deliver(e.req) {
				completion[e.req.from.idx] = s.now - e.req.from.joined
			}
		case evTimeout:
			s.timeout(e.req)
		case evCancelArrive:
			s.cancelAtServer(e.req)
		case evBusy:
			s.busy(e.req)
		}
		if s.res.Completed == s.res.Downloaders {
			break
		}
	}
	s.res.End = s.now
	s.res.Pieces = s.n
	for k := range s.n {
		for _, p := range s.online() {
			if p.have.Has(k) {
				s.res.Coverage++
				break
			}
		}
	}
	for i, p := range s.peers {
		s.res.Uploaded = append(s.res.Uploaded, p.uploaded)
		if p.spec.Seed {
			s.res.SeedUploaded += p.uploaded
		} else {
			s.res.Completion = append(s.res.Completion, completion[i])
		}
	}
	return s.res
}

func (s *sim) push(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.q, e)
}

func (s *sim) lat(a, b *speer) time.Duration { return a.spec.Latency + b.spec.Latency }

func (s *sim) pieceLen(i int) int {
	if i == s.n-1 {
		return int(s.cfg.FileSize - int64(i)*int64(s.cfg.PieceSize))
	}
	return s.cfg.PieceSize
}

func (s *sim) online() []*speer {
	var out []*speer
	for _, p := range s.peers {
		if p.online {
			out = append(out, p)
		}
	}
	return out
}

func (s *sim) join(p *speer) {
	p.online, p.joined = true, s.now
	for _, o := range s.online() {
		if o == p {
			continue
		}
		for k := range s.n {
			if o.have.Has(k) {
				p.avail[k]++
			}
			if p.have.Has(k) {
				o.avail[k]++
			}
		}
		rtt := 2 * s.lat(p, o)
		p.stats[o.idx].OnRTT(at(s.now), rtt)
		o.stats[p.idx].OnRTT(at(s.now), rtt)
	}
	s.scheduleAll()
}

func (s *sim) leave(p *speer) {
	if !p.online {
		return
	}
	p.online = false
	for _, o := range s.online() {
		for k := range s.n {
			if p.have.Has(k) {
				o.avail[k]--
			}
		}
		for _, r := range sortedReqs(o.active) {
			if r.to == p {
				o.finish(r)
				o.release(r.piece)
			}
		}
	}
	for _, r := range sortedReqs(p.active) {
		p.finish(r)
	}
	p.queue, p.xfers = nil, nil
	p.version++
	s.scheduleAll()
}

func sortedReqs(m map[int]*sreq) []*sreq {
	out := make([]*sreq, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b *sreq) int { return cmp.Compare(a.id, b.id) })
	return out
}

// --- upload side (processor sharing) ---

func (s *sim) advance(p *speer) {
	if len(p.xfers) > 0 {
		rate := p.spec.Up / float64(len(p.xfers))
		el := (s.now - p.lastUpdate).Seconds()
		for _, x := range p.xfers {
			x.remaining -= rate * el
		}
	}
	p.lastUpdate = s.now
}

func (s *sim) reschedule(p *speer) {
	for len(p.xfers) < s.cfg.UploadSlots && len(p.queue) > 0 {
		r := p.queue[0]
		p.queue = p.queue[1:]
		p.xfers = append(p.xfers, &xfer{req: r, remaining: float64(s.pieceLen(r.piece))})
	}
	p.version++
	if len(p.xfers) == 0 || p.spec.Up <= 0 {
		return
	}
	minRem := p.xfers[0].remaining
	for _, x := range p.xfers[1:] {
		minRem = min(minRem, x.remaining)
	}
	// Round up so the event always lands after the transfer is complete; a
	// zero delay would re-fire at the same instant without progress.
	dt := time.Duration(math.Ceil(max(minRem, 0) / (p.spec.Up / float64(len(p.xfers))) * 1e9))
	s.push(&event{at: s.now + max(dt, 1), kind: evServerDone, peer: p, version: p.version})
}

func (s *sim) reqArrive(r *sreq) {
	srv := r.to
	if !srv.online || !r.active {
		return
	}
	if c := s.cfg.ServerQueueCap; c > 0 && len(srv.queue)+len(srv.xfers) >= c {
		s.push(&event{at: s.now + s.lat(srv, r.from), kind: evBusy, req: r})
		return
	}
	s.advance(srv)
	srv.queue = append(srv.queue, r)
	s.res.MaxQueue = max(s.res.MaxQueue, len(srv.queue)+len(srv.xfers))
	s.reschedule(srv)
}

func (s *sim) serverDone(p *speer) {
	s.advance(p)
	// Anything within 2 ns of transfer of finishing is finished (float rounding).
	eps := 2e-9 * p.spec.Up / float64(max(len(p.xfers), 1))
	kept := p.xfers[:0]
	for _, x := range p.xfers {
		if x.remaining <= eps+1e-6 {
			p.uploaded += int64(s.pieceLen(x.req.piece))
			s.push(&event{at: s.now + s.lat(p, x.req.from), kind: evDeliver, req: x.req})
		} else {
			kept = append(kept, x)
		}
	}
	p.xfers = kept
	s.reschedule(p)
}

func (s *sim) cancelAtServer(r *sreq) {
	srv := r.to
	if !srv.online {
		return
	}
	if i := slices.Index(srv.queue, r); i >= 0 {
		srv.queue = slices.Delete(srv.queue, i, i+1)
		return
	}
	for i, x := range srv.xfers {
		if x.req == r {
			s.advance(srv)
			srv.xfers = slices.Delete(srv.xfers, i, i+1)
			s.reschedule(srv)
			return
		}
	}
}

// --- download side (mirrors the coordinator) ---

func (p *speer) NumPieces() int         { return len(p.state) }
func (p *speer) Needed(i int) bool      { return p.state[i] == psMissing }
func (p *speer) Availability(i int) int { return p.avail[i] }

func (p *speer) finish(r *sreq) {
	r.active = false
	delete(p.active, r.id)
	p.perPeer[r.to.idx]--
	p.inflight[r.piece]--
}

func (p *speer) release(i int) {
	if p.state[i] == psRequested && p.inflight[i] == 0 {
		p.state[i] = psMissing
	}
}

func (s *sim) candidate(p, o *speer) scheduler.Candidate {
	st := p.stats[o.idx]
	c := scheduler.Candidate{Peer: o.id, Outstanding: p.perPeer[o.idx], Reliability: st.Reliability(), Samples: st.Successes}
	c.Throughput, c.HasThroughput = st.Throughput()
	c.RTT, c.HasRTT = st.RTT()
	return c
}

func (s *sim) sources(p *speer, piece int, skip func(*speer) bool) ([]scheduler.Candidate, []*speer) {
	var cands []scheduler.Candidate
	var ps, cooling []*speer
	for _, o := range s.peers {
		if o == p || !o.online || !o.have.Has(piece) || p.perPeer[o.idx] >= s.cfg.MaxPerPeer ||
			s.now < p.busyUntil[o.idx] || (skip != nil && skip(o)) {
			continue
		}
		if p.stats[o.idx].InCooldown(at(s.now)) {
			cooling = append(cooling, o)
			continue
		}
		cands, ps = append(cands, s.candidate(p, o)), append(ps, o)
	}
	if len(ps) == 0 {
		for _, o := range cooling {
			if p.perPeer[o.idx] == 0 {
				cands, ps = append(cands, s.candidate(p, o)), append(ps, o)
			}
		}
	}
	return cands, ps
}

func (s *sim) timeoutFor(p, o *speer) time.Duration {
	_, p95, ok := p.stats[o.idx].Latency()
	if !ok {
		return s.cfg.RequestTimeout
	}
	return min(max(4*p95, 2*time.Second), 30*time.Second)
}

func (s *sim) send(p, o *speer, piece int, cands []scheduler.Candidate) {
	tp, rtt := scheduler.Priors(cands)
	if v, ok := p.stats[o.idx].Throughput(); ok && v > 0 {
		tp = v
	}
	if v, ok := p.stats[o.idx].RTT(); ok {
		rtt = v
	}
	s.nextID++
	r := &sreq{id: s.nextID, from: p, to: o, piece: piece, sent: s.now, active: true,
		expected: rtt + time.Duration(float64(p.perPeer[o.idx]+1)*float64(s.pieceLen(piece))/tp*float64(time.Second))}
	p.active[r.id] = r
	p.perPeer[o.idx]++
	p.inflight[piece]++
	p.state[piece] = psRequested
	s.res.Requests++
	s.push(&event{at: s.now + s.lat(p, o), kind: evReqArrive, req: r})
	s.push(&event{at: s.now + s.timeoutFor(p, o), kind: evTimeout, req: r})
}

func (s *sim) scheduleAll() {
	for _, p := range s.peers {
		s.schedule(p)
	}
}

func (s *sim) schedule(p *speer) {
	if !p.online || p.done {
		return
	}
	budget := s.cfg.MaxOutstanding - len(p.active)
	if budget <= 0 {
		return
	}
	budget -= p.picker.Pick(p, budget, func(piece int) bool {
		cands, ps := s.sources(p, piece, nil)
		if len(cands) == 0 {
			return false
		}
		s.send(p, ps[p.selector.Select(piece, s.pieceLen(piece), cands)], piece, cands)
		return true
	})
	if budget <= 0 || len(p.active) == 0 || !s.cfg.Endgame.Enabled {
		return
	}
	for k, st := range p.state {
		if st == psMissing && p.avail[k] > 0 {
			return // not every fetchable piece is in flight yet
		}
	}
	oldest := map[int]*sreq{}
	for _, r := range sortedReqs(p.active) {
		if o := oldest[r.piece]; o == nil || r.sent < o.sent {
			oldest[r.piece] = r
		}
	}
	var inflight []scheduler.InFlight
	for piece, r := range oldest {
		inflight = append(inflight, scheduler.InFlight{Piece: piece, Requests: p.inflight[piece], Age: s.now - r.sent, Expected: r.expected})
	}
	slices.SortFunc(inflight, func(a, b scheduler.InFlight) int { return cmp.Compare(a.Piece, b.Piece) })
	for _, piece := range s.cfg.Endgame.Overdue(inflight) {
		if budget == 0 {
			break
		}
		cands, ps := s.sources(p, piece, func(o *speer) bool {
			for _, r := range p.active {
				if r.piece == piece && r.to == o {
					return true
				}
			}
			return false
		})
		if len(cands) == 0 {
			continue
		}
		s.send(p, ps[p.selector.Select(piece, s.pieceLen(piece), cands)], piece, cands)
		s.res.EndgameRequests++
		budget--
	}
	// Overdue requests become eligible as time passes without any event at
	// this peer, so poll while in endgame (like the coordinator's tick).
	if !p.pollPending {
		p.pollPending = true
		s.push(&event{at: s.now + 100*time.Millisecond, kind: evTimeout, req: &sreq{from: p}})
	}
}

// deliver handles a piece arriving at its requester and reports whether that
// completed the requester's download.
func (s *sim) deliver(r *sreq) bool {
	p := r.from
	size := s.pieceLen(r.piece)
	s.res.DeliveredBytes += int64(size)
	if !p.online || !r.active {
		s.res.WastedBytes += int64(size)
		return false
	}
	p.finish(r)
	p.stats[r.to.idx].OnSuccess(at(s.now), at(r.sent), size)
	if p.state[r.piece] == psDone {
		s.res.WastedBytes += int64(size)
		return false
	}
	p.state[r.piece] = psDone
	p.have.Set(r.piece)
	for _, o := range s.online() {
		if o != p {
			o.avail[r.piece]++
		}
	}
	for _, d := range sortedReqs(p.active) {
		if d.piece == r.piece {
			p.finish(d)
			s.push(&event{at: s.now + s.lat(p, d.to), kind: evCancelArrive, req: d})
		}
	}
	completed := false
	if p.have.Complete() {
		p.done, completed = true, true
		s.res.Completed++
		if p.spec.LeaveOnComplete {
			s.leave(p)
		}
	}
	s.scheduleAll()
	return completed
}

// busy handles a REJECT(busy): the request is dropped without a reliability
// penalty and the peer is skipped for BusyBackoff.
func (s *sim) busy(r *sreq) {
	p := r.from
	if !r.active {
		return
	}
	p.finish(r)
	p.release(r.piece)
	s.res.Busy++
	p.busyUntil[r.to.idx] = s.now + s.cfg.BusyBackoff
	s.push(&event{at: s.now + s.cfg.BusyBackoff, kind: evTimeout, req: &sreq{from: p}}) // re-schedule then
	s.schedule(p)
}

func (s *sim) timeout(r *sreq) {
	if r.to == nil { // endgame poll or busy back-off expiry
		r.from.pollPending = false
		s.schedule(r.from)
		return
	}
	p := r.from
	if !r.active {
		return
	}
	p.finish(r)
	p.stats[r.to.idx].OnTimeout(at(s.now))
	s.res.Timeouts++
	p.release(r.piece)
	s.push(&event{at: s.now + s.lat(p, r.to), kind: evCancelArrive, req: r})
	s.schedule(p)
}
