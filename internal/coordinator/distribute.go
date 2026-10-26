package coordinator

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/events"
	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// DistributeConfig controls seed-side distribution, a Selective-Repeat-style
// scheme for getting every piece out of a seed once before any piece is sent
// twice (decision 0007):
//
//   - The seed advertises nothing at first, then reveals to each peer a window
//     of distinct, never-sent pieces (HAVE). Peers request only what they see.
//   - A piece is acknowledged when any peer announces it (BITFIELD/HAVE); that
//     frees a window slot and the next unsent piece is revealed.
//   - Only unacknowledged pieces are repeated: those held by a peer that
//     disconnects, or not acknowledged within AckTimeout, become unsent again.
//   - Once every piece has been acknowledged, the seed advertises everything
//     and behaves as a normal seed.
//
// It needs no protocol change: peers simply see a seed that appears to hold a
// moving handful of pieces. Only seeds holding the complete file distribute.
type DistributeConfig struct {
	Enabled bool
	// Window is how many revealed-but-unacknowledged pieces a peer may hold.
	// It should be at least the peers' per-peer request depth (MaxPerPeer).
	Window     int
	AckTimeout time.Duration
}

func DefaultDistribute() DistributeConfig {
	return DistributeConfig{Window: 8, AckTimeout: 20 * time.Second}
}

type distPiece uint8

const (
	distUnsent distPiece = iota
	distAssigned
	distAcked
)

// distributor is owned by the coordinator's Run goroutine.
type distributor struct {
	active   bool // false once every piece is acknowledged (or when disabled)
	state    []distPiece
	owner    []protocol.PeerID // for assigned pieces
	since    []time.Time       // when assigned
	order    []int             // order in which unsent pieces are handed out
	next     int               // scan position in order
	retry    []int             // pieces lost or timed out: handed out before untouched ones
	acked    int
	repeated int // pieces re-assigned after their holder left or timed out
}

func newDistributor(cfg DistributeConfig, n int, seed uint64, complete bool) *distributor {
	d := &distributor{active: cfg.Enabled && complete}
	if !d.active {
		return d
	}
	d.state = make([]distPiece, n)
	d.owner = make([]protocol.PeerID, n)
	d.since = make([]time.Time, n)
	d.order = rand.New(rand.NewPCG(seed, 0xd157)).Perm(n)
	return d
}

// initialBitfield is what we announce to a new peer.
func (co *Coordinator) initialBitfield(p *peer) []byte {
	if !co.dist.active {
		return co.have.Bytes()
	}
	p.advertised = bitfield.New(co.m.NumPieces()) // filled by reveals
	return p.advertised.Bytes()
}

// windowUsed counts pieces assigned to p and not yet acknowledged.
func (co *Coordinator) windowUsed(p *peer) int {
	n := 0
	for i, s := range co.dist.state {
		if s == distAssigned && co.dist.owner[i] == p.id {
			n++
		}
	}
	return n
}

// fillWindow reveals unsent pieces to p until its window is full.
func (co *Coordinator) fillWindow(p *peer, now time.Time) {
	d := co.dist
	if !d.active || p.closing || !p.gotBitfield {
		return
	}
	free := co.cfg.Distribute.Window - co.windowUsed(p)
	assign := func(i int) bool {
		if d.state[i] != distUnsent || p.has.Has(i) {
			return false
		}
		d.state[i], d.owner[i], d.since[i] = distAssigned, p.id, now
		if !p.advertised.Has(i) {
			p.advertised.Set(i)
			p.c.SendControl(protocol.Have{Piece: uint32(i)})
		}
		free--
		return true
	}
	// Repeats first: these are the pieces at risk of never leaving the seed.
	kept := d.retry[:0]
	for _, i := range d.retry {
		if free == 0 || !assign(i) {
			if d.state[i] == distUnsent {
				kept = append(kept, i)
			}
		}
	}
	d.retry = kept
	for scanned := 0; free > 0 && scanned < len(d.order); scanned++ {
		i := d.order[d.next]
		d.next = (d.next + 1) % len(d.order)
		assign(i)
	}
}

// distAck records that piece i reached some peer and refills the window it used.
func (co *Coordinator) distAck(i int, now time.Time) {
	d := co.dist
	if !d.active || d.state[i] == distAcked {
		return
	}
	owner := d.owner[i]
	wasAssigned := d.state[i] == distAssigned
	d.state[i] = distAcked
	d.acked++
	if d.acked == len(d.state) {
		co.finishDistribution()
		return
	}
	if wasAssigned {
		if p := co.peers[owner]; p != nil {
			co.fillWindow(p, now)
		}
	}
}

// distRelease returns pieces assigned to a departing peer to the unsent pool and
// gives them to others ("selective repeat" of only what was lost).
func (co *Coordinator) distRelease(id protocol.PeerID, now time.Time) {
	d := co.dist
	if !d.active {
		return
	}
	released := 0
	for i, s := range d.state {
		if s == distAssigned && d.owner[i] == id {
			d.state[i] = distUnsent
			d.retry = append(d.retry, i)
			released++
		}
	}
	if released == 0 {
		return
	}
	d.repeated += released
	co.emit(events.DistributionRepeat, fmt.Sprintf("%d pieces assigned to %s were not acknowledged before it left; reassigning them",
		released, id.Short()), map[string]any{"pieces": released, "remote": id.Short()})
	for _, p := range co.peerList {
		co.fillWindow(p, now)
	}
}

// distTimeouts repeats pieces whose assignee has not acknowledged them in time.
func (co *Coordinator) distTimeouts(now time.Time) {
	d := co.dist
	if !d.active {
		return
	}
	expired := 0
	for i, s := range d.state {
		if s == distAssigned && now.Sub(d.since[i]) > co.cfg.Distribute.AckTimeout {
			d.state[i] = distUnsent // the peer keeps it advertised and may still fetch it
			d.retry = append(d.retry, i)
			expired++
		}
	}
	if expired > 0 {
		d.repeated += expired
		co.emit(events.DistributionRepeat, fmt.Sprintf("%d assigned pieces were not acknowledged within %s; reassigning them",
			expired, co.cfg.Distribute.AckTimeout), map[string]any{"pieces": expired})
		for _, p := range co.peerList {
			co.fillWindow(p, now)
		}
	}
}

// finishDistribution switches to normal seeding once every piece left the seed.
func (co *Coordinator) finishDistribution() {
	co.dist.active = false
	for _, p := range co.peerList {
		if p.closing {
			continue
		}
		for i := range co.m.NumPieces() {
			if !p.advertised.Has(i) && co.have.Has(i) {
				p.advertised.Set(i)
				p.c.SendControl(protocol.Have{Piece: uint32(i)})
			}
		}
	}
	co.emit(events.DistributionComplete, fmt.Sprintf("every piece has reached at least one peer (%d re-sent after losses); now seeding normally",
		co.dist.repeated), map[string]any{"repeated": co.dist.repeated})
	co.log.Info("distribution complete; seeding normally", "repeated", co.dist.repeated)
}

// DistributionStatus summarises seed-side distribution.
type DistributionStatus struct {
	Active   bool `json:"active"`
	Unsent   int  `json:"unsent"`
	Assigned int  `json:"assigned"`
	Acked    int  `json:"acknowledged"`
	Repeated int  `json:"repeated"`
}

func (co *Coordinator) distributionStatus() *DistributionStatus {
	d := co.dist
	if d.state == nil {
		return nil
	}
	s := &DistributionStatus{Active: d.active, Acked: d.acked, Repeated: d.repeated}
	for _, st := range d.state {
		switch st {
		case distUnsent:
			s.Unsent++
		case distAssigned:
			s.Assigned++
		}
	}
	return s
}
