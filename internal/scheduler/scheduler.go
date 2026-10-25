// Package scheduler holds the download policies: which piece to fetch next
// (PiecePicker) and which peer to fetch it from (PeerSelector). Policies are pure
// functions of the snapshot they are given — no networking, storage or clocks —
// so they can be tested and compared on synthetic workloads.
package scheduler

import (
	"fmt"
	"math/rand/v2"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// View is the coordinator's state as seen by a PiecePicker.
type View interface {
	NumPieces() int
	// Needed reports whether the piece is missing and has no request in flight.
	Needed(piece int) bool
	// Availability is the number of connected peers that have the piece.
	Availability(piece int) int
}

// PiecePicker offers pieces to try in priority order. try issues a request for
// the piece if a source has capacity and reports whether it did. Pick stops after
// limit successful tries and returns how many succeeded.
type PiecePicker interface {
	Name() string
	Pick(v View, limit int, try func(piece int) bool) int
}

// Candidate is a peer that has the piece and spare request capacity.
type Candidate struct {
	Peer        protocol.PeerID
	Outstanding int
}

// PeerSelector chooses one of the candidates (len(cands) ≥ 1) and returns its index.
type PeerSelector interface {
	Name() string
	Select(piece int, cands []Candidate) int
}

// Sequential fetches pieces in index order. It is the baseline picker.
type Sequential struct{}

func (Sequential) Name() string { return "sequential" }

func (Sequential) Pick(v View, limit int, try func(int) bool) int {
	n := 0
	for i := 0; i < v.NumPieces() && n < limit; i++ {
		if v.Needed(i) && v.Availability(i) > 0 && try(i) {
			n++
		}
	}
	return n
}

// RandomOrder fetches pieces in a fixed pseudo-random order derived from a seed.
type RandomOrder struct {
	seed  uint64
	order []int
}

func NewRandomOrder(seed uint64) *RandomOrder { return &RandomOrder{seed: seed} }

func (*RandomOrder) Name() string { return "random" }

func (r *RandomOrder) Pick(v View, limit int, try func(int) bool) int {
	if len(r.order) != v.NumPieces() {
		r.order = rand.New(rand.NewPCG(r.seed, 0x5157)).Perm(v.NumPieces())
	}
	n := 0
	for _, i := range r.order {
		if n >= limit {
			break
		}
		if v.Needed(i) && v.Availability(i) > 0 && try(i) {
			n++
		}
	}
	return n
}

// RandomPeer picks uniformly among candidates using a seeded generator.
type RandomPeer struct{ rng *rand.Rand }

func NewRandomPeer(seed uint64) *RandomPeer {
	return &RandomPeer{rng: rand.New(rand.NewPCG(seed, 0x9e37))}
}

func (*RandomPeer) Name() string { return "random" }

func (r *RandomPeer) Select(_ int, cands []Candidate) int { return r.rng.IntN(len(cands)) }

// NewPicker returns the named piece picker.
func NewPicker(name string, seed uint64) (PiecePicker, error) {
	switch name {
	case "sequential":
		return Sequential{}, nil
	case "random":
		return NewRandomOrder(seed), nil
	}
	return nil, fmt.Errorf("unknown piece picker %q (have: sequential, random)", name)
}

// NewSelector returns the named peer selector.
func NewSelector(name string, seed uint64) (PeerSelector, error) {
	switch name {
	case "random":
		return NewRandomPeer(seed), nil
	}
	return nil, fmt.Errorf("unknown peer selector %q (have: random)", name)
}
