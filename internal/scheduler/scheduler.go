// Package scheduler holds the download policies: which piece to fetch next
// (PiecePicker), which peer to fetch it from (PeerSelector), and when to issue
// duplicate requests near the end (EndgamePolicy). Policies are pure functions
// of the snapshot they are given — no networking, storage or clocks — so they
// can be unit-tested and compared in the simulator on synthetic workloads.
package scheduler

import (
	"fmt"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// View is the downloader's state as seen by a PiecePicker.
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

// Candidate is a peer that has the piece and spare request capacity, with the
// measurements a selector may use.
type Candidate struct {
	Peer        protocol.PeerID
	Outstanding int
	// Throughput is the smoothed payload delivery rate in bytes/s, valid when HasThroughput.
	Throughput    float64
	HasThroughput bool
	// RTT is the smoothed application round-trip time, valid when HasRTT.
	RTT    time.Duration
	HasRTT bool
	// Reliability is the smoothed fraction of successful requests, in [0, 1].
	Reliability float64
	// Samples is the number of completed requests behind the estimates.
	Samples int
}

// PeerSelector chooses one of the candidates (len(cands) ≥ 1) for a piece of
// size bytes and returns its index.
type PeerSelector interface {
	Name() string
	Select(piece, size int, cands []Candidate) int
}

// Pickers and Selectors list the policy names accepted by NewPicker and NewSelector.
var (
	Pickers   = []string{"sequential", "random", "rarest"}
	Selectors = []string{"random", "fastest", "adaptive"}
)

func NewPicker(name string, seed uint64) (PiecePicker, error) {
	switch name {
	case "sequential":
		return Sequential{}, nil
	case "random":
		return NewRandomOrder(seed), nil
	case "rarest":
		return NewRarestFirst(seed), nil
	}
	return nil, fmt.Errorf("unknown piece picker %q (have: %v)", name, Pickers)
}

func NewSelector(name string, seed uint64) (PeerSelector, error) {
	switch name {
	case "random":
		return NewRandomPeer(seed), nil
	case "fastest":
		return FastestPeer{}, nil
	case "adaptive":
		return NewAdaptivePeer(seed, DefaultExplore), nil
	}
	return nil, fmt.Errorf("unknown peer selector %q (have: %v)", name, Selectors)
}
