package scheduler

import (
	"cmp"
	"slices"
	"time"
)

// InFlight describes a piece that has at least one outstanding request.
type InFlight struct {
	Piece    int
	Requests int
	// Age is how long the oldest outstanding request for the piece has waited.
	Age time.Duration
	// Expected is the expected completion time of that oldest request.
	Expected time.Duration
}

// EndgamePolicy decides which in-flight pieces get a duplicate request once
// every missing piece is already requested. Only overdue requests are
// duplicated, so endgame spends extra bandwidth where a slow or stuck peer is
// actually delaying completion, not on every remaining piece.
type EndgamePolicy struct {
	Enabled bool
	// MaxDuplicates is the most concurrent requests one piece may have.
	MaxDuplicates int
	// Slack: a request is overdue once Age ≥ Slack × Expected.
	Slack float64
	// MinAge prevents duplicating requests younger than this.
	MinAge time.Duration
}

func DefaultEndgame() EndgamePolicy {
	return EndgamePolicy{Enabled: true, MaxDuplicates: 2, Slack: 1.5, MinAge: 200 * time.Millisecond}
}

// Overdue returns the pieces eligible for a duplicate request, most overdue first
// (ties by piece index, for determinism).
func (p EndgamePolicy) Overdue(in []InFlight) []int {
	if !p.Enabled {
		return nil
	}
	type cand struct {
		piece int
		ratio float64
	}
	var cs []cand
	for _, f := range in {
		if f.Requests >= p.MaxDuplicates || f.Age < p.MinAge {
			continue
		}
		exp := max(f.Expected, time.Millisecond)
		if r := f.Age.Seconds() / exp.Seconds(); r >= p.Slack {
			cs = append(cs, cand{f.Piece, r})
		}
	}
	slices.SortFunc(cs, func(a, b cand) int {
		if c := cmp.Compare(b.ratio, a.ratio); c != 0 {
			return c
		}
		return cmp.Compare(a.piece, b.piece)
	})
	out := make([]int, len(cs))
	for i, c := range cs {
		out[i] = c.piece
	}
	return out
}
