package scheduler

import (
	"slices"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

type fakeView struct {
	needed []bool
	avail  []int
}

func (v *fakeView) NumPieces() int         { return len(v.avail) }
func (v *fakeView) Needed(i int) bool      { return v.needed[i] }
func (v *fakeView) Availability(i int) int { return v.avail[i] }
func newView(avail ...int) *fakeView {
	needed := make([]bool, len(avail))
	for i := range needed {
		needed[i] = true
	}
	return &fakeView{needed: needed, avail: avail}
}

// pickAll records the order in which a picker offers pieces when every try succeeds.
func pickAll(p PiecePicker, v View, limit int) []int {
	var got []int
	p.Pick(v, limit, func(i int) bool { got = append(got, i); return true })
	return got
}

func TestRarestFirstOrder(t *testing.T) {
	v := newView(3, 1, 0, 2, 1, 5, 2)
	v.needed[3] = false // already in flight
	got := pickAll(NewRarestFirst(1), v, 100)
	avail := func(i int) int { return v.avail[i] }
	if len(got) != 5 {
		t.Fatalf("offered %v; want 5 pieces (unavailable and in-flight excluded)", got)
	}
	for k := 1; k < len(got); k++ {
		if avail(got[k]) < avail(got[k-1]) {
			t.Fatalf("not rarest-first: %v", got)
		}
	}
	if slices.Contains(got, 2) || slices.Contains(got, 3) {
		t.Fatalf("offered unavailable or in-flight piece: %v", got)
	}
}

func TestRarestFirstDeterministicTies(t *testing.T) {
	v := newView(slices.Repeat([]int{1}, 64)...)
	a := pickAll(NewRarestFirst(7), v, 10)
	b := pickAll(NewRarestFirst(7), v, 10)
	c := pickAll(NewRarestFirst(8), v, 10)
	if !slices.Equal(a, b) {
		t.Fatal("same seed gave different tie order")
	}
	if slices.Equal(a, c) {
		t.Fatal("different seeds gave the same tie order")
	}
}

func TestPickRespectsLimitAndSkipsFailedTries(t *testing.T) {
	v := newView(1, 1, 1, 1, 1)
	tried := 0
	n := Sequential{}.Pick(v, 2, func(i int) bool { tried++; return i%2 == 1 })
	if n != 2 || tried != 4 {
		t.Fatalf("succeeded %d after %d tries; want 2 after 4", n, tried)
	}
}

func cand(id byte, tp float64, rtt time.Duration, rel float64, out, samples int) Candidate {
	return Candidate{Peer: protocol.PeerID{id}, Throughput: tp, HasThroughput: samples >= 3, RTT: rtt,
		HasRTT: rtt > 0, Reliability: rel, Outstanding: out, Samples: samples}
}

func TestFastestPicksHighestThroughput(t *testing.T) {
	cs := []Candidate{cand(1, 1e6, 0, 1, 0, 10), cand(2, 5e6, 0, 0.1, 3, 10), cand(3, 2e6, 0, 1, 0, 10)}
	if got := (FastestPeer{}).Select(0, 1<<18, cs); got != 1 {
		t.Fatalf("fastest chose %d", got)
	}
}

func TestAdaptiveSpreadsLoadAndPenalizesUnreliable(t *testing.T) {
	a := NewAdaptivePeer(1, 0)
	size := 256 << 10
	// Peer 0 is 2x faster but already has 3 requests queued: (3+1)/2 > 1/1.
	cs := []Candidate{cand(1, 2e6, 0, 1, 3, 10), cand(2, 1e6, 0, 1, 0, 10)}
	if got := a.Select(0, size, cs); got != 1 {
		t.Fatalf("adaptive ignored load: chose %d", got)
	}
	// Same speed and load, but peer 0 times out half the time.
	cs = []Candidate{cand(1, 1e6, 0, 0.5, 0, 10), cand(2, 1e6, 0, 1, 0, 10)}
	if got := a.Select(0, size, cs); got != 1 {
		t.Fatalf("adaptive ignored reliability: chose %d", got)
	}
	// High RTT dominates for an otherwise equal peer.
	cs = []Candidate{cand(1, 1e6, 500*time.Millisecond, 1, 0, 10), cand(2, 1e6, 10*time.Millisecond, 1, 0, 10)}
	if got := a.Select(0, size, cs); got != 1 {
		t.Fatalf("adaptive ignored RTT: chose %d", got)
	}
}

func TestAdaptiveColdStartUsesMedianPrior(t *testing.T) {
	cs := []Candidate{cand(1, 1e6, 0, 1, 0, 10), cand(2, 3e6, 0, 1, 0, 10), cand(3, 0, 0, 1, 0, 0)}
	tp, _ := Priors(cs)
	if tp != 3e6 { // median of [1e6, 3e6] with upper-median rule
		t.Fatalf("prior %v", tp)
	}
	if c := Cost(cs[2], 1<<20, tp, 0); c != Cost(cs[1], 1<<20, tp, 0) {
		t.Fatal("unsampled peer should cost the same as a peer at the prior")
	}
}

func TestAdaptiveExplorationMeasuresNewPeers(t *testing.T) {
	a := NewAdaptivePeer(3, 0.5)
	cs := []Candidate{cand(1, 10e6, 0, 1, 0, 50), cand(2, 0, 0, 1, 0, 0)}
	explored := 0
	for range 1000 {
		if a.Select(0, 1<<18, cs) == 1 {
			explored++
		}
	}
	if explored < 400 || explored > 600 {
		t.Fatalf("explored %d/1000 with Explore=0.5", explored)
	}
}

func TestAdaptiveExploreNewOnlyStopsOnceMeasured(t *testing.T) {
	a := NewAdaptivePeer(3, 0.5)
	a.ExploreNewOnly = true
	slow := cand(2, 0.1e6, 0, 1, 0, 0)
	fast := cand(1, 10e6, 0, 1, 0, 50)
	explored := 0
	for range 1000 {
		if a.Select(0, 1<<18, []Candidate{fast, slow}) == 1 {
			explored++
		}
	}
	if explored < 400 {
		t.Fatalf("unmeasured peer explored only %d/1000 times", explored)
	}
	slow.Samples, slow.HasThroughput = NewPeerSamples, true
	for range 1000 {
		if a.Select(0, 1<<18, []Candidate{fast, slow}) == 1 {
			t.Fatal("measured slow peer still chosen for exploration")
		}
	}
}

func TestEndgameOverdue(t *testing.T) {
	p := DefaultEndgame()
	in := []InFlight{
		{Piece: 1, Requests: 1, Age: 3 * time.Second, Expected: time.Second},             // ratio 3
		{Piece: 2, Requests: 1, Age: time.Second, Expected: time.Second},                 // not overdue
		{Piece: 3, Requests: 2, Age: 10 * time.Second, Expected: time.Second},            // at max duplicates
		{Piece: 4, Requests: 1, Age: 6 * time.Second, Expected: time.Second},             // ratio 6
		{Piece: 5, Requests: 1, Age: 100 * time.Millisecond, Expected: time.Millisecond}, // below MinAge
		{Piece: 6, Requests: 1, Age: 3 * time.Second, Expected: time.Second},             // ties with 1
	}
	if got := p.Overdue(in); !slices.Equal(got, []int{4, 1, 6}) {
		t.Fatalf("overdue %v, want [4 1 6]", got)
	}
	p.Enabled = false
	if got := p.Overdue(in); got != nil {
		t.Fatalf("disabled endgame returned %v", got)
	}
}
