package scheduler

import "math/rand/v2"

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

func seededPerm(seed uint64, n int) []int {
	return rand.New(rand.NewPCG(seed, 0x5157)).Perm(n)
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
		r.order = seededPerm(r.seed, v.NumPieces())
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

// RarestFirst prefers pieces held by the fewest connected peers, so rare pieces
// are replicated before their sources leave and downloaders end up holding
// different pieces they can trade. Ties are broken by a seeded permutation:
// deterministic for a given seed, and different seeds on different peers keep
// downloaders from all choosing the same piece among equally rare ones.
//
// Pick is O(pieces + max availability) using bucket order.
type RarestFirst struct {
	seed    uint64
	order   []int
	buckets [][]int
}

func NewRarestFirst(seed uint64) *RarestFirst { return &RarestFirst{seed: seed} }

func (*RarestFirst) Name() string { return "rarest" }

func (r *RarestFirst) Pick(v View, limit int, try func(int) bool) int {
	if len(r.order) != v.NumPieces() {
		r.order = seededPerm(r.seed, v.NumPieces())
	}
	for a := range r.buckets {
		r.buckets[a] = r.buckets[a][:0]
	}
	for _, i := range r.order {
		if !v.Needed(i) {
			continue
		}
		a := v.Availability(i)
		if a <= 0 {
			continue
		}
		for len(r.buckets) <= a {
			r.buckets = append(r.buckets, nil)
		}
		r.buckets[a] = append(r.buckets[a], i)
	}
	n := 0
	for _, bucket := range r.buckets {
		for _, i := range bucket {
			if n >= limit {
				return n
			}
			if try(i) {
				n++
			}
		}
	}
	return n
}
