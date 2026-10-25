package scheduler

import (
	"math"
	"math/rand/v2"
	"slices"
	"time"
)

const (
	// DefaultPriorThroughput is assumed for every peer when none has history yet.
	// Its value only matters relative to measured peers, which then replace it.
	DefaultPriorThroughput = 1 << 20
	// ReliabilityFloor keeps an unreliable peer's cost finite.
	ReliabilityFloor = 0.05
	// DefaultExplore is the fraction of adaptive decisions spent measuring the
	// least-sampled candidate.
	DefaultExplore = 0.1
)

// Priors returns the values assumed for candidates without enough history:
// the median of the candidates that have it, so a new peer is treated as
// typical rather than as best or worst.
func Priors(cands []Candidate) (throughput float64, rtt time.Duration) {
	var tps []float64
	var rtts []time.Duration
	for _, c := range cands {
		if c.HasThroughput {
			tps = append(tps, c.Throughput)
		}
		if c.HasRTT {
			rtts = append(rtts, c.RTT)
		}
	}
	throughput = DefaultPriorThroughput
	if len(tps) > 0 {
		slices.Sort(tps)
		throughput = tps[len(tps)/2]
	}
	if len(rtts) > 0 {
		slices.Sort(rtts)
		rtt = rtts[len(rtts)/2]
	}
	return throughput, rtt
}

// RandomPeer picks uniformly among candidates using a seeded generator.
type RandomPeer struct{ rng *rand.Rand }

func NewRandomPeer(seed uint64) *RandomPeer {
	return &RandomPeer{rng: rand.New(rand.NewPCG(seed, 0x9e37))}
}

func (*RandomPeer) Name() string { return "random" }

func (r *RandomPeer) Select(_, _ int, cands []Candidate) int { return r.rng.IntN(len(cands)) }

// FastestPeer picks the highest measured throughput, ignoring RTT, reliability
// and load (beyond the per-peer request cap). Ties go to the less-loaded peer.
type FastestPeer struct{}

func (FastestPeer) Name() string { return "fastest" }

func (FastestPeer) Select(_, _ int, cands []Candidate) int {
	prior, _ := Priors(cands)
	best, bestTP := 0, -1.0
	for i, c := range cands {
		tp := prior
		if c.HasThroughput {
			tp = c.Throughput
		}
		if tp > bestTP || (tp == bestTP && c.Outstanding < cands[best].Outstanding) {
			best, bestTP = i, tp
		}
	}
	return best
}

// AdaptivePeer minimises Cost: the expected time to receive the piece from a
// peer, inflated by its unreliability. With probability Explore it instead
// picks the least-sampled candidate so new or idle peers get measured.
type AdaptivePeer struct {
	Explore float64
	rng     *rand.Rand
}

func NewAdaptivePeer(seed uint64, explore float64) *AdaptivePeer {
	return &AdaptivePeer{Explore: explore, rng: rand.New(rand.NewPCG(seed, 0xada9))}
}

func (*AdaptivePeer) Name() string { return "adaptive" }

func (a *AdaptivePeer) Select(_, size int, cands []Candidate) int {
	if a.Explore > 0 && len(cands) > 1 && a.rng.Float64() < a.Explore {
		least := 0
		for i, c := range cands {
			if c.Samples < cands[least].Samples {
				least = i
			}
		}
		return least
	}
	tp, rtt := Priors(cands)
	best, bestCost := 0, math.Inf(1)
	for i, c := range cands {
		if cost := Cost(c, size, tp, rtt); cost < bestCost {
			best, bestCost = i, cost
		}
	}
	return best
}

// Cost is the adaptive objective in seconds:
//
//	ECT  = RTT + (outstanding + 1) × size / throughput
//	cost = ECT / max(reliability, ReliabilityFloor)
//
// ECT is the expected completion time of a new request queued behind the
// peer's outstanding ones; dividing by reliability approximates the expected
// time including retries elsewhere. Candidates without history use the priors.
func Cost(c Candidate, size int, priorTP float64, priorRTT time.Duration) float64 {
	tp, rtt := priorTP, priorRTT
	if c.HasThroughput && c.Throughput > 0 {
		tp = c.Throughput
	}
	if c.HasRTT {
		rtt = c.RTT
	}
	ect := rtt.Seconds() + float64(c.Outstanding+1)*float64(size)/tp
	return ect / max(c.Reliability, ReliabilityFloor)
}
