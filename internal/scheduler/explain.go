package scheduler

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

// Factors are the measured inputs behind one candidate in a decision.
type Factors struct {
	Peer            string  `json:"peer"`
	ThroughputBps   float64 `json:"throughput_bps"` // measured, or the prior if not yet measured
	ThroughputKnown bool    `json:"throughput_measured"`
	RTTms           float64 `json:"rtt_ms"`
	RTTKnown        bool    `json:"rtt_measured"`
	Reliability     float64 `json:"reliability"`
	Outstanding     int     `json:"outstanding"`
	Samples         int     `json:"samples"`
	// ExpectedS is the adaptive cost (expected completion ÷ reliability),
	// computed for every selector so decisions can be compared.
	ExpectedS float64 `json:"expected_s"`
}

// Explanation describes why a selector chose a peer, from the same
// candidates and measurements the selector saw.
type Explanation struct {
	Selector   string   `json:"selector"`
	Reason     string   `json:"reason"`
	Chosen     Factors  `json:"chosen"`
	RunnerUp   *Factors `json:"runner_up,omitempty"`
	Candidates int      `json:"candidates"`
}

func factors(c Candidate, size int, priorTP float64, priorRTT time.Duration) Factors {
	f := Factors{Peer: c.Peer.Short(), ThroughputBps: priorTP, ThroughputKnown: c.HasThroughput,
		RTTms: float64(priorRTT) / float64(time.Millisecond), RTTKnown: c.HasRTT,
		Reliability: c.Reliability, Outstanding: c.Outstanding, Samples: c.Samples,
		ExpectedS: Cost(c, size, priorTP, priorRTT)}
	if c.HasThroughput && c.Throughput > 0 {
		f.ThroughputBps = c.Throughput
	}
	if c.HasRTT {
		f.RTTms = float64(c.RTT) / float64(time.Millisecond)
	}
	return f
}

func mbps(bps float64) string { return fmt.Sprintf("%.1f MB/s", bps/1e6) }

// Explain reconstructs the reasoning for choosing cands[chosen].
func Explain(selector string, size int, cands []Candidate, chosen int) Explanation {
	tp, rtt := Priors(cands)
	fs := make([]Factors, len(cands))
	for i, c := range cands {
		fs[i] = factors(c, size, tp, rtt)
	}
	ex := Explanation{Selector: selector, Chosen: fs[chosen], Candidates: len(cands)}
	order := make([]int, len(cands))
	for i := range order {
		order[i] = i
	}
	measured := func(f Factors) string {
		if f.ThroughputKnown {
			return mbps(f.ThroughputBps)
		}
		return mbps(f.ThroughputBps) + " assumed (not yet measured)"
	}
	switch selector {
	case "adaptive":
		slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(fs[a].ExpectedS, fs[b].ExpectedS) })
		c := fs[chosen]
		if order[0] != chosen && fs[order[0]].ExpectedS < c.ExpectedS {
			ex.Reason = fmt.Sprintf("exploration: least-sampled peer (%d completed requests)", c.Samples)
		} else {
			ex.Reason = fmt.Sprintf("lowest expected completion %.2fs (throughput %s, RTT %.1f ms, %d queued, reliability %.2f)",
				c.ExpectedS, measured(c), c.RTTms, c.Outstanding, c.Reliability)
		}
	case "fastest":
		slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(fs[b].ThroughputBps, fs[a].ThroughputBps) })
		ex.Reason = "highest throughput " + measured(fs[chosen])
	default:
		ex.Reason = fmt.Sprintf("random choice among %d eligible peers", len(cands))
		return ex
	}
	for _, i := range order {
		if i != chosen {
			r := fs[i]
			ex.RunnerUp = &r
			break
		}
	}
	return ex
}
