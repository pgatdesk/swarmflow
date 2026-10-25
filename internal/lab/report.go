package lab

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

type ScenarioResult struct {
	Scenario *Scenario     `json:"scenario"`
	Started  time.Time     `json:"started"`
	Finished time.Time     `json:"finished"`
	Trials   []TrialResult `json:"trials"`
}

type stat struct{ med, p25, p75 float64 }

func summarize(xs []float64) stat {
	if len(xs) == 0 {
		return stat{math.NaN(), math.NaN(), math.NaN()}
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	q := func(p float64) float64 {
		pos := p * float64(len(s)-1)
		lo := int(pos)
		hi := min(lo+1, len(s)-1)
		return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
	}
	return stat{q(0.5), q(0.25), q(0.75)}
}

func (s stat) fmt(f string) string {
	if math.IsNaN(s.med) {
		return "—"
	}
	return fmt.Sprintf(f+" ["+f+"–"+f+"]", s.med, s.p25, s.p75)
}

// VariantSummary aggregates one variant's trials.
type VariantSummary struct {
	Name           string
	Trials, Failed int
	MeanCompletion stat // seconds, mean over downloaders, per trial
	Makespan       stat // seconds, slowest downloader, per trial
	Throughput     stat // MiB/s per downloader
	Success        float64
	TimeoutRate    stat // timeouts / requests
	Fairness       stat // Jain index of capacity-normalised source load
	CPU, RSS       stat
	Shares         map[string]float64 // mean share of useful bytes per source
}

// capacityShares returns each initial source's (seed or replica) share of their
// total upload capacity. Downloaders also upload once they have pieces, but
// start with nothing, so they are excluded from the reference.
func capacityShares(sc *Scenario) map[string]float64 {
	total := 0.0
	rates := map[string]float64{}
	for _, p := range sc.Peers {
		if !isSource(sc, p.Name) {
			continue
		}
		r, _ := p.Link.RateBits()
		rates[p.Name] = r
		total += r
	}
	for k := range rates {
		rates[k] /= total
	}
	return rates
}

func (r *ScenarioResult) Summaries() []VariantSummary {
	caps := capacityShares(r.Scenario)
	var out []VariantSummary
	for _, v := range r.Scenario.Variants {
		vs := VariantSummary{Name: v.Name, Shares: map[string]float64{}}
		var mean, span, tput, trate, fair, cpu, rss []float64
		gets, done := 0, 0
		shareTrials := 0
		for _, t := range r.Trials {
			if t.Variant != v.Name {
				continue
			}
			vs.Trials++
			if t.Error != "" {
				vs.Failed++
				continue
			}
			var sum, worst float64
			var n, req, tos int
			bySource := map[string]int64{}
			var total int64
			allDone := true
			for _, p := range t.Peers {
				if p.Role != "get" {
					continue
				}
				gets++
				req += p.Requests
				tos += p.Timeouts
				cpu, rss = append(cpu, p.CPUSeconds), append(rss, p.MaxRSSMiB)
				if !p.Completed {
					allDone = false
					continue
				}
				done++
				n++
				sum += p.CompletionS
				worst = max(worst, p.CompletionS)
				tput = append(tput, p.ThroughputMiBps)
				for s, b := range p.Sources {
					bySource[s] += b
					total += b
				}
			}
			if n > 0 {
				mean = append(mean, sum/float64(n))
			}
			if allDone && n > 0 {
				span = append(span, worst)
			}
			if req > 0 {
				trate = append(trate, float64(tos)/float64(req))
			}
			if total > 0 {
				shareTrials++
				var sx, sx2 float64
				k := 0
				for _, p := range r.Scenario.Peers {
					vs.Shares[p.Name] += float64(bySource[p.Name]) / float64(total)
				}
				// Fairness among initial sources: load share relative to capacity share,
				// computed over what the sources delivered (excluding peer-to-peer trading).
				var fromSources int64
				for name := range caps {
					fromSources += bySource[name]
				}
				for name, c := range caps {
					if c > 0 && fromSources > 0 {
						x := float64(bySource[name]) / float64(fromSources) / c
						sx, sx2 = sx+x, sx2+x*x
						k++
					}
				}
				if sx2 > 0 {
					fair = append(fair, sx*sx/(float64(k)*sx2))
				}
			}
		}
		for k := range vs.Shares {
			vs.Shares[k] /= float64(max(shareTrials, 1))
		}
		if gets > 0 {
			vs.Success = float64(done) / float64(gets)
		}
		vs.MeanCompletion, vs.Makespan, vs.Throughput = summarize(mean), summarize(span), summarize(tput)
		vs.TimeoutRate, vs.Fairness = summarize(trate), summarize(fair)
		vs.CPU, vs.RSS = summarize(cpu), summarize(rss)
		out = append(out, vs)
	}
	return out
}

// isSource reports whether a peer can serve pieces from the start (seeds and replicas).
func isSource(sc *Scenario, name string) bool {
	for _, p := range sc.Peers {
		if p.Name == name {
			return p.Role == "seed" || p.Role == "replica"
		}
	}
	return false
}

func (r *ScenarioResult) Markdown() string {
	sc := r.Scenario
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\n%s\n\n", sc.Name, sc.Description)
	fmt.Fprintf(&b, "File %s in %s pieces. Each peer process runs in its own network namespace, with its access link "+
		"shaped by `tc netem` in both directions (decision 0005). Trials: %d per variant, interleaved. "+
		"Values are median [p25–p75] across trials.\n\n", sc.FileSize, sc.PieceSize, r.trialCount())
	b.WriteString("| peer | role | rate | one-way delay | jitter | loss | joins |\n|---|---|---|---|---|---|---|\n")
	for _, p := range sc.Peers {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", p.Name, p.Role, p.Link.Rate, p.Link.Delay,
			dash(p.Link.Jitter), dash(pct(p.Link.Loss)), p.StartAt.Duration)
	}
	if len(sc.Events) > 0 {
		b.WriteString("\nEvents: ")
		for i, e := range sc.Events {
			if i > 0 {
				b.WriteString("; ")
			}
			fmt.Fprintf(&b, "%s at %s → %s", e.Peer, e.At.Duration, e.Action)
		}
		b.WriteString("\n")
	}
	b.WriteString("\n| variant | mean completion (s) | makespan (s) | per-downloader MiB/s | completed | timeout rate | fairness (Jain, capacity-normalised) | CPU s / downloader | peak RSS MiB |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|\n")
	sums := r.Summaries()
	for _, v := range sums {
		name := v.Name
		if v.Failed > 0 {
			name += fmt.Sprintf(" (%d/%d trials errored)", v.Failed, v.Trials)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %.0f%% | %s | %s | %s | %s |\n", name, v.MeanCompletion.fmt("%.1f"),
			v.Makespan.fmt("%.1f"), v.Throughput.fmt("%.2f"), 100*v.Success, v.TimeoutRate.fmt("%.3f"),
			v.Fairness.fmt("%.2f"), v.CPU.fmt("%.1f"), v.RSS.fmt("%.0f"))
	}
	b.WriteString("\nShare of useful bytes downloaders received from each peer (mean across trials), next to each seed's " +
		"share of seed upload capacity. Downloaders also trade pieces with each other; their rows show that share.\n\n")
	b.WriteString("| source | capacity share |")
	for _, v := range sums {
		fmt.Fprintf(&b, " %s |", v.Name)
	}
	b.WriteString("\n|---|---|" + strings.Repeat("---|", len(sums)) + "\n")
	caps := capacityShares(sc)
	for _, p := range sc.Peers {
		capShare := "—"
		if c, ok := caps[p.Name]; ok {
			capShare = fmt.Sprintf("%.2f", c)
		}
		fmt.Fprintf(&b, "| %s | %s |", p.Name, capShare)
		for _, v := range sums {
			fmt.Fprintf(&b, " %.2f |", v.Shares[p.Name])
		}
		b.WriteString("\n")
	}
	return b.String()
}

func (r *ScenarioResult) trialCount() int {
	n := map[string]int{}
	for _, t := range r.Trials {
		n[t.Variant]++
	}
	best := 0
	for _, c := range n {
		best = max(best, c)
	}
	return best
}

func dash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func pct(s string) string {
	if s == "" {
		return ""
	}
	return s + "%"
}
