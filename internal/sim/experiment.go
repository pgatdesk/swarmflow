package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/pgatdesk/swarmflow/internal/peerstats"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
)

const MiB = 1 << 20

// Variant is one policy combination under test.
type Variant struct {
	Picker   string `json:"picker"`
	Selector string `json:"selector"`
	Endgame  bool   `json:"endgame"`
	// QueueCap bounds each peer's total upload queue (0 = unbounded); see Config.ServerQueueCap.
	QueueCap int `json:"queue_cap"`
}

func (v Variant) String() string {
	s := v.Picker + "/" + v.Selector
	if !v.Endgame {
		s += " (no endgame)"
	}
	if v.QueueCap > 0 {
		s += fmt.Sprintf(" queue≤%d", v.QueueCap)
	}
	return s
}

// Scenario is a reproducible workload; Peers may depend on the trial number
// (join jitter, ordering) but never on the variant.
type Scenario struct {
	Name        string
	Description string
	FileSize    int64
	PieceSize   int
	Horizon     time.Duration
	Peers       func(trial uint64) []PeerSpec
	Variants    []Variant
}

func (sc Scenario) Config(v Variant, trial uint64) Config {
	picker := func(seed uint64) scheduler.PiecePicker {
		p, err := scheduler.NewPicker(v.Picker, seed)
		if err != nil {
			panic(err)
		}
		return p
	}
	selector := func(seed uint64) scheduler.PeerSelector {
		s, err := scheduler.NewSelector(v.Selector, seed)
		if err != nil {
			panic(err)
		}
		return s
	}
	eg := scheduler.DefaultEndgame()
	eg.Enabled = v.Endgame
	return Config{
		FileSize: sc.FileSize, PieceSize: sc.PieceSize, Peers: sc.Peers(trial),
		NewPicker: picker, NewSelector: selector,
		MaxOutstanding: 32, MaxPerPeer: 4, UploadSlots: 4, RequestTimeout: 10 * time.Second,
		Endgame: eg, Stats: peerstats.DefaultConfig(), Seed: trial, Horizon: sc.Horizon,
		ServerQueueCap: v.QueueCap, BusyBackoff: 250 * time.Millisecond,
	}
}

// Metrics summarises one run.
type Metrics struct {
	MeanCompletion  float64 `json:"mean_completion_s"` // over downloaders that finished
	Makespan        float64 `json:"makespan_s"`        // last completion; NaN-free: -1 if any did not finish
	CompletedFrac   float64 `json:"completed_frac"`
	SeedUploadRatio float64 `json:"seed_upload_ratio"` // seed bytes uploaded / file size
	WasteRatio      float64 `json:"waste_ratio"`       // wasted / delivered bytes
	Coverage        float64 `json:"coverage"`          // fraction of pieces still held by an online peer
	Timeouts        int     `json:"timeouts"`
	Busy            int     `json:"busy_rejects"`
	EndgameRequests int     `json:"endgame_requests"`
}

func Measure(cfg Config, r Result) Metrics {
	m := Metrics{Timeouts: r.Timeouts, Busy: r.Busy, EndgameRequests: r.EndgameRequests,
		Coverage: float64(r.Coverage) / float64(max(r.Pieces, 1))}
	var sum time.Duration
	var last time.Duration
	for _, c := range r.Completion {
		if c >= 0 {
			sum += c
			last = max(last, c)
		}
	}
	if r.Completed > 0 {
		m.MeanCompletion = (sum / time.Duration(r.Completed)).Seconds()
	}
	m.Makespan = last.Seconds()
	if r.Completed < r.Downloaders {
		m.Makespan = -1
	}
	m.CompletedFrac = float64(r.Completed) / float64(max(r.Downloaders, 1))
	m.SeedUploadRatio = float64(r.SeedUploaded) / float64(cfg.FileSize)
	if r.DeliveredBytes > 0 {
		m.WasteRatio = float64(r.WastedBytes) / float64(r.DeliveredBytes)
	}
	return m
}

// Summary is a metric's distribution across trials.
type Summary struct {
	Median float64 `json:"median"`
	P25    float64 `json:"p25"`
	P75    float64 `json:"p75"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func Summarize(xs []float64) Summary {
	s := slices.Clone(xs)
	slices.Sort(s)
	q := func(p float64) float64 { // linear interpolation between closest ranks
		if len(s) == 0 {
			return 0
		}
		pos := p * float64(len(s)-1)
		lo := int(pos)
		hi := min(lo+1, len(s)-1)
		return s[lo] + (s[hi]-s[lo])*(pos-float64(lo))
	}
	return Summary{Median: q(0.5), P25: q(0.25), P75: q(0.75), Min: s[0], Max: s[len(s)-1]}
}

type VariantResult struct {
	Variant         Variant   `json:"variant"`
	Trials          []Metrics `json:"trials"`
	MeanCompletion  Summary   `json:"mean_completion_s"`
	Makespan        Summary   `json:"makespan_s"`
	CompletedFrac   Summary   `json:"completed_frac"`
	SeedUploadRatio Summary   `json:"seed_upload_ratio"`
	WasteRatio      Summary   `json:"waste_ratio"`
	Coverage        Summary   `json:"coverage"`
	Timeouts        Summary   `json:"timeouts"`
	AllCompleted    int       `json:"trials_all_completed"`
}

type ScenarioResult struct {
	Scenario    string          `json:"scenario"`
	Description string          `json:"description"`
	TrialCount  int             `json:"trials"`
	Variants    []VariantResult `json:"variants"`
}

// RunScenario runs every variant on trials 1..n with identical workloads.
func RunScenario(sc Scenario, n int) ScenarioResult {
	out := ScenarioResult{Scenario: sc.Name, Description: sc.Description, TrialCount: n}
	for _, v := range sc.Variants {
		vr := VariantResult{Variant: v}
		var mean, span, frac, seedUp, waste, cov, tos []float64
		for trial := uint64(1); trial <= uint64(n); trial++ {
			cfg := sc.Config(v, trial)
			m := Measure(cfg, Run(cfg))
			vr.Trials = append(vr.Trials, m)
			mean, frac, seedUp, waste = append(mean, m.MeanCompletion), append(frac, m.CompletedFrac),
				append(seedUp, m.SeedUploadRatio), append(waste, m.WasteRatio)
			cov, tos = append(cov, m.Coverage), append(tos, float64(m.Timeouts))
			if m.Makespan >= 0 {
				span = append(span, m.Makespan)
				vr.AllCompleted++
			}
		}
		vr.MeanCompletion, vr.CompletedFrac = Summarize(mean), Summarize(frac)
		vr.SeedUploadRatio, vr.WasteRatio = Summarize(seedUp), Summarize(waste)
		vr.Coverage, vr.Timeouts = Summarize(cov), Summarize(tos)
		if len(span) > 0 {
			vr.Makespan = Summarize(span)
		}
		out.Variants = append(out.Variants, vr)
	}
	return out
}

// Markdown renders a result table: medians with [p25–p75] across trials.
func (r ScenarioResult) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "### %s\n\n%s\n\nTrials: %d per variant (identical workloads per trial). Values: median [p25–p75].\n\n",
		r.Scenario, r.Description, r.TrialCount)
	b.WriteString("| variant | mean completion (s) | makespan (s) | completed | pieces surviving | seed upload (× file) | timeouts | wasted bytes |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	iqr := func(s Summary, f string) string {
		return fmt.Sprintf(f+" ["+f+"–"+f+"]", s.Median, s.P25, s.P75)
	}
	for _, v := range r.Variants {
		span := "—"
		if v.AllCompleted > 0 {
			span = iqr(v.Makespan, "%.1f")
			if v.AllCompleted < r.TrialCount {
				span += fmt.Sprintf(" (%d/%d trials)", v.AllCompleted, r.TrialCount)
			}
		}
		mean := iqr(v.MeanCompletion, "%.1f")
		if v.CompletedFrac.Max == 0 {
			mean = "— (none finished)"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s |\n", v.Variant, mean, span,
			iqr(v.CompletedFrac, "%.2f"), iqr(v.Coverage, "%.3f"), iqr(v.SeedUploadRatio, "%.2f"),
			iqr(v.Timeouts, "%.0f"), iqr(v.WasteRatio, "%.3f"))
	}
	return b.String()
}

func jitter(rng *rand.Rand, d time.Duration) time.Duration {
	return time.Duration(rng.Int64N(int64(d)))
}

// Scenarios are the standard comparison workloads.
func Scenarios() []Scenario {
	pickerVariants := []Variant{
		{"sequential", "random", true, 0},
		{"random", "random", true, 0},
		{"rarest", "random", true, 0},
		{"rarest", "adaptive", true, 0},
		{"sequential", "random", true, 16},
		{"random", "random", true, 16},
		{"rarest", "random", true, 16},
		{"rarest", "adaptive", true, 16},
	}
	selectorVariants := []Variant{
		{"rarest", "random", true, 0},
		{"rarest", "fastest", true, 0},
		{"rarest", "adaptive", true, 0},
		{"rarest", "adaptive", false, 0},
	}
	flash := func(seedLeaveAt time.Duration) func(uint64) []PeerSpec {
		return func(trial uint64) []PeerSpec {
			rng := rand.New(rand.NewPCG(trial, 1))
			peers := []PeerSpec{{Name: "seed", Seed: true, Up: 8 * MiB, Latency: 20 * time.Millisecond, LeaveAt: seedLeaveAt}}
			for i := range 20 {
				peers = append(peers, PeerSpec{
					Name: fmt.Sprintf("d%02d", i), Up: 1 * MiB,
					Latency: time.Duration(10+(i*7)%50) * time.Millisecond,
					JoinAt:  jitter(rng, 2*time.Second),
				})
			}
			return peers
		}
	}
	hetero := func(fastLeavesAt time.Duration) func(uint64) []PeerSpec {
		return func(trial uint64) []PeerSpec {
			rng := rand.New(rand.NewPCG(trial, 2))
			seeds := []PeerSpec{
				{Name: "s-fast", Seed: true, Up: 16 * MiB, Latency: 5 * time.Millisecond, LeaveAt: fastLeavesAt},
				{Name: "s-mid", Seed: true, Up: 4 * MiB, Latency: 20 * time.Millisecond},
				{Name: "s-slow", Seed: true, Up: 1 * MiB, Latency: 50 * time.Millisecond},
				{Name: "s-far", Seed: true, Up: 0.25 * MiB, Latency: 200 * time.Millisecond},
			}
			rng.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })
			for i := range 4 {
				seeds = append(seeds, PeerSpec{Name: fmt.Sprintf("d%d", i), Latency: 10 * time.Millisecond,
					JoinAt: jitter(rng, 500*time.Millisecond)})
			}
			return seeds
		}
	}
	return []Scenario{
		{
			Name: "flash-crowd",
			Description: "1 seed (8 MiB/s up) and 20 downloaders (1 MiB/s up each, 10–59 ms latency) joining within 2 s; " +
				"64 MiB file, 256 KiB pieces. Downloaders upload to each other.",
			FileSize: 64 * MiB, PieceSize: 256 << 10, Horizon: 10 * time.Minute,
			Peers: flash(0), Variants: pickerVariants,
		},
		{
			Name: "seed-departs",
			Description: "As flash-crowd, but the only seed leaves after 12 s (time to upload ~1.5 copies). " +
				"Downloaders can only finish if, between them, they hold every piece.",
			FileSize: 64 * MiB, PieceSize: 256 << 10, Horizon: 10 * time.Minute,
			Peers: flash(12 * time.Second), Variants: pickerVariants,
		},
		{
			Name: "heterogeneous-seeds",
			Description: "4 seeds with very different upload capacity and latency (16 MiB/s·5 ms, 4 MiB/s·20 ms, " +
				"1 MiB/s·50 ms, 0.25 MiB/s·200 ms) serving 4 downloaders that do not upload; 64 MiB file.",
			FileSize: 64 * MiB, PieceSize: 256 << 10, Horizon: 10 * time.Minute,
			Peers: hetero(0), Variants: selectorVariants,
		},
		{
			Name:        "heterogeneous-churn",
			Description: "As heterogeneous-seeds, but the fastest seed leaves after 2 s.",
			FileSize:    64 * MiB, PieceSize: 256 << 10, Horizon: 10 * time.Minute,
			Peers: hetero(2 * time.Second), Variants: selectorVariants,
		},
	}
}
