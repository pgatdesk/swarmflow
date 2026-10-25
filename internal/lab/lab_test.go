package lab

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRateBits(t *testing.T) {
	for in, want := range map[string]float64{"100mbit": 100e6, "2.5mbit": 2.5e6, "1gbit": 1e9, "800kbit": 800e3, "64bit": 64} {
		got, err := Link{Rate: in}.RateBits()
		if err != nil || got != want {
			t.Errorf("%s: got %v %v, want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "fast", "-1mbit", "0mbit"} {
		if _, err := (Link{Rate: bad}).RateBits(); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestNetemArgs(t *testing.T) {
	got := strings.Join(Link{Rate: "20mbit", Delay: "15ms", Jitter: "5ms", Loss: "1"}.netemArgs(), " ")
	want := "delay 15ms 5ms distribution normal rate 20mbit loss 1% limit 1000"
	if got != want {
		t.Fatalf("netem args %q, want %q", got, want)
	}
}

func TestPieceFilter(t *testing.T) {
	collect := func(has string) []int {
		f, err := pieceFilter(has, 10)
		if err != nil {
			t.Fatal(err)
		}
		var out []int
		for i := range 10 {
			if f(i) {
				out = append(out, i)
			}
		}
		return out
	}
	if got := collect("even"); !slices.Equal(got, []int{0, 2, 4, 6, 8}) {
		t.Errorf("even: %v", got)
	}
	if got := collect("first:30%"); !slices.Equal(got, []int{0, 1, 2}) {
		t.Errorf("first:30%%: %v", got)
	}
	if got := collect("last:20%"); !slices.Equal(got, []int{8, 9}) {
		t.Errorf("last:20%%: %v", got)
	}
	if _, err := pieceFilter("some", 10); err == nil {
		t.Error("unknown filter accepted")
	}
}

func writeScenario(t *testing.T, body string) string {
	p := filepath.Join(t.TempDir(), "s.json")
	os.WriteFile(p, []byte(body), 0o644)
	return p
}

func TestLoadScenarioValidation(t *testing.T) {
	ok := `{"name":"x","file_size":"1MiB","peers":[
		{"name":"s","role":"seed","link":{"rate":"10mbit","delay":"5ms"}},
		{"name":"d","role":"get","link":{"rate":"10mbit","delay":"5ms"},"start_at":"1s"}],
		"events":[{"at":"2s","peer":"s","action":"kill"}],
		"variants":[{"name":"v","args":[]}]}`
	sc, err := LoadScenario(writeScenario(t, ok))
	if err != nil {
		t.Fatal(err)
	}
	if sc.PieceSize != "256KiB" || sc.Timeout.Duration != 5*time.Minute || sc.Peers[1].StartAt.Duration != time.Second {
		t.Fatalf("defaults/durations not applied: %+v", sc)
	}
	bad := map[string]string{
		"no downloader":  strings.Replace(ok, `"role":"get"`, `"role":"seed"`, 1),
		"duplicate name": strings.Replace(ok, `"name":"d"`, `"name":"s"`, 1),
		"bad role":       strings.Replace(ok, `"role":"get"`, `"role":"leech"`, 1),
		"bad rate":       strings.Replace(ok, `"rate":"10mbit","delay":"5ms"}},`, `"rate":"lots","delay":"5ms"}},`, 1),
		"unknown peer":   strings.Replace(ok, `"peer":"s"`, `"peer":"nobody"`, 1),
		"bad action":     strings.Replace(ok, `"action":"kill"`, `"action":"explode"`, 1),
		"unknown field":  strings.Replace(ok, `"name":"x"`, `"name":"x","colour":"red"`, 1),
	}
	for name, body := range bad {
		if _, err := LoadScenario(writeScenario(t, body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSummariesAndFairness(t *testing.T) {
	sc := &Scenario{
		Peers: []PeerSpec{
			{Name: "fast", Role: "seed", Link: Link{Rate: "30mbit"}},
			{Name: "slow", Role: "seed", Link: Link{Rate: "10mbit"}},
			{Name: "d", Role: "get", Link: Link{Rate: "100mbit"}},
		},
		Variants: []Variant{{Name: "prop"}, {Name: "even"}},
	}
	trial := func(v string, fast, slow int64, secs float64) TrialResult {
		return TrialResult{Variant: v, Peers: []PeerResult{{Name: "d", Role: "get", Completed: true, CompletionS: secs,
			Requests: 100, Timeouts: 2, Sources: map[string]int64{"fast": fast, "slow": slow}}}}
	}
	r := &ScenarioResult{Scenario: sc, Trials: []TrialResult{
		trial("prop", 75, 25, 4), trial("prop", 75, 25, 6), trial("even", 50, 50, 5),
	}}
	sums := r.Summaries()
	prop, even := sums[0], sums[1]
	if prop.MeanCompletion.med != 5 || prop.Success != 1 || math.Abs(prop.TimeoutRate.med-0.02) > 1e-9 {
		t.Fatalf("prop summary: %+v", prop)
	}
	// Load exactly proportional to capacity (3:1) is perfectly fair.
	if math.Abs(prop.Fairness.med-1) > 1e-9 {
		t.Fatalf("proportional load fairness %v, want 1", prop.Fairness.med)
	}
	// Equal load on unequal capacity: x = (0.5/0.75, 0.5/0.25) → Jain = 0.8.
	if math.Abs(even.Fairness.med-0.8) > 1e-9 {
		t.Fatalf("even load fairness %v, want 0.8", even.Fairness.med)
	}
	if math.Abs(prop.Shares["fast"]-0.75) > 1e-9 {
		t.Fatalf("share %v", prop.Shares["fast"])
	}
}
