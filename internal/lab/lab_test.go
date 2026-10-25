package lab

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/events"
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

func TestObserveDetectionAndReReplication(t *testing.T) {
	t0 := time.Unix(1000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	applied := []AppliedEvent{{Peer: "r1", Action: "kill", At: at(0)}}
	all := map[string][]events.Event{
		"d1": {
			{Time: at(-500), Type: events.PeerDisconnected, Fields: map[string]any{"remote": "aaaa"}}, // before the kill
			{Time: at(40), Type: events.PeerDisconnected, Fields: map[string]any{"remote": "rrrr"}},
		},
		"s1": {
			{Time: at(-100), Type: events.ReplicationTarget},
			{Time: at(60), Type: events.RequestTimeout, Fields: map[string]any{"remote": "rrrr"}},
			{Time: at(100), Type: events.UnderReplicated},
			{Time: at(900), Type: events.ReplicationTarget},
		},
	}
	got := observe(applied, all, map[string]string{"rrrr": "r1", "aaaa": "s9"})
	want := []Observation{
		{Kind: "detect_failure", Peer: "d1", Target: "r1", Seconds: 0.04},
		{Kind: "detect_failure", Peer: "s1", Target: "r1", Seconds: 0.06},
		{Kind: "re_replicate", Peer: "s1", Target: "r1", Seconds: 0.9},
	}
	if len(got) != len(want) {
		t.Fatalf("observations %+v", got)
	}
	for i := range want {
		if got[i].Kind != want[i].Kind || got[i].Peer != want[i].Peer || math.Abs(got[i].Seconds-want[i].Seconds) > 1e-9 {
			t.Errorf("observation %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestRepositoryScenariosLoad validates every scenario file shipped in the repo.
func TestRepositoryScenariosLoad(t *testing.T) {
	var files []string
	filepath.WalkDir("../../scenarios", func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".json") {
			files = append(files, path)
		}
		return nil
	})
	if len(files) < 12 {
		t.Fatalf("found only %d scenario files", len(files))
	}
	for _, f := range files {
		sc, err := LoadScenario(f)
		if err != nil {
			t.Errorf("%s: %v", f, err)
			continue
		}
		for _, p := range sc.Peers {
			if strings.HasPrefix(p.Has, "corrupt:") && p.Role != "seed" {
				t.Errorf("%s: corrupt data only makes sense for seeds", f)
			}
		}
	}
	t.Logf("%d scenario files load", len(files))
}

func TestVariantArgsByRole(t *testing.T) {
	v := Variant{Args: []string{"-a"}, RoleArgs: map[string][]string{"*": {"-all"}, "seed": {"-s"}}}
	if got := strings.Join(v.argsFor("get"), " "); got != "-a -all" {
		t.Errorf("get: %q", got)
	}
	if got := strings.Join(v.argsFor("seed"), " "); got != "-all -s" {
		t.Errorf("seed: %q", got)
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
