package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/events"
	"github.com/pgatdesk/swarmflow/internal/manifest"
)

type Options struct {
	BinDir string    // directory with linux swarmflow and tracker binaries
	Work   string    // scratch directory (fast local disk)
	Trials int       // trials per variant
	Only   []string  // variant names to run; empty = all
	Log    io.Writer // progress output
}

// PeerResult is one peer's outcome in one trial (across restarts, if any).
type PeerResult struct {
	Name            string                         `json:"name"`
	Role            string                         `json:"role"`
	Exit            string                         `json:"exit"` // ok, killed, stopped, timeout, failed: ...
	Runs            int                            `json:"runs"`
	Completed       bool                           `json:"completed"`
	CompletionS     float64                        `json:"completion_s,omitempty"`       // last run: coordinator start → complete
	FromFirstStartS float64                        `json:"from_first_start_s,omitempty"` // first process start → complete
	ThroughputMiBps float64                        `json:"throughput_mibps,omitempty"`
	WaitS           float64                        `json:"time_to_first_piece_s,omitempty"` // last run
	BytesDown       int64                          `json:"bytes_down"`
	BytesUp         int64                          `json:"bytes_up"`
	Requests        int                            `json:"requests"`
	Timeouts        int                            `json:"timeouts"`
	Reassigned      int                            `json:"reassigned"`
	Rejected        int                            `json:"rejected_pieces"`
	Banned          int                            `json:"banned"`
	EndgameRequests int                            `json:"endgame_requests"`
	BusyReceived    int                            `json:"busy_received"`
	Resume          *coordinator.ResumeInfo        `json:"resume,omitempty"`
	Replication     *coordinator.ReplicationStatus `json:"replication,omitempty"`
	Sources         map[string]int64               `json:"sources,omitempty"` // useful bytes received, by source peer name
	CPUSeconds      float64                        `json:"cpu_s"`
	MaxRSSMiB       float64                        `json:"max_rss_mib"`
}

// AppliedEvent is a scenario event as actually applied, with its wall-clock time.
type AppliedEvent struct {
	Peer   string    `json:"peer"`
	Action string    `json:"action"`
	At     time.Time `json:"at"`
}

// Observation is a duration measured from peers' event logs, e.g. how long a
// downloader took to notice a failure, or a replicator to restore copies.
type Observation struct {
	Kind    string  `json:"kind"`   // detect_failure, re_replicate, resume_overhead
	Peer    string  `json:"peer"`   // the peer that observed
	Target  string  `json:"target"` // the peer the event happened to
	Seconds float64 `json:"seconds"`
}

type TrialResult struct {
	Variant      string         `json:"variant"`
	Trial        int            `json:"trial"`
	WallS        float64        `json:"wall_s"`
	Error        string         `json:"error,omitempty"`
	Peers        []PeerResult   `json:"peers"`
	Applied      []AppliedEvent `json:"applied_events,omitempty"`
	Observations []Observation  `json:"observations,omitempty"`
}

// prepareData writes the scenario's file and manifest once.
func prepareData(sc *Scenario, work string) (src, mpath string, m *manifest.Manifest, err error) {
	size, err := dataset.ParseSize(sc.FileSize)
	if err != nil {
		return "", "", nil, err
	}
	ps, err := dataset.ParseSize(sc.PieceSize)
	if err != nil {
		return "", "", nil, err
	}
	dir := filepath.Join(work, "data", fmt.Sprintf("%s-%s-%d", sc.FileSize, sc.PieceSize, sc.DataSeed))
	src, mpath = filepath.Join(dir, "payload.bin"), filepath.Join(dir, "payload.swarm")
	if m, err = manifest.Load(mpath); err == nil && m.PieceSize() == int(ps) {
		return src, mpath, m, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", nil, err
	}
	if err := dataset.WriteFile(src, size, sc.DataSeed); err != nil {
		return "", "", nil, err
	}
	if m, err = manifest.BuildFile(src, int(ps)); err != nil {
		return "", "", nil, err
	}
	return src, mpath, m, m.Save(mpath)
}

// seedFile returns a file for a seed with the given Has spec: selected pieces
// kept (others zeroed), or with "corrupt:N%" every piece kept but N% of them
// corrupted by one flipped byte.
func seedFile(src string, m *manifest.Manifest, has string) (string, error) {
	if has == "" || has == "all" {
		return src, nil
	}
	path := src + "." + strings.NewReplacer(":", "-", "%", "pct").Replace(has)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	// Patch only the affected pieces of a streamed copy: the lab never holds the
	// whole file in memory (its RSS would otherwise be counted in children's rusage).
	var patch func(f *os.File) error
	if pct, ok := strings.CutPrefix(has, "corrupt:"); ok {
		var p float64
		if _, err := fmt.Sscanf(pct, "%g%%", &p); err != nil || p <= 0 || p > 100 {
			return "", fmt.Errorf("bad has %q", has)
		}
		every := max(int(100/p+0.5), 1)
		patch = func(f *os.File) error {
			b := make([]byte, 1)
			for i := 0; i < m.NumPieces(); i += every {
				off := m.PieceOffset(i) + int64(m.PieceLen(i)/2)
				if _, err := f.ReadAt(b, off); err != nil {
					return err
				}
				b[0] ^= 0xFF
				if _, err := f.WriteAt(b, off); err != nil {
					return err
				}
			}
			return nil
		}
	} else {
		keep, err := pieceFilter(has, m.NumPieces())
		if err != nil {
			return "", err
		}
		patch = func(f *os.File) error {
			zeros := make([]byte, m.PieceSize())
			for i := range m.NumPieces() {
				if !keep(i) {
					if _, err := f.WriteAt(zeros[:m.PieceLen(i)], m.PieceOffset(i)); err != nil {
						return err
					}
				}
			}
			return nil
		}
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return "", err
	}
	if err := patch(out); err != nil {
		out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

func pieceFilter(has string, n int) (func(int) bool, error) {
	switch {
	case has == "even":
		return func(i int) bool { return i%2 == 0 }, nil
	case has == "odd":
		return func(i int) bool { return i%2 == 1 }, nil
	case strings.HasPrefix(has, "first:") || strings.HasPrefix(has, "last:"):
		var pct float64
		if _, err := fmt.Sscanf(has[strings.Index(has, ":")+1:], "%g%%", &pct); err != nil {
			return nil, fmt.Errorf("bad has %q", has)
		}
		k := int(pct / 100 * float64(n))
		if strings.HasPrefix(has, "first:") {
			return func(i int) bool { return i < k }, nil
		}
		return func(i int) bool { return i >= n-k }, nil
	}
	return nil, fmt.Errorf("unknown has %q (all, even, odd, first:N%%, last:N%%, corrupt:N%%)", has)
}

func (o Options) logf(format string, args ...any) {
	if o.Log != nil {
		fmt.Fprintf(o.Log, format+"\n", args...)
	}
}

// RunScenario runs every variant for every trial. Trials are interleaved
// (trial 1 of each variant, then trial 2, ...) so slow drift on the host does
// not favour one variant.
func RunScenario(ctx context.Context, sc *Scenario, opt Options) (*ScenarioResult, error) {
	src, mpath, m, err := prepareData(sc, opt.Work)
	if err != nil {
		return nil, fmt.Errorf("preparing data: %w", err)
	}
	res := &ScenarioResult{Scenario: sc, Started: time.Now()}
	for trial := 1; trial <= opt.Trials; trial++ {
		for _, v := range sc.Variants {
			if len(opt.Only) > 0 && !slices.Contains(opt.Only, v.Name) {
				continue
			}
			if ctx.Err() != nil {
				return res, ctx.Err()
			}
			opt.logf("[%s] variant %s trial %d/%d", sc.Name, v.Name, trial, opt.Trials)
			tr := runTrial(ctx, sc, v, trial, src, mpath, m, opt)
			if tr.Error != "" {
				opt.logf("  error: %s", tr.Error)
			}
			for _, p := range tr.Peers {
				if p.Role == "get" {
					opt.logf("  %-10s %-8s completion %6.2fs  %6.2f MiB/s  timeouts %d  rejected %d", p.Name, p.Exit,
						p.CompletionS, p.ThroughputMiBps, p.Timeouts, p.Rejected)
				}
			}
			for _, o := range tr.Observations {
				opt.logf("  %s %s→%s %.3fs", o.Kind, o.Peer, o.Target, o.Seconds)
			}
			res.Trials = append(res.Trials, tr)
		}
	}
	res.Finished = time.Now()
	return res, nil
}

type proc struct {
	idx     int
	spec    PeerSpec
	dir     string
	restart func(after time.Time) bool // reports whether a start event for this peer is still due

	mu         sync.Mutex
	cmd        *exec.Cmd
	runDone    chan struct{}
	finished   chan struct{} // closed once the peer is done for this trial
	finishOnce sync.Once
	runs       int
	firstStart time.Time
	exitHow    string // set by events: killed, stopped
	waitErr    error
	cpu, rss   float64
}

func (p *proc) finish() { p.finishOnce.Do(func() { close(p.finished) }) }

func (p *proc) running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cmd != nil && p.runDone != nil && !isClosed(p.runDone)
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func runTrial(ctx context.Context, sc *Scenario, v Variant, trial int, src, mpath string, m *manifest.Manifest, opt Options) (tr TrialResult) {
	tr = TrialResult{Variant: v.Name, Trial: trial}
	start := time.Now()
	defer func() { tr.WallS = time.Since(start).Seconds() }()
	fail := func(err error) TrialResult { tr.Error = err.Error(); return tr }

	if err := cleanupStale(); err != nil {
		return fail(err)
	}
	if err := setupHub(); err != nil {
		return fail(err)
	}
	defer teardown(len(sc.Peers))
	ipName := map[string]string{}
	for i, p := range sc.Peers {
		if err := addPeer(i+1, p.Link); err != nil {
			return fail(err)
		}
		ipName[peerIP(i+1)] = p.Name
	}
	trialDir := filepath.Join(opt.Work, "runs", sc.Name, v.Name, fmt.Sprint(trial))
	os.RemoveAll(trialDir)
	defer os.RemoveAll(trialDir)

	tracker := exec.Command("ip", "netns", "exec", hubNS, filepath.Join(opt.BinDir, "tracker"),
		"-listen", trackerIP+":7000", "-interval", "2s", "-ttl", "10s")
	if err := tracker.Start(); err != nil {
		return fail(fmt.Errorf("starting tracker: %w", err))
	}
	defer func() { tracker.Process.Kill(); tracker.Wait() }()

	t0 := time.Now()
	procs := make([]*proc, len(sc.Peers))
	for i, p := range sc.Peers {
		procs[i] = &proc{idx: i + 1, spec: p, dir: filepath.Join(trialDir, p.Name), finished: make(chan struct{})}
		os.MkdirAll(procs[i].dir, 0o755)
		name := p.Name
		procs[i].restart = func(after time.Time) bool {
			for _, e := range sc.Events {
				if e.Peer == name && e.Action == "start" && t0.Add(e.At.Duration).After(after) {
					return true
				}
			}
			return false
		}
	}
	runCtx, cancel := context.WithTimeout(ctx, sc.Timeout.Duration)
	defer cancel()

	var wg sync.WaitGroup
	var appliedMu sync.Mutex
	startErr := make(chan error, len(procs)*2)
	for _, p := range procs {
		if p.spec.Manual {
			continue
		}
		wg.Go(func() {
			if sleepUntil(runCtx, t0.Add(p.spec.StartAt.Duration)) {
				if err := p.start(sc, v, trial, src, mpath, m, opt); err != nil {
					startErr <- err
				}
			}
		})
	}
	for _, e := range sc.Events {
		wg.Go(func() {
			if !sleepUntil(runCtx, t0.Add(e.At.Duration)) {
				return
			}
			i := slices.IndexFunc(procs, func(p *proc) bool { return p.spec.Name == e.Peer })
			at := time.Now()
			var err error
			if e.Action == "start" {
				err = procs[i].start(sc, v, trial, src, mpath, m, opt)
			} else {
				err = procs[i].apply(e)
			}
			if err != nil {
				opt.logf("  event %s %s failed: %v", e.Action, e.Peer, err)
				return
			}
			appliedMu.Lock()
			tr.Applied = append(tr.Applied, AppliedEvent{Peer: e.Peer, Action: e.Action, At: at})
			appliedMu.Unlock()
		})
	}

	// Wait for every downloader to finish (or the trial deadline), then stop the rest.
	for _, p := range procs {
		if p.spec.Role != "get" {
			continue
		}
		select {
		case <-p.finished:
		case <-runCtx.Done():
		}
	}
	// Decide before cancelling below, which would make runCtx.Err() non-nil regardless.
	timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded)
	cancel()
	wg.Wait() // no further starts or events after this point
	for _, p := range procs {
		p.signal(syscall.SIGINT, "")
	}
	grace := time.After(5 * time.Second)
	for _, p := range procs {
		p.mu.Lock()
		done := p.runDone
		p.mu.Unlock()
		if done == nil {
			continue
		}
		select {
		case <-done:
		case <-grace:
			p.signal(syscall.SIGKILL, "timeout")
			<-done
		}
	}
	select {
	case err := <-startErr:
		return fail(err)
	default:
	}

	idName := map[string]string{}
	allEvents := map[string][]events.Event{}
	for _, p := range procs {
		tr.Peers = append(tr.Peers, p.result(m, ipName, timedOut))
		if evs := readEvents(filepath.Join(p.dir, "events.json")); evs != nil {
			allEvents[p.spec.Name] = evs
			for _, e := range evs {
				if e.Type == events.PeerConnected {
					id, _ := e.Fields["remote"].(string)
					addr, _ := e.Fields["addr"].(string)
					if ap, err := netip.ParseAddrPort(addr); err == nil && id != "" {
						if n, ok := ipName[ap.Addr().String()]; ok {
							idName[id] = n
						}
					}
				}
			}
		}
	}
	slices.SortFunc(tr.Applied, func(a, b AppliedEvent) int { return a.At.Compare(b.At) })
	tr.Observations = observe(tr.Applied, allEvents, idName)
	for _, r := range tr.Peers {
		if r.Resume != nil {
			tr.Observations = append(tr.Observations, Observation{Kind: "resume_overhead", Peer: r.Name, Target: r.Name,
				Seconds: float64(r.Resume.OverheadMs) / 1000})
		}
		if r.WaitS > 0.5 {
			tr.Observations = append(tr.Observations, Observation{Kind: "first_piece_wait", Peer: r.Name, Target: "-", Seconds: r.WaitS})
		}
	}
	return tr
}

func readEvents(path string) []events.Event {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var evs []events.Event
	if json.Unmarshal(b, &evs) != nil {
		return nil
	}
	return evs
}

// observe derives durations from what peers recorded after each applied failure:
//   - detect_failure: from the failure to an observer's first disconnect from,
//     or request timeout to, the failed peer;
//   - re_replicate: from the failure to a replicator's next "replication target
//     reached" after it reported under-replication.
func observe(applied []AppliedEvent, all map[string][]events.Event, idName map[string]string) []Observation {
	var out []Observation
	for _, a := range applied {
		if a.Action != "kill" && a.Action != "stop" && a.Action != "link-down" {
			continue
		}
		for observer, evs := range all {
			if observer == a.Peer {
				continue
			}
			for _, e := range evs {
				if e.Time.Before(a.At) {
					continue
				}
				remote, _ := e.Fields["remote"].(string)
				if (e.Type == events.PeerDisconnected || e.Type == events.RequestTimeout) && idName[remote] == a.Peer {
					out = append(out, Observation{Kind: "detect_failure", Peer: observer, Target: a.Peer, Seconds: e.Time.Sub(a.At).Seconds()})
					break
				}
			}
			under := -1 // index of the first under-replication report after the failure
			for i, e := range evs {
				if !e.Time.Before(a.At) && e.Type == events.UnderReplicated {
					under = i
					break
				}
			}
			if under >= 0 {
				for _, e := range evs[under:] {
					if e.Type == events.ReplicationTarget {
						out = append(out, Observation{Kind: "re_replicate", Peer: observer, Target: a.Peer, Seconds: e.Time.Sub(a.At).Seconds()})
						break
					}
				}
			}
		}
	}
	slices.SortFunc(out, func(x, y Observation) int {
		return strings.Compare(x.Kind+x.Peer+x.Target, y.Kind+y.Peer+y.Target)
	})
	return out
}

func sleepUntil(ctx context.Context, t time.Time) bool {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// start launches a run of the peer unless one is already running. Later runs
// reuse the same directory, so a restarted downloader resumes from its state DB.
func (p *proc) start(sc *Scenario, v Variant, trial int, src, mpath string, m *manifest.Manifest, opt Options) error {
	if p.running() {
		return nil
	}
	args := []string{"netns", "exec", peerNS(p.idx), filepath.Join(opt.BinDir, "swarmflow"), p.spec.Role,
		"-manifest", mpath, "-listen", peerIP(p.idx) + ":7001", "-tracker", "http://" + trackerIP + ":7000",
		"-stats-out", filepath.Join(p.dir, "stats.json"), "-events-out", filepath.Join(p.dir, "events.json"),
		"-log-json", "-progress", "0", "-seed", fmt.Sprint(trial*1000 + p.idx)}
	switch p.spec.Role {
	case "seed":
		f, err := seedFile(src, m, p.spec.Has)
		if err != nil {
			return err
		}
		args = append(args, "-file", f)
		if strings.HasPrefix(p.spec.Has, "corrupt:") {
			args = append(args, "-no-verify") // serve the damaged file as if complete
		}
	case "get", "replica":
		args = append(args, "-out", filepath.Join(p.dir, "payload.bin"), "-state-db", filepath.Join(p.dir, "state.db"))
		if p.spec.Role == "get" {
			args = append(args, "-exit-on-complete")
		}
	}
	args = append(args, v.argsFor(p.spec.Role)...)
	args = append(args, sc.CommonArgs...)
	args = append(args, p.spec.Args...)
	logf, err := os.OpenFile(filepath.Join(p.dir, "peer.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	cmd := exec.Command("ip", args...)
	cmd.Stderr, cmd.Stdout = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("starting %s: %w", p.spec.Name, err)
	}
	done := make(chan struct{})
	p.mu.Lock()
	p.cmd, p.runDone, p.exitHow, p.waitErr = cmd, done, "", nil
	p.runs++
	if p.firstStart.IsZero() {
		p.firstStart = time.Now()
	}
	p.mu.Unlock()
	go func() {
		err := cmd.Wait()
		logf.Close()
		p.mu.Lock()
		p.waitErr = err
		if st := cmd.ProcessState; st != nil {
			p.cpu += (st.UserTime() + st.SystemTime()).Seconds()
			p.rss = max(p.rss, maxRSSMiB(st))
		}
		how := p.exitHow
		p.mu.Unlock()
		close(done)
		// A downloader killed by an event is not done if the scenario restarts it later.
		if how == "" || !p.restart(time.Now()) {
			p.finish()
		}
	}()
	return nil
}

// signal sends sig if the process is running; how records the reason for the result.
func (p *proc) signal(sig syscall.Signal, how string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.runDone == nil || isClosed(p.runDone) {
		return
	}
	if how != "" && p.exitHow == "" {
		p.exitHow = how
	}
	p.cmd.Process.Signal(sig)
}

func (p *proc) apply(e Event) error {
	switch e.Action {
	case "kill":
		p.signal(syscall.SIGKILL, "killed")
	case "stop":
		p.signal(syscall.SIGINT, "stopped")
	case "link-down":
		return setLinkState(p.idx, false)
	case "link-up":
		return setLinkState(p.idx, true)
	case "set-link":
		return setLink(p.idx, *e.Link)
	}
	return nil
}

func (p *proc) result(m *manifest.Manifest, ipName map[string]string, timedOut bool) PeerResult {
	r := PeerResult{Name: p.spec.Name, Role: p.spec.Role}
	p.mu.Lock()
	cmd, how, runs, first, waitErr := p.cmd, p.exitHow, p.runs, p.firstStart, p.waitErr
	r.CPUSeconds, r.MaxRSSMiB = p.cpu, p.rss
	p.mu.Unlock()
	r.Runs = runs
	switch {
	case cmd == nil:
		r.Exit = "not-started"
		return r
	case how != "":
		r.Exit = how
	case waitErr != nil && p.spec.Role == "get" && !timedOut:
		r.Exit = "failed: " + waitErr.Error()
	case timedOut && p.spec.Role == "get":
		r.Exit = "timeout" // the trial deadline passed before this downloader exited
	default:
		r.Exit = "ok"
	}
	if runs > 1 {
		r.Exit += fmt.Sprintf(" (%d runs)", runs)
	}
	b, err := os.ReadFile(filepath.Join(p.dir, "stats.json"))
	if err != nil {
		return r // a killed final run writes no stats
	}
	var s coordinator.Status
	if err := json.Unmarshal(b, &s); err != nil {
		r.Exit += " (bad stats: " + err.Error() + ")"
		return r
	}
	r.BytesDown, r.BytesUp = s.BytesDown, s.BytesUp
	r.Requests, r.Timeouts, r.Rejected = s.Counters.Requests, s.Counters.Timeouts, s.Counters.Rejected
	r.Reassigned, r.Banned = s.Counters.Reassigned, s.Counters.Banned
	r.EndgameRequests, r.BusyReceived = s.Counters.EndgameRequests, s.Counters.BusyReceived
	r.Resume = s.Resume
	if s.Process != nil && s.Process.PeakRSSMiB > 0 {
		// The process's own peak; rusage from Wait can include the lab's memory
		// shared before exec (Go starts children with a vfork-style clone).
		r.MaxRSSMiB = s.Process.PeakRSSMiB
	}
	if s.Replication.Target > 1 || s.Replication.Accepting {
		repl := s.Replication
		r.Replication = &repl
	}
	if !s.FirstPieceAt.IsZero() {
		r.WaitS = s.FirstPieceAt.Sub(s.StartedAt).Seconds()
	}
	if p.spec.Role == "get" && s.Complete && !s.FinishedAt.IsZero() {
		r.Completed = true
		r.CompletionS = s.FinishedAt.Sub(s.StartedAt).Seconds()
		r.FromFirstStartS = s.FinishedAt.Sub(first).Seconds()
		r.ThroughputMiBps = float64(m.Size()) / (1 << 20) / r.FromFirstStartS
	}
	r.Sources = map[string]int64{}
	for _, src := range s.Sources {
		name := src.Addr
		if ap, err := netip.ParseAddrPort(src.Addr); err == nil {
			if n, ok := ipName[ap.Addr().String()]; ok {
				name = n
			}
		}
		r.Sources[name] += src.Bytes
	}
	return r
}

var errNotLinux = errors.New("the netem lab needs Linux (run it inside WSL2) and root")
