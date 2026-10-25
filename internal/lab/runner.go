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
	"github.com/pgatdesk/swarmflow/internal/manifest"
)

type Options struct {
	BinDir string    // directory with linux swarmflow and tracker binaries
	Work   string    // scratch directory (fast local disk)
	Trials int       // trials per variant
	Only   []string  // variant names to run; empty = all
	Log    io.Writer // progress output
}

// PeerResult is one peer process's outcome in one trial.
type PeerResult struct {
	Name            string           `json:"name"`
	Role            string           `json:"role"`
	Exit            string           `json:"exit"` // ok, killed, stopped, timeout, failed: ...
	Completed       bool             `json:"completed"`
	CompletionS     float64          `json:"completion_s,omitempty"`
	ThroughputMiBps float64          `json:"throughput_mibps,omitempty"`
	BytesDown       int64            `json:"bytes_down"`
	BytesUp         int64            `json:"bytes_up"`
	Requests        int              `json:"requests"`
	Timeouts        int              `json:"timeouts"`
	Rejected        int              `json:"rejected_pieces"`
	EndgameRequests int              `json:"endgame_requests"`
	BusyReceived    int              `json:"busy_received"`
	Sources         map[string]int64 `json:"sources,omitempty"` // useful bytes received, by source peer name
	CPUSeconds      float64          `json:"cpu_s"`
	MaxRSSMiB       float64          `json:"max_rss_mib"`
}

type TrialResult struct {
	Variant string       `json:"variant"`
	Trial   int          `json:"trial"`
	WallS   float64      `json:"wall_s"`
	Error   string       `json:"error,omitempty"`
	Peers   []PeerResult `json:"peers"`
}

type proc struct {
	idx       int
	spec      PeerSpec
	dir       string
	cmd       *exec.Cmd
	done      chan struct{}
	waitErr   error
	exitHow   string // set by events: killed, stopped
	startedAt time.Time
	mu        sync.Mutex
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
	dir := filepath.Join(work, "data", fmt.Sprintf("%s-%d", strings.ReplaceAll(sc.FileSize, "/", "_"), sc.DataSeed))
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

// seedFile returns a file holding the pieces selected by has (others zeroed).
func seedFile(src string, m *manifest.Manifest, has string) (string, error) {
	if has == "" || has == "all" {
		return src, nil
	}
	keep, err := pieceFilter(has, m.NumPieces())
	if err != nil {
		return "", err
	}
	path := src + "." + strings.NewReplacer(":", "-", "%", "pct").Replace(has)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return "", err
	}
	for i := range m.NumPieces() {
		if !keep(i) {
			off := m.PieceOffset(i)
			clear(data[off : off+int64(m.PieceLen(i))])
		}
	}
	return path, os.WriteFile(path, data, 0o644)
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
	return nil, fmt.Errorf("unknown has %q (all, even, odd, first:N%%, last:N%%)", has)
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
					opt.logf("  %-10s %-8s completion %6.2fs  %6.2f MiB/s  timeouts %d", p.Name, p.Exit, p.CompletionS, p.ThroughputMiBps, p.Timeouts)
				}
			}
			res.Trials = append(res.Trials, tr)
		}
	}
	res.Finished = time.Now()
	return res, nil
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

	procs := make([]*proc, len(sc.Peers))
	for i, p := range sc.Peers {
		procs[i] = &proc{idx: i + 1, spec: p, dir: filepath.Join(trialDir, p.Name), done: make(chan struct{})}
		os.MkdirAll(procs[i].dir, 0o755)
	}
	runCtx, cancel := context.WithTimeout(ctx, sc.Timeout.Duration)
	defer cancel()

	var wg sync.WaitGroup
	t0 := time.Now()
	startErr := make(chan error, len(procs))
	for _, p := range procs {
		wg.Go(func() {
			if !sleepUntil(runCtx, t0.Add(p.spec.StartAt.Duration)) {
				close(p.done)
				return
			}
			if err := p.start(sc, v, trial, src, mpath, m, opt); err != nil {
				startErr <- err
				close(p.done)
				return
			}
			p.waitErr = p.cmd.Wait()
			close(p.done)
		})
	}
	for _, e := range sc.Events {
		wg.Go(func() {
			if !sleepUntil(runCtx, t0.Add(e.At.Duration)) {
				return
			}
			i := slices.IndexFunc(procs, func(p *proc) bool { return p.spec.Name == e.Peer })
			if err := procs[i].apply(e); err != nil {
				opt.logf("  event %s %s failed: %v", e.Action, e.Peer, err)
			}
		})
	}

	// Wait for every downloader, then stop the rest.
	for _, p := range procs {
		if p.spec.Role != "get" {
			continue
		}
		select {
		case <-p.done:
		case <-runCtx.Done():
		}
	}
	// Decide before cancelling below, which would make runCtx.Err() non-nil regardless.
	timedOut := errors.Is(runCtx.Err(), context.DeadlineExceeded)
	for _, p := range procs {
		p.signal(syscall.SIGINT, "")
	}
	grace := time.After(5 * time.Second)
	for _, p := range procs {
		select {
		case <-p.done:
		case <-grace:
			p.signal(syscall.SIGKILL, "timeout")
			<-p.done
		}
	}
	cancel()
	wg.Wait()
	select {
	case err := <-startErr:
		return fail(err)
	default:
	}

	for _, p := range procs {
		tr.Peers = append(tr.Peers, p.result(m, ipName, timedOut))
	}
	return tr
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

func (p *proc) start(sc *Scenario, v Variant, trial int, src, mpath string, m *manifest.Manifest, opt Options) error {
	args := []string{"netns", "exec", peerNS(p.idx), filepath.Join(opt.BinDir, "swarmflow"), p.spec.Role,
		"-manifest", mpath, "-listen", peerIP(p.idx) + ":7001", "-tracker", "http://" + trackerIP + ":7000",
		"-stats-out", filepath.Join(p.dir, "stats.json"), "-log-json", "-progress", "0",
		"-seed", fmt.Sprint(trial*1000 + p.idx)}
	switch p.spec.Role {
	case "seed":
		f, err := seedFile(src, m, p.spec.Has)
		if err != nil {
			return err
		}
		args = append(args, "-file", f)
	case "get", "replica":
		args = append(args, "-out", filepath.Join(p.dir, "payload.bin"), "-state-db", filepath.Join(p.dir, "state.db"))
		if p.spec.Role == "get" {
			args = append(args, "-exit-on-complete")
			args = append(args, v.Args...)
		}
	}
	args = append(args, sc.CommonArgs...)
	args = append(args, p.spec.Args...)
	logf, err := os.Create(filepath.Join(p.dir, "peer.log"))
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cmd = exec.Command("ip", args...)
	p.cmd.Stderr, p.cmd.Stdout = logf, logf
	p.startedAt = time.Now()
	if err := p.cmd.Start(); err != nil {
		logf.Close()
		return fmt.Errorf("starting %s: %w", p.spec.Name, err)
	}
	return nil
}

// signal sends sig if the process is running; how records the reason for the result.
func (p *proc) signal(sig syscall.Signal, how string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmd == nil || p.cmd.Process == nil || p.cmd.ProcessState != nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
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
	cmd, how := p.cmd, p.exitHow
	p.mu.Unlock()
	switch {
	case cmd == nil:
		r.Exit = "not-started"
		return r
	case how != "":
		r.Exit = how
	case p.waitErr != nil && p.spec.Role == "get" && !timedOut:
		r.Exit = "failed: " + p.waitErr.Error()
	case timedOut && p.spec.Role == "get":
		r.Exit = "timeout" // the trial deadline passed before this downloader exited
	default:
		r.Exit = "ok"
	}
	if st := cmd.ProcessState; st != nil {
		r.CPUSeconds = (st.UserTime() + st.SystemTime()).Seconds()
		r.MaxRSSMiB = maxRSSMiB(st)
	}
	b, err := os.ReadFile(filepath.Join(p.dir, "stats.json"))
	if err != nil {
		return r // killed processes write no stats
	}
	var s coordinator.Status
	if err := json.Unmarshal(b, &s); err != nil {
		r.Exit += " (bad stats: " + err.Error() + ")"
		return r
	}
	r.BytesDown, r.BytesUp = s.BytesDown, s.BytesUp
	r.Requests, r.Timeouts, r.Rejected = s.Counters.Requests, s.Counters.Timeouts, s.Counters.Rejected
	r.EndgameRequests, r.BusyReceived = s.Counters.EndgameRequests, s.Counters.BusyReceived
	if p.spec.Role == "get" && s.Complete && !s.FinishedAt.IsZero() {
		r.Completed = true
		r.CompletionS = s.FinishedAt.Sub(s.StartedAt).Seconds()
		r.ThroughputMiBps = float64(m.Size()) / (1 << 20) / r.CompletionS
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
