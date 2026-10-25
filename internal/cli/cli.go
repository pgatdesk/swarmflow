// Package cli implements the swarmflow command. It lives outside cmd/ so that
// tests can run the real command, including as a separate OS process.
package cli

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/bitfield"
	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/node"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
	"github.com/pgatdesk/swarmflow/internal/statedb"
	"github.com/pgatdesk/swarmflow/internal/store"
)

const usage = `usage: swarmflow <command> [flags]

commands:
  gendata        write a reproducible synthetic test file
  make-manifest  hash a file into a .swarm manifest
  seed           serve a file (complete or partial) to the swarm
  get            download a file (resuming if interrupted), then keep seeding unless -exit-on-complete

run "swarmflow <command> -h" for the flags of a command.
`

// Main runs the command with args (excluding the program name) and returns the exit code.
func Main(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch cmd, rest := args[0], args[1:]; cmd {
	case "gendata":
		err = cmdGenData(rest, stderr)
	case "make-manifest":
		err = cmdMakeManifest(rest, stdout, stderr)
	case "seed", "get":
		err = cmdPeer(cmd, rest, stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func cmdGenData(args []string, stderr io.Writer) error {
	fs := newFlagSet("gendata", stderr)
	out := fs.String("out", "", "output file (required)")
	size := fs.String("size", "64MiB", "file size, e.g. 4096, 512KiB, 64MiB, 1GiB")
	seed := fs.Uint64("seed", 1, "generator seed; same size and seed give identical bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("-out is required")
	}
	n, err := dataset.ParseSize(*size)
	if err != nil {
		return err
	}
	return dataset.WriteFile(*out, n, *seed)
}

func cmdMakeManifest(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("make-manifest", stderr)
	in := fs.String("in", "", "file to describe (required)")
	out := fs.String("out", "", "manifest path (default: <in>.swarm)")
	ps := fs.String("piece-size", "256KiB", "piece size, a power of two between 4KiB and 1MiB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *in == "" {
		return errors.New("-in is required")
	}
	pieceSize, err := dataset.ParseSize(*ps)
	if err != nil {
		return err
	}
	m, err := manifest.BuildFile(*in, int(pieceSize))
	if err != nil {
		return err
	}
	if *out == "" {
		*out = *in + ".swarm"
	}
	if err := m.Save(*out); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %s\n  name:   %s\n  size:   %d bytes\n  pieces: %d x %d bytes\n  swarm:  %s\n",
		*out, m.Name(), m.Size(), m.NumPieces(), m.PieceSize(), m.ID())
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdPeer(mode string, args []string, stderr io.Writer) error {
	fs := newFlagSet(mode, stderr)
	manifestPath := fs.String("manifest", "", "path to the .swarm manifest (required)")
	file := fs.String("file", "", "seed: the data file to serve (required for seed)")
	out := fs.String("out", "", "get: output path (default: manifest name in the current directory)")
	stateDB := fs.String("state-db", "", "get: transfer state database (default: <out dir>/.swarmflow/state.db)")
	resumeVerify := fs.String("resume-verify", "full", "get: on restart, re-hash persisted pieces (full) or trust the state database (trust)")
	listen := fs.String("listen", "127.0.0.1:0", "TCP listen address; use 0.0.0.0:PORT to accept other machines")
	tracker := fs.String("tracker", "", "bootstrap tracker URL, e.g. http://127.0.0.1:7000")
	var peers multiFlag
	fs.Var(&peers, "peer", "static peer host:port to connect to (repeatable)")
	advIP := fs.String("advertise-ip", "", "IP to announce to the tracker instead of the observed one")
	advPort := fs.Uint("advertise-port", 0, "port to announce instead of the listening port")
	maxOut := fs.Int("max-outstanding", 32, "max requests in flight plus pieces being verified")
	maxPerPeer := fs.Int("max-per-peer", 4, "max requests in flight to one peer")
	uploadSlots := fs.Int("upload-slots", 4, "max queued uploads per remote peer")
	uploadQueue := fs.Int("upload-queue", 16, "max queued uploads across all peers before replying busy (0: unlimited)")
	reqTimeout := fs.Duration("request-timeout", 10*time.Second, "deadline for requests to peers without latency history")
	maxPeers := fs.Int("max-peers", 50, "max connected peers")
	picker := fs.String("picker", "rarest", "piece picker: "+strings.Join(scheduler.Pickers, ", "))
	selector := fs.String("selector", "adaptive", "peer selector: "+strings.Join(scheduler.Selectors, ", "))
	endgame := fs.Bool("endgame", true, "issue duplicate requests for overdue pieces near completion")
	seed := fs.Uint64("seed", 0, "seed for randomized policies (0: derive from the random peer ID)")
	exitOnComplete := fs.Bool("exit-on-complete", false, "get: exit once the download is complete")
	progress := fs.Duration("progress", 2*time.Second, "progress report interval (0 disables)")
	logLevel := fs.String("log-level", "info", "debug, info, warn, error")
	logJSON := fs.Bool("log-json", false, "structured JSON logs")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log, err := newLogger(stderr, *logLevel, *logJSON)
	if err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("-manifest is required")
	}
	if *resumeVerify != "full" && *resumeVerify != "trust" {
		return fmt.Errorf("-resume-verify must be full or trust, not %q", *resumeVerify)
	}
	m, err := manifest.Load(*manifestPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var (
		st      *store.Store
		have    bitfield.Bitfield
		journal coordinator.Journal
	)
	switch mode {
	case "seed":
		if st, have, err = openSeed(ctx, m, *file, log); err != nil {
			return err
		}
	case "get":
		if *out == "" {
			*out = m.Name()
		}
		if *stateDB == "" {
			*stateDB = filepath.Join(filepath.Dir(*out), ".swarmflow", "state.db")
		}
		db, err := statedb.Open(*stateDB)
		if err != nil {
			return err
		}
		defer db.Close()
		if st, have, err = OpenDownload(ctx, db, m, *out, *resumeVerify == "full", log); err != nil {
			return err
		}
		journal = dbJournal{db: db, m: m}
	}
	defer st.Close()

	ncfg := node.DefaultConfig()
	ncfg.ListenAddr, ncfg.TrackerURL, ncfg.StaticPeers = *listen, *tracker, peers
	ncfg.AdvertiseIP, ncfg.AdvertisePort, ncfg.MaxPeers = *advIP, uint16(*advPort), *maxPeers
	n, err := node.New(ncfg, log)
	if err != nil {
		return err
	}

	ccfg := coordinator.DefaultConfig()
	ccfg.MaxOutstanding, ccfg.MaxPerPeer, ccfg.UploadSlots = *maxOut, *maxPerPeer, *uploadSlots
	ccfg.UploadQueueCap, ccfg.Journal = *uploadQueue, journal
	ccfg.RequestTimeout, ccfg.MaxPeers, ccfg.Endgame.Enabled = *reqTimeout, *maxPeers, *endgame
	if *seed == 0 {
		// Distinct seeds per peer keep downloaders from breaking rarest-first
		// ties identically and all fetching the same piece.
		id := n.ID()
		*seed = binary.BigEndian.Uint64(id[:8])
	}
	if ccfg.Picker, err = scheduler.NewPicker(*picker, *seed); err != nil {
		return err
	}
	if ccfg.Selector, err = scheduler.NewSelector(*selector, *seed); err != nil {
		return err
	}
	co := coordinator.New(m, st, have, n.ID(), ccfg, n.Logger())
	n.AddSwarm(co)
	if *tracker == "" && len(peers) == 0 {
		log.Warn("no -tracker and no -peer given; waiting for inbound connections only")
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	wg.Go(func() { co.Run(runCtx) })
	wg.Go(func() { n.Run(runCtx) })
	if *progress > 0 {
		wg.Go(func() { reportProgress(runCtx, co, *progress, log) })
	}

	select {
	case <-co.Complete():
		if mode == "get" && *exitOnComplete {
			cancel()
		}
	case <-ctx.Done():
	}
	<-runCtx.Done()
	cancel()
	wg.Wait()

	select {
	case <-co.Complete():
		s := co.Status()
		log.Info("exiting", "bytes_down", s.BytesDown, "bytes_up", s.BytesUp, "requests", s.Counters.Requests)
		return nil
	default:
		s := co.Status()
		return fmt.Errorf("interrupted with %d/%d pieces", s.Have, s.Pieces)
	}
}

// dbJournal records coordinator progress in the state database.
type dbJournal struct {
	db *statedb.DB
	m  *manifest.Manifest
}

func (j dbJournal) PiecePersisted(i int) error { return j.db.MarkPersisted(j.m.ID(), i, j.m.Hash(i)) }
func (j dbJournal) Completed() error           { return j.db.SetStatus(j.m.ID(), statedb.StatusComplete) }

func verifyWorkers() int { return max(runtime.NumCPU()/2, 1) }

func openSeed(ctx context.Context, m *manifest.Manifest, file string, log *slog.Logger) (*store.Store, bitfield.Bitfield, error) {
	if file == "" {
		return nil, bitfield.Bitfield{}, errors.New("-file is required for seed")
	}
	st, err := store.OpenComplete(file, m)
	if err != nil {
		return nil, bitfield.Bitfield{}, err
	}
	start := time.Now()
	have, err := st.Recheck(ctx, verifyWorkers())
	if err != nil {
		st.Close()
		return nil, bitfield.Bitfield{}, fmt.Errorf("checking %s: %w", file, err)
	}
	log.Info("verified local data", "path", file, "valid_pieces", have.Count(), "pieces", m.NumPieces(),
		"took", time.Since(start).Round(time.Millisecond))
	if !have.Complete() {
		log.Warn("seeding a partial file", "valid_pieces", have.Count(), "pieces", m.NumPieces())
	}
	return st, have, nil
}

// OpenDownload prepares storage for a download into out, resuming from db:
//
//   - A complete file at out is verified and seeded.
//   - Otherwise out+".part" is opened or created. Pieces recorded as persisted
//     are re-hashed (verifyResumed) or trusted; any that fail are unrecorded and
//     fetched again. Data on disk that was never recorded is ignored, because a
//     crash may have interrupted its write.
//   - A .part file with no database record (for example from a version without
//     a state database) is fully re-hashed and adopted.
func OpenDownload(ctx context.Context, db *statedb.DB, m *manifest.Manifest, out string, verifyResumed bool, log *slog.Logger) (*store.Store, bitfield.Bitfield, error) {
	id := m.ID()
	none := bitfield.Bitfield{}
	if fi, err := os.Stat(out); err == nil && fi.Size() == m.Size() {
		st, err := store.OpenComplete(out, m)
		if err != nil {
			return nil, none, err
		}
		start := time.Now()
		have, err := st.Recheck(ctx, verifyWorkers())
		if err != nil || !have.Complete() {
			st.Close()
			if err == nil {
				err = fmt.Errorf("%d/%d pieces match the manifest; move it away to download again", have.Count(), m.NumPieces())
			}
			return nil, none, fmt.Errorf("existing file %s: %w", out, err)
		}
		log.Info("verified existing download; seeding", "path", out, "took", time.Since(start).Round(time.Millisecond))
		return st, have, nil
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return nil, none, err
	}
	rec, found, err := db.Swarm(id)
	if err != nil {
		return nil, none, err
	}
	st, existed, err := store.Create(out+".part", out, m)
	if err != nil {
		return nil, none, err
	}
	fail := func(err error) (*store.Store, bitfield.Bitfield, error) {
		st.Close()
		return nil, none, err
	}
	if err := db.UpsertSwarm(statedb.Swarm{ID: id, Name: m.Name(), Size: m.Size(), PieceSize: m.PieceSize(),
		Pieces: m.NumPieces(), Manifest: m.Canonical(), PartPath: out + ".part", FinalPath: out,
		Status: statedb.StatusDownloading}); err != nil {
		return fail(err)
	}

	start := time.Now()
	have := bitfield.New(m.NumPieces())
	switch {
	case !existed:
		if found {
			if err := db.ClearPieces(id); err != nil { // the data file is gone; its records are stale
				return fail(err)
			}
		}
		log.Info("starting new download", "path", out, "pieces", m.NumPieces())
		return st, have, nil
	case !found:
		if have, err = st.Recheck(ctx, verifyWorkers()); err != nil {
			return fail(err)
		}
		for i := range m.NumPieces() {
			if have.Has(i) {
				if err := db.MarkPersisted(id, i, m.Hash(i)); err != nil {
					return fail(err)
				}
			}
		}
		log.Info("adopted existing partial file", "path", out+".part", "valid_pieces", have.Count(),
			"took", time.Since(start).Round(time.Millisecond))
		return st, have, nil
	}

	recs, err := db.Persisted(id)
	if err != nil {
		return fail(err)
	}
	var idxs []int
	var bad []int
	for _, r := range recs {
		if r.Index >= m.NumPieces() || r.Hash != m.Hash(r.Index) {
			bad = append(bad, r.Index)
			continue
		}
		idxs = append(idxs, r.Index)
	}
	if verifyResumed {
		valid, err := st.Verify(ctx, verifyWorkers(), idxs)
		if err != nil {
			return fail(err)
		}
		for _, i := range idxs {
			if valid.Has(i) {
				have.Set(i)
			} else {
				bad = append(bad, i)
			}
		}
	} else {
		for _, i := range idxs {
			have.Set(i)
		}
	}
	if err := db.Unmark(id, bad); err != nil {
		return fail(err)
	}
	log.Info("resumed download", "path", out+".part", "resumed_pieces", have.Count(), "pieces", m.NumPieces(),
		"invalid_on_resume", len(bad), "verify", map[bool]string{true: "full", false: "trust"}[verifyResumed],
		"resume_overhead_ms", time.Since(start).Milliseconds(), "previously_created", rec.Created.Format(time.RFC3339))
	return st, have, nil
}

func reportProgress(ctx context.Context, co *coordinator.Coordinator, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	var lastDown, lastUp int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		s := co.Status()
		secs := every.Seconds()
		log.Info("progress",
			"pct", fmt.Sprintf("%.1f", 100*float64(s.Have)/float64(s.Pieces)),
			"pieces", fmt.Sprintf("%d/%d", s.Have, s.Pieces),
			"peers", len(s.Peers),
			"outstanding", s.Outstanding,
			"down_MiBps", fmt.Sprintf("%.2f", float64(s.BytesDown-lastDown)/secs/(1<<20)),
			"up_MiBps", fmt.Sprintf("%.2f", float64(s.BytesUp-lastUp)/secs/(1<<20)))
		lastDown, lastUp = s.BytesDown, s.BytesUp
	}
}

func newLogger(w io.Writer, level string, asJSON bool) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("bad -log-level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: l}
	if asJSON {
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	}
	return slog.New(slog.NewTextHandler(w, opts)), nil
}
