// Command swarmflow is a SwarmFlow peer and its helper tools.
//
//	swarmflow gendata       -out FILE -size 64MiB [-seed N]
//	swarmflow make-manifest -in FILE [-out FILE.swarm] [-piece-size 256KiB]
//	swarmflow seed          -manifest FILE.swarm -file FILE [peer flags]
//	swarmflow get           -manifest FILE.swarm [-out PATH] [peer flags]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	"github.com/pgatdesk/swarmflow/internal/store"
)

const usage = `usage: swarmflow <command> [flags]

commands:
  gendata        write a reproducible synthetic test file
  make-manifest  hash a file into a .swarm manifest
  seed           serve a file (complete or partial) to the swarm
  get            download a file, then keep seeding unless -exit-on-complete

run "swarmflow <command> -h" for the flags of a command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch cmd, args := os.Args[1], os.Args[2:]; cmd {
	case "gendata":
		err = cmdGenData(args)
	case "make-manifest":
		err = cmdMakeManifest(args)
	case "seed":
		err = cmdPeer("seed", args)
	case "get":
		err = cmdPeer("get", args)
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdGenData(args []string) error {
	fs := flag.NewFlagSet("gendata", flag.ExitOnError)
	out := fs.String("out", "", "output file (required)")
	size := fs.String("size", "64MiB", "file size, e.g. 4096, 512KiB, 64MiB, 1GiB")
	seed := fs.Uint64("seed", 1, "generator seed; same size and seed give identical bytes")
	fs.Parse(args)
	if *out == "" {
		return errors.New("-out is required")
	}
	n, err := dataset.ParseSize(*size)
	if err != nil {
		return err
	}
	return dataset.WriteFile(*out, n, *seed)
}

func cmdMakeManifest(args []string) error {
	fs := flag.NewFlagSet("make-manifest", flag.ExitOnError)
	in := fs.String("in", "", "file to describe (required)")
	out := fs.String("out", "", "manifest path (default: <in>.swarm)")
	ps := fs.String("piece-size", "256KiB", "piece size, a power of two between 4KiB and 1MiB")
	fs.Parse(args)
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
	fmt.Printf("wrote %s\n  name:   %s\n  size:   %d bytes\n  pieces: %d x %d bytes\n  swarm:  %s\n",
		*out, m.Name(), m.Size(), m.NumPieces(), m.PieceSize(), m.ID())
	return nil
}

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func cmdPeer(mode string, args []string) error {
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	manifestPath := fs.String("manifest", "", "path to the .swarm manifest (required)")
	file := fs.String("file", "", "seed: the data file to serve (required for seed)")
	out := fs.String("out", "", "get: output path (default: manifest name in the current directory)")
	listen := fs.String("listen", "127.0.0.1:0", "TCP listen address; use 0.0.0.0:PORT to accept other machines")
	tracker := fs.String("tracker", "", "bootstrap tracker URL, e.g. http://127.0.0.1:7000")
	var peers multiFlag
	fs.Var(&peers, "peer", "static peer host:port to connect to (repeatable)")
	advIP := fs.String("advertise-ip", "", "IP to announce to the tracker instead of the observed one")
	advPort := fs.Uint("advertise-port", 0, "port to announce instead of the listening port")
	maxOut := fs.Int("max-outstanding", 32, "max requests in flight plus pieces being verified")
	maxPerPeer := fs.Int("max-per-peer", 4, "max requests in flight to one peer")
	uploadSlots := fs.Int("upload-slots", 4, "max queued uploads per remote peer")
	reqTimeout := fs.Duration("request-timeout", 10*time.Second, "deadline for one piece request")
	maxPeers := fs.Int("max-peers", 50, "max connected peers")
	picker := fs.String("picker", "sequential", "piece picker: sequential, random")
	selector := fs.String("selector", "random", "peer selector: random")
	seed := fs.Uint64("seed", 1, "seed for randomized policies")
	exitOnComplete := fs.Bool("exit-on-complete", false, "get: exit once the download is complete")
	progress := fs.Duration("progress", 2*time.Second, "progress report interval (0 disables)")
	logLevel := fs.String("log-level", "info", "debug, info, warn, error")
	logJSON := fs.Bool("log-json", false, "structured JSON logs")
	fs.Parse(args)

	log, err := newLogger(*logLevel, *logJSON)
	if err != nil {
		return err
	}
	if *manifestPath == "" {
		return errors.New("-manifest is required")
	}
	m, err := manifest.Load(*manifestPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	st, have, err := openStore(ctx, mode, m, *file, *out, log)
	if err != nil {
		return err
	}
	defer st.Close()

	ccfg := coordinator.DefaultConfig()
	ccfg.MaxOutstanding, ccfg.MaxPerPeer, ccfg.UploadSlots = *maxOut, *maxPerPeer, *uploadSlots
	ccfg.RequestTimeout, ccfg.MaxPeers = *reqTimeout, *maxPeers
	if ccfg.Picker, err = scheduler.NewPicker(*picker, *seed); err != nil {
		return err
	}
	if ccfg.Selector, err = scheduler.NewSelector(*selector, *seed); err != nil {
		return err
	}

	ncfg := node.DefaultConfig()
	ncfg.ListenAddr, ncfg.TrackerURL, ncfg.StaticPeers = *listen, *tracker, peers
	ncfg.AdvertiseIP, ncfg.AdvertisePort, ncfg.MaxPeers = *advIP, uint16(*advPort), *maxPeers
	n, err := node.New(ncfg, log)
	if err != nil {
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
		return nil
	default:
		s := co.Status()
		return fmt.Errorf("interrupted with %d/%d pieces", s.Have, s.Pieces)
	}
}

func openStore(ctx context.Context, mode string, m *manifest.Manifest, file, out string, log *slog.Logger) (*store.Store, bitfield.Bitfield, error) {
	workers := max(runtime.NumCPU()/2, 1)
	var st *store.Store
	recheck := true
	switch mode {
	case "seed":
		if file == "" {
			return nil, bitfield.Bitfield{}, errors.New("-file is required for seed")
		}
		var err error
		if st, err = store.OpenComplete(file, m); err != nil {
			return nil, bitfield.Bitfield{}, err
		}
	case "get":
		if out == "" {
			out = m.Name()
		}
		if fi, err := os.Stat(out); err == nil && fi.Size() == m.Size() {
			// A finished download from an earlier run: verify it and seed it.
			s, err := store.OpenComplete(out, m)
			if err != nil {
				return nil, bitfield.Bitfield{}, err
			}
			st = s
			break
		}
		if dir := filepath.Dir(out); dir != "" {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, bitfield.Bitfield{}, err
			}
		}
		s, existed, err := store.Create(out+".part", out, m)
		if err != nil {
			return nil, bitfield.Bitfield{}, err
		}
		st, recheck = s, existed
	}
	if !recheck {
		return st, bitfield.New(m.NumPieces()), nil
	}
	start := time.Now()
	have, err := st.Recheck(ctx, workers)
	if err != nil {
		st.Close()
		return nil, bitfield.Bitfield{}, fmt.Errorf("checking %s: %w", st.Path(), err)
	}
	log.Info("verified local data", "path", st.Path(), "valid_pieces", have.Count(), "pieces", m.NumPieces(),
		"took", time.Since(start).Round(time.Millisecond))
	if mode == "seed" && !have.Complete() {
		log.Warn("seeding a partial file", "valid_pieces", have.Count(), "pieces", m.NumPieces())
	}
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

func newLogger(level string, asJSON bool) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("bad -log-level: %w", err)
	}
	opts := &slog.HandlerOptions{Level: l}
	if asJSON {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
}
