package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/dataset"
	"github.com/pgatdesk/swarmflow/internal/faultnet"
	"github.com/pgatdesk/swarmflow/internal/manifest"
	"github.com/pgatdesk/swarmflow/internal/node"
	"github.com/pgatdesk/swarmflow/internal/statedb"
	"github.com/pgatdesk/swarmflow/internal/store"
)

// TestMain lets the test binary act as the swarmflow command in a child
// process, so tests can kill a real peer process abruptly.
func TestMain(m *testing.M) {
	if args := os.Getenv("SWARMFLOW_TEST_CHILD"); args != "" {
		var a []string
		if err := json.Unmarshal([]byte(args), &a); err != nil {
			panic(err)
		}
		os.Exit(Main(a, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

type child struct {
	cmd  *exec.Cmd
	mu   sync.Mutex
	logs []map[string]any
	done chan struct{}
}

func startChild(t *testing.T, args ...string) *child {
	t.Helper()
	b, _ := json.Marshal(args)
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "SWARMFLOW_TEST_CHILD="+string(b))
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	c := &child{cmd: cmd, done: make(chan struct{})}
	go func() {
		defer close(c.done)
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			var rec map[string]any
			if json.Unmarshal(sc.Bytes(), &rec) == nil {
				c.mu.Lock()
				c.logs = append(c.logs, rec)
				c.mu.Unlock()
			} else if testing.Verbose() {
				t.Logf("child: %s", sc.Text())
			}
		}
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})
	return c
}

func (c *child) find(msg string) map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.logs {
		if r["msg"] == msg {
			return r
		}
	}
	return nil
}

// startSeeder runs an in-process seeder whose uploads are rate limited, so the
// child download takes long enough to be killed midway.
func startSeeder(t *testing.T, m *manifest.Manifest, path string, bytesPerSec int) string {
	t.Helper()
	st, err := store.OpenComplete(path, m)
	if err != nil {
		t.Fatal(err)
	}
	have, _ := st.Recheck(context.Background(), 2)
	ln, err := faultnet.Listen("127.0.0.1:0", faultnet.Impairment{BytesPerSec: bytesPerSec})
	if err != nil {
		t.Fatal(err)
	}
	ncfg := node.DefaultConfig()
	ncfg.Listener = ln
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	n, err := node.New(ncfg, log)
	if err != nil {
		t.Fatal(err)
	}
	co := coordinator.New(m, st, have, n.ID(), coordinator.DefaultConfig(), log)
	n.AddSwarm(co)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Go(func() { co.Run(ctx) })
	wg.Go(func() { n.Run(ctx) })
	t.Cleanup(func() { cancel(); wg.Wait(); st.Close() })
	return n.Addr().String()
}

func TestCrashAndResume(t *testing.T) {
	dir := t.TempDir()
	const size, pieceSize = 8 << 20, 64 << 10 // 128 pieces
	src := filepath.Join(dir, "src.bin")
	if err := dataset.WriteFile(src, size, 99); err != nil {
		t.Fatal(err)
	}
	m, err := manifest.BuildFile(src, pieceSize)
	if err != nil {
		t.Fatal(err)
	}
	mpath := filepath.Join(dir, "src.swarm")
	m.Save(mpath)
	seeder := startSeeder(t, m, src, 2<<20) // ~4 s for the whole file
	out := filepath.Join(dir, "dl", "payload.bin")
	dbPath := filepath.Join(dir, "dl", "state.db")
	args := []string{"get", "-manifest", mpath, "-out", out, "-state-db", dbPath, "-peer", seeder,
		"-log-json", "-progress", "0", "-exit-on-complete"}

	// Run 1: hard-kill the process once about a third of the pieces are recorded.
	c1 := startChild(t, args...)
	db, err := statedb.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	deadline := time.Now().Add(30 * time.Second)
	var atKill int
	for {
		if n, err := db.CountPersisted(m.ID()); err == nil && n >= m.NumPieces()/3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child made no progress")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := c1.cmd.Process.Kill(); err != nil { // TerminateProcess on Windows, SIGKILL elsewhere
		t.Fatal(err)
	}
	c1.cmd.Wait()
	<-c1.done
	atKill, _ = db.CountPersisted(m.ID())
	if atKill == m.NumPieces() {
		t.Fatal("download finished before it could be killed")
	}

	// Every recorded piece must be intact on disk: the record is only written after the flush.
	recs, _ := db.Persisted(m.ID())
	part, _, err := store.Create(out+".part", out, m)
	if err != nil {
		t.Fatal(err)
	}
	idx := make([]int, len(recs))
	for i, r := range recs {
		idx[i] = r.Index
	}
	valid, err := part.Verify(context.Background(), 2, idx)
	if err != nil || valid.Count() != len(recs) {
		t.Fatalf("after crash: %d records, %d valid on disk (%v)", len(recs), valid.Count(), err)
	}
	// Corrupt one recorded piece, as a damaged disk block would; full resume verification must catch it.
	corrupt := recs[len(recs)/2].Index
	data, _ := part.ReadPiece(corrupt)
	data[10] ^= 0xFF
	part.WritePiece(corrupt, data)
	part.Close()
	t.Logf("killed at %d/%d recorded pieces; corrupted piece %d", atKill, m.NumPieces(), corrupt)

	// Run 2: resume to completion.
	c2 := startChild(t, args...)
	waitErr := make(chan error, 1)
	go func() { waitErr <- c2.cmd.Wait() }()
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("resumed run failed: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("resumed run did not finish")
	}
	<-c2.done

	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := os.ReadFile(src)
	if !bytes.Equal(got, want) {
		t.Fatal("resumed download differs from source")
	}
	res := c2.find("resumed download")
	if res == nil {
		t.Fatal("no resume log record")
	}
	resumed, invalid := int(res["resumed_pieces"].(float64)), int(res["invalid_on_resume"].(float64))
	if resumed != atKill-1 || invalid != 1 {
		t.Fatalf("resumed %d pieces with %d invalid; want %d and 1", resumed, invalid, atKill-1)
	}
	exit := c2.find("exiting")
	down := int64(exit["bytes_down"].(float64))
	need := int64(m.NumPieces()-resumed) * pieceSize
	if down < need || down > need+8*pieceSize {
		t.Fatalf("resumed run downloaded %d bytes; expected about %d (only the missing pieces)", down, need)
	}
	if s, _, _ := db.Swarm(m.ID()); s.Status != statedb.StatusComplete {
		t.Fatalf("status %q after completion", s.Status)
	}
	t.Logf("resumed %d pieces (overhead %v ms), re-downloaded %d bytes of %d", resumed, res["resume_overhead_ms"], down, size)
}

func TestHelpAndBadFlags(t *testing.T) {
	var out, errb strings.Builder
	if code := Main([]string{"get", "-h"}, &out, &errb); code != 0 {
		t.Fatalf("-h exit code %d", code)
	}
	if code := Main([]string{"get", "-resume-verify", "maybe", "-manifest", "x"}, &out, &errb); code != 1 {
		t.Fatalf("bad -resume-verify exit code %d", code)
	}
	if code := Main([]string{"nope"}, &out, &errb); code != 2 {
		t.Fatalf("unknown command exit code %d", code)
	}
}
