package node

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/protocol"
	"github.com/pgatdesk/swarmflow/internal/statedb"
)

type benchJournal struct {
	db *statedb.DB
	id protocol.SwarmID
}

func (j benchJournal) PiecePersisted(i int) error             { return j.db.MarkPersisted(j.id, i, [32]byte{}) }
func (j benchJournal) Completed() error                       { return nil }
func (j benchJournal) PeerSeen(string, protocol.PeerID) error { return nil }

// BenchmarkTransfer measures one seeder → one downloader over loopback in one
// process (no shaping), varying the knobs that bound pipelining.
func BenchmarkTransfer(b *testing.B) {
	for _, tc := range []struct {
		perPeer, verifiers, piece int
		journal                   bool
	}{
		{4, 2, 256 << 10, false}, {16, 2, 256 << 10, false}, {16, 4, 256 << 10, false}, {4, 2, 1 << 20, false},
		{4, 2, 256 << 10, true}, {4, 2, 1 << 20, true},
	} {
		b.Run(fmt.Sprintf("perPeer=%d/verifiers=%d/piece=%dKiB/journal=%v", tc.perPeer, tc.verifiers, tc.piece>>10, tc.journal), func(b *testing.B) {
			const size = 128 << 20
			b.SetBytes(size)
			for range b.N {
				b.StopTimer()
				f := newFixtureSized(b, size, tc.piece)
				seed := f.start("seed", peerOpts{seedFile: f.writeSource("src.bin", nil),
					ccfg: func(c *coordinator.Config) { c.UploadSlots, c.UploadQueueCap = tc.perPeer, 4*tc.perPeer }})
				var j coordinator.Journal
				if tc.journal { // the CLI's durable per-piece record, as in `swarmflow get`
					db, err := statedb.Open(filepath.Join(f.dir, "state.db"))
					if err != nil {
						b.Fatal(err)
					}
					db.UpsertSwarm(statedb.Swarm{ID: f.m.ID(), Name: "p", Size: f.m.Size(), PieceSize: f.m.PieceSize(),
						Pieces: f.m.NumPieces(), Manifest: f.m.Canonical(), Status: statedb.StatusDownloading})
					defer db.Close()
					j = benchJournal{db: db, id: f.m.ID()}
				}
				b.StartTimer()
				d := f.start("d", peerOpts{peers: []string{seed.addr()}, ccfg: func(c *coordinator.Config) {
					c.MaxPerPeer, c.VerifyWorkers, c.Journal = tc.perPeer, tc.verifiers, j
				}})
				select {
				case <-d.co.Complete():
				case <-time.After(2 * time.Minute):
					b.Fatal("transfer did not complete")
				}
				b.StopTimer()
				d.stop()
				seed.stop()
			}
		})
	}
}
