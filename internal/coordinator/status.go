package coordinator

import (
	"fmt"
	"time"

	"github.com/pgatdesk/swarmflow/internal/events"
)

// Counters are cumulative event counts for a swarm.
type Counters struct {
	BytesDown       int64 `json:"bytes_down"`
	Requests        int   `json:"requests"`
	Timeouts        int   `json:"timeouts"`
	Reassigned      int   `json:"reassigned"`
	Rejected        int   `json:"rejected_pieces"`
	DuplicatePieces int   `json:"duplicate_pieces"`
	EndgameRequests int   `json:"endgame_requests"`
	Cancelled       int   `json:"cancelled"`
	Banned          int   `json:"banned"`
	// BusySent counts REJECT(busy) we sent (our upload queue was full);
	// BusyReceived counts those we received.
	BusySent     int `json:"busy_sent"`
	BusyReceived int `json:"busy_received"`
}

// Status is an immutable snapshot of a swarm, published by the Run goroutine
// for readers on other goroutines (CLI progress, API).
type Status struct {
	SwarmID     string   `json:"swarm_id"`
	Name        string   `json:"name"`
	Size        int64    `json:"size"`
	Pieces      int      `json:"pieces"`
	Have        int      `json:"have"`
	Outstanding int      `json:"outstanding_requests"`
	Verifying   int      `json:"verifying"`
	UploadQueue int      `json:"upload_queue"` // queued + in-progress uploads, all peers
	BytesDown   int64    `json:"bytes_down"`
	BytesUp     int64    `json:"bytes_up"`
	Rejected    int      `json:"rejected_pieces"`
	Complete    bool     `json:"complete"`
	Endgame     bool     `json:"endgame"`
	Picker      string   `json:"picker"`
	Selector    string   `json:"selector"`
	Counters    Counters `json:"counters"`
	// Unavailable counts needed pieces that no connected peer has: the download
	// cannot finish until a source for them (re)appears.
	Unavailable int               `json:"unavailable_pieces"`
	Replication ReplicationStatus `json:"replication"`
	// Sources lists every peer we accepted a (non-duplicate) piece from,
	// including peers that have since disconnected, in first-contribution order.
	Sources   []SourceStatus `json:"sources"`
	StartedAt time.Time      `json:"started_at"`
	// FirstPeerAt is when the first peer connected: time before it is spent
	// waiting for sources, not transferring.
	FirstPeerAt time.Time `json:"first_peer_at,omitzero"`
	// FirstPieceAt is when this run persisted its first piece (time to first data).
	FirstPieceAt time.Time `json:"first_piece_at,omitzero"`
	// Process is set only in -stats-out files.
	Process *ProcessInfo `json:"process,omitempty"`
	// Resume describes what this run recovered from durable state, if anything.
	Resume     *ResumeInfo  `json:"resume,omitempty"`
	FinishedAt time.Time    `json:"finished_at,omitzero"`
	Peers      []PeerStatus `json:"peers"`
}

// ProcessInfo is filled in by the hosting process when it writes final stats.
type ProcessInfo struct {
	PeakRSSMiB float64 `json:"peak_rss_mib"` // from the kernel's own accounting (Linux VmHWM)
}

// ResumeInfo records what was recovered from durable state at startup.
type ResumeInfo struct {
	ResumedPieces   int   `json:"resumed_pieces"`
	InvalidOnResume int   `json:"invalid_on_resume"`
	OverheadMs      int64 `json:"overhead_ms"` // time spent re-verifying before the transfer could start
}

// SourceStatus is one peer's cumulative contribution to our download.
type SourceStatus struct {
	ID     string `json:"id"`
	Addr   string `json:"addr"` // listen address as observed by us
	Pieces int    `json:"pieces"`
	Bytes  int64  `json:"bytes"`
}

type PeerStatus struct {
	ID          string    `json:"id"`
	Addr        string    `json:"addr"`
	Outbound    bool      `json:"outbound"`
	ConnectedAt time.Time `json:"connected_at"`
	Has         int       `json:"has"`
	Outstanding int       `json:"outstanding"`
	PiecesFrom  int       `json:"pieces_from"`
	BytesFrom   int64     `json:"bytes_from"`
	Timeouts    int       `json:"timeouts"`
	Rejects     int       `json:"rejects"`
	Busy        int       `json:"busy"`
	Invalid     int       `json:"invalid"`
	Duplicates  int       `json:"duplicates"`
	// Measurements; zero when not yet known (see the *Known flags).
	ThroughputBps   float64 `json:"throughput_bps"`
	ThroughputKnown bool    `json:"throughput_known"`
	RTTMillis       float64 `json:"rtt_ms"`
	RTTKnown        bool    `json:"rtt_known"`
	LatencyP50ms    float64 `json:"latency_p50_ms"`
	LatencyP95ms    float64 `json:"latency_p95_ms"`
	Reliability     float64 `json:"reliability"`
	Cooldown        bool    `json:"cooldown"`
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// countUnavailable counts needed pieces without any connected source and logs
// when that number changes.
func (co *Coordinator) countUnavailable() int {
	n := 0
	for i := range co.state {
		if co.Needed(i) && co.avail[i] == 0 {
			n++
		}
	}
	informed := false // before any peer reports its pieces, "no source" means nothing yet
	for _, p := range co.peerList {
		informed = informed || p.gotBitfield
	}
	if n != co.unavailable && informed {
		if n > 0 {
			co.log.Warn("pieces have no available source; waiting for a peer that has them", "pieces", n)
			co.emit(events.SourcesLost, fmt.Sprintf("%d needed pieces have no connected source; waiting and re-discovering", n),
				map[string]any{"pieces": n})
		} else {
			co.log.Info("every needed piece has a source again")
			co.emit(events.SourcesRestored, "every needed piece has a connected source again", nil)
		}
		co.unavailable = n
	}
	return n
}

func (co *Coordinator) publishStatus(now time.Time) {
	co.lastStatus = now
	s := &Status{
		Unavailable: co.countUnavailable(), Replication: co.replicationStatus(now),
		SwarmID: co.m.ID().String(), Name: co.m.Name(), Size: co.m.Size(), Pieces: co.m.NumPieces(),
		Have: co.have.Count(), Outstanding: len(co.reqs), Verifying: co.verifying,
		UploadQueue: int(co.uploadQueued.Load()),
		BytesDown:   co.counters.BytesDown, BytesUp: co.bytesUp.Load(), Rejected: co.counters.Rejected,
		Complete: co.have.Complete(), Endgame: co.inEndgame && !co.have.Complete(),
		Picker: co.cfg.Picker.Name(), Selector: co.cfg.Selector.Name(), Counters: co.counters,
		StartedAt: co.started, FinishedAt: co.finished, FirstPeerAt: co.firstPeerAt, FirstPieceAt: co.firstPiece,
		Resume: co.cfg.Resume,
	}
	for _, id := range co.contribOrder {
		s.Sources = append(s.Sources, *co.contrib[id])
	}
	for _, p := range co.peerList {
		st := p.stats
		ps := PeerStatus{
			ID: p.id.Short(), Addr: p.c.RemoteAddr().String(), Outbound: p.c.Outbound(),
			ConnectedAt: p.c.ConnectedAt(), Has: p.has.Count(), Outstanding: len(p.outstanding),
			PiecesFrom: p.piecesFrom, BytesFrom: p.bytesFrom, Timeouts: st.Timeouts,
			Rejects: st.Rejects, Busy: st.Busy, Invalid: st.Invalid, Duplicates: p.duplicates,
			Reliability: st.Reliability(), Cooldown: st.InCooldown(now),
		}
		ps.ThroughputBps, ps.ThroughputKnown = st.Throughput()
		rtt, ok := st.RTT()
		ps.RTTMillis, ps.RTTKnown = ms(rtt), ok
		if p50, p95, ok := st.Latency(); ok {
			ps.LatencyP50ms, ps.LatencyP95ms = ms(p50), ms(p95)
		}
		s.Peers = append(s.Peers, ps)
	}
	co.status.Store(s)
}
