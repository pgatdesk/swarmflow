package coordinator

import "time"

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
}

// Status is an immutable snapshot of a swarm, published by the Run goroutine
// for readers on other goroutines (CLI progress, API).
type Status struct {
	SwarmID     string       `json:"swarm_id"`
	Name        string       `json:"name"`
	Size        int64        `json:"size"`
	Pieces      int          `json:"pieces"`
	Have        int          `json:"have"`
	Outstanding int          `json:"outstanding_requests"`
	Verifying   int          `json:"verifying"`
	BytesDown   int64        `json:"bytes_down"`
	BytesUp     int64        `json:"bytes_up"`
	Rejected    int          `json:"rejected_pieces"`
	Complete    bool         `json:"complete"`
	Endgame     bool         `json:"endgame"`
	Picker      string       `json:"picker"`
	Selector    string       `json:"selector"`
	Counters    Counters     `json:"counters"`
	StartedAt   time.Time    `json:"started_at"`
	FinishedAt  time.Time    `json:"finished_at,omitzero"`
	Peers       []PeerStatus `json:"peers"`
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

func (co *Coordinator) publishStatus(now time.Time) {
	co.lastStatus = now
	s := &Status{
		SwarmID: co.m.ID().String(), Name: co.m.Name(), Size: co.m.Size(), Pieces: co.m.NumPieces(),
		Have: co.have.Count(), Outstanding: len(co.reqs), Verifying: co.verifying,
		BytesDown: co.counters.BytesDown, BytesUp: co.bytesUp.Load(), Rejected: co.counters.Rejected,
		Complete: co.have.Complete(), Endgame: co.inEndgame,
		Picker: co.cfg.Picker.Name(), Selector: co.cfg.Selector.Name(), Counters: co.counters,
		StartedAt: co.started, FinishedAt: co.finished,
	}
	for _, p := range co.peerList {
		st := p.stats
		ps := PeerStatus{
			ID: p.id.Short(), Addr: p.c.RemoteAddr().String(), Outbound: p.c.Outbound(),
			ConnectedAt: p.c.ConnectedAt(), Has: p.has.Count(), Outstanding: len(p.outstanding),
			PiecesFrom: p.piecesFrom, BytesFrom: p.bytesFrom, Timeouts: st.Timeouts,
			Rejects: st.Rejects, Invalid: st.Invalid, Duplicates: p.duplicates,
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
