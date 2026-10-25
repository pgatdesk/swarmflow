package coordinator

import "time"

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
}

func (co *Coordinator) publishStatus() {
	s := &Status{
		SwarmID: co.m.ID().String(), Name: co.m.Name(), Size: co.m.Size(), Pieces: co.m.NumPieces(),
		Have: co.have.Count(), Outstanding: len(co.reqs), Verifying: co.verifying,
		BytesDown: co.bytesDown, BytesUp: co.bytesUp.Load(), Rejected: co.rejected,
		Complete: co.have.Complete(), StartedAt: co.started, FinishedAt: co.finished,
	}
	for _, p := range co.peerList {
		s.Peers = append(s.Peers, PeerStatus{
			ID: p.id.Short(), Addr: p.c.RemoteAddr().String(), Outbound: p.c.Outbound(),
			ConnectedAt: p.c.ConnectedAt(), Has: p.has.Count(), Outstanding: len(p.outstanding),
			PiecesFrom: p.piecesFrom, BytesFrom: p.bytesFrom, Timeouts: p.timeouts,
			Rejects: p.rejects, Invalid: p.invalid, Duplicates: p.duplicates,
		})
	}
	co.status.Store(s)
}
