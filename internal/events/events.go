// Package events keeps a bounded, sequence-numbered log of what a peer did and
// why: connections, scheduling decisions, failures, recovery. Adding an event
// never blocks; when the buffer is full the oldest events are overwritten and
// counted as dropped. Messages are built from the real decision inputs.
package events

import (
	"sync"
	"time"
)

// Types of events.
const (
	PeerConnected     = "peer_connected"
	PeerDisconnected  = "peer_disconnected"
	PieceScheduled    = "piece_scheduled"
	PieceRejected     = "piece_rejected"
	RequestTimeout    = "request_timeout"
	PeerCooldown      = "peer_cooldown"
	PeerBanned        = "peer_banned"
	EndgameEntered    = "endgame_entered"
	EndgameDuplicate  = "endgame_duplicate"
	DownloadComplete  = "download_complete"
	SourcesLost       = "sources_lost"
	SourcesRestored   = "sources_restored"
	UnderReplicated   = "under_replicated"
	ReplicationTarget = "replication_target_reached"
	ProofFailed       = "proof_failed"
)

type Event struct {
	Seq     uint64         `json:"seq"`
	Time    time.Time      `json:"time"`
	Type    string         `json:"type"`
	Message string         `json:"message"`
	Fields  map[string]any `json:"fields,omitempty"`
}

type Ring struct {
	mu      sync.Mutex
	buf     []Event
	next    uint64 // sequence number of the next event; also the total added
	dropped uint64
}

func NewRing(size int) *Ring { return &Ring{buf: make([]Event, max(size, 1))} }

// Add records an event, assigning its sequence number and (if unset) time.
// A nil Ring discards events, so callers need not check.
func (r *Ring) Add(e Event) {
	if r == nil {
		return
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next >= uint64(len(r.buf)) {
		r.dropped++
	}
	e.Seq = r.next
	r.buf[r.next%uint64(len(r.buf))] = e
	r.next++
}

// Since returns up to limit events with Seq ≥ seq, oldest first, and the
// sequence number to ask for next. Events already overwritten are skipped.
func (r *Ring) Since(seq uint64, limit int) ([]Event, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	oldest := uint64(0)
	if r.next > uint64(len(r.buf)) {
		oldest = r.next - uint64(len(r.buf))
	}
	seq = max(seq, oldest)
	var out []Event
	for s := seq; s < r.next && (limit <= 0 || len(out) < limit); s++ {
		out = append(out, r.buf[s%uint64(len(r.buf))])
	}
	return out, seq + uint64(len(out))
}

// Stats returns the total number of events added and how many were overwritten.
func (r *Ring) Stats() (total, dropped uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.next, r.dropped
}
