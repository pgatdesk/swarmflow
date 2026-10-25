package coordinator

import (
	"fmt"
	"strings"
	"time"

	"github.com/pgatdesk/swarmflow/internal/events"
	"github.com/pgatdesk/swarmflow/internal/scheduler"
)

// Observer receives the outcome and latency of every request, for metrics.
// It is called on the event loop and must not block.
type Observer interface {
	RequestDone(outcome string, latency time.Duration)
}

// Request outcomes reported to the Observer.
const (
	OutcomeOK       = "ok"
	OutcomeTimeout  = "timeout"
	OutcomeBusy     = "busy"
	OutcomeRejected = "rejected"
	OutcomeInvalid  = "invalid"
)

func (co *Coordinator) observe(outcome string, d time.Duration) {
	if co.cfg.Observer != nil {
		co.cfg.Observer.RequestDone(outcome, d)
	}
}

func (co *Coordinator) emit(typ, msg string, fields map[string]any) {
	co.cfg.Events.Add(events.Event{Type: typ, Message: msg, Fields: fields}) // nil-safe
}

// skippedPeer is a source that had the piece but was passed over, and why.
type skippedPeer struct {
	Peer   string `json:"peer"`
	Reason string `json:"reason"`
}

func (co *Coordinator) pickerReason(piece int) string {
	switch co.cfg.Picker.Name() {
	case "rarest":
		return fmt.Sprintf("rarest-first: held by %d of %d connected peers", co.avail[piece], len(co.peerList))
	case "sequential":
		return "sequential: next missing piece by index"
	}
	return "next piece in the random order"
}

func dur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond).String()
	default:
		return d.Round(time.Millisecond).String()
	}
}

// emitScheduled records a scheduling decision with the measurements behind it.
// waiting is the request being duplicated, for endgame decisions.
func (co *Coordinator) emitScheduled(piece int, cands []scheduler.Candidate, chosen int, skipped []skippedPeer, waiting *request, now time.Time) {
	if co.cfg.Events == nil {
		return
	}
	ex := scheduler.Explain(co.cfg.Selector.Name(), co.m.PieceLen(piece), cands, chosen)
	var b strings.Builder
	typ := events.PieceScheduled
	if waiting != nil {
		typ = events.EndgameDuplicate
		fmt.Fprintf(&b, "endgame: piece %d also requested from %s (request to %s has waited %s, expected %s); %s",
			piece, ex.Chosen.Peer, waiting.peer.id.Short(), dur(now.Sub(waiting.sent)), dur(waiting.expected), ex.Reason)
	} else {
		fmt.Fprintf(&b, "piece %d → %s; %s; %s", piece, ex.Chosen.Peer, co.pickerReason(piece), ex.Reason)
	}
	if ex.RunnerUp != nil {
		fmt.Fprintf(&b, "; runner-up %s (expected %.2fs, %d queued)", ex.RunnerUp.Peer, ex.RunnerUp.ExpectedS, ex.RunnerUp.Outstanding)
	}
	for _, s := range skipped {
		fmt.Fprintf(&b, "; skipped %s: %s", s.Peer, s.Reason)
	}
	co.emit(typ, b.String(), map[string]any{
		"piece": piece, "availability": co.avail[piece], "connected": len(co.peerList),
		"picker": co.cfg.Picker.Name(), "explanation": ex, "skipped": skipped,
	})
}
