package coordinator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"time"

	"github.com/pgatdesk/swarmflow/internal/events"
	"github.com/pgatdesk/swarmflow/internal/protocol"
)

// ReplicationConfig controls replica placement (design §10, decision 0001 #6).
//
// A peer with Factor R ≥ 2 tries to keep, for every piece it holds, at least
// R-1 *verified* copies on other connected peers. A copy is verified only when
// its holder answers a possession challenge: SHA-256(nonce ‖ piece). A claim
// (BITFIELD/HAVE) alone never counts. Proofs expire after ProofTTL and are
// dropped when the holder disconnects, so lost copies are noticed and
// replaced. There is no consensus: several holders may replicate the same
// piece concurrently, which can overshoot the target slightly.
type ReplicationConfig struct {
	// Factor is the target number of copies including our own; ≤ 1 disables offering.
	Factor int
	// Accept makes us accept REPL_OFFERs (advertised by the node's handshake flag).
	Accept bool
	// Budget bounds the bytes we accept as replicas; 0 is unlimited.
	Budget int64
	// Interval is how often placement is re-evaluated.
	Interval       time.Duration
	ProofTTL       time.Duration
	ProofTimeout   time.Duration
	OfferTimeout   time.Duration
	DeclineBackoff time.Duration
	// MaxChallenges and MaxOffers bound outstanding challenges and offers.
	MaxChallenges int
	MaxOffers     int
}

func DefaultReplication() ReplicationConfig {
	return ReplicationConfig{
		Interval:       500 * time.Millisecond,
		ProofTTL:       5 * time.Minute,
		ProofTimeout:   10 * time.Second,
		OfferTimeout:   60 * time.Second,
		DeclineBackoff: 30 * time.Second,
		MaxChallenges:  32,
		MaxOffers:      32,
	}
}

type replicaKey struct {
	peer  protocol.PeerID
	piece int
}

type challenge struct {
	nonce    [16]byte
	expected [32]byte
	ready    bool // expected digest computed and CHALLENGE sent
	created  time.Time
	deadline time.Time
}

// replication is owned by the coordinator's Run goroutine.
type replication struct {
	verified        []map[protocol.PeerID]time.Time // per piece: holder → last valid proof
	challenges      map[replicaKey]*challenge
	offers          map[replicaKey]time.Time // pending offers → sent
	pendingPerPiece []int                    // offers + challenges per piece
	under           int                      // under-replicated pieces at the last sweep
	lastSweep       time.Time
	counters        ReplicationCounters
}

// ReplicationCounters are cumulative replication events.
type ReplicationCounters struct {
	ProofsOK       int `json:"proofs_ok"`
	ProofsFailed   int `json:"proofs_failed"`
	ProofTimeouts  int `json:"proof_timeouts"`
	OffersSent     int `json:"offers_sent"`
	OffersAccepted int `json:"offers_accepted"`
	OffersDeclined int `json:"offers_declined"`
	OffersReceived int `json:"offers_received"`
	ReplicasStored int `json:"replicas_stored"` // offered pieces we accepted and persisted
}

func newReplication(pieces int) *replication {
	r := &replication{
		verified:        make([]map[protocol.PeerID]time.Time, pieces),
		challenges:      make(map[replicaKey]*challenge),
		offers:          make(map[replicaKey]time.Time),
		pendingPerPiece: make([]int, pieces),
	}
	for i := range r.verified {
		r.verified[i] = make(map[protocol.PeerID]time.Time)
	}
	return r
}

type proofKind uint8

const (
	proofExpect  proofKind = iota // we challenge: compute the digest we expect
	proofRespond                  // we were challenged: compute the digest to send
)

type proofJob struct {
	kind  proofKind
	peer  protocol.PeerID
	piece int
	nonce [16]byte
}

type proofResult struct {
	proofJob
	digest [32]byte
	err    error
}

// proofWorker reads pieces and computes possession digests off the event loop.
func (co *Coordinator) proofWorker(ctx context.Context) {
	for {
		var j proofJob
		select {
		case <-ctx.Done():
			return
		case j = <-co.proofJobs:
		}
		res := proofResult{proofJob: j}
		data, err := co.st.ReadPiece(j.piece)
		if err != nil {
			res.err = err
		} else {
			h := sha256.New()
			h.Write(j.nonce[:])
			h.Write(data)
			copy(res.digest[:], h.Sum(nil))
		}
		select {
		case co.proofResults <- res:
		case <-ctx.Done():
			return
		}
	}
}

func (co *Coordinator) submitProof(j proofJob) bool {
	select {
	case co.proofJobs <- j:
		return true
	default:
		return false
	}
}

func (co *Coordinator) onProofResult(r proofResult, now time.Time) {
	p := co.peers[r.peer]
	switch r.kind {
	case proofRespond:
		if r.err == nil && p != nil && !p.closing {
			p.c.SendControl(protocol.Proof{Piece: uint32(r.piece), Nonce: r.nonce, Digest: r.digest})
		}
	case proofExpect:
		k := replicaKey{r.peer, r.piece}
		c := co.repl.challenges[k]
		if c == nil || c.nonce != r.nonce {
			return
		}
		if r.err != nil || p == nil || p.closing {
			co.dropChallenge(k)
			return
		}
		c.expected, c.ready, c.deadline = r.digest, true, now.Add(co.cfg.Replication.ProofTimeout)
		p.c.SendControl(protocol.Challenge{Piece: uint32(r.piece), Nonce: r.nonce})
	}
}

func (co *Coordinator) dropChallenge(k replicaKey) {
	if _, ok := co.repl.challenges[k]; ok {
		delete(co.repl.challenges, k)
		co.repl.pendingPerPiece[k.piece]--
	}
}

func (co *Coordinator) dropOffer(k replicaKey) {
	if _, ok := co.repl.offers[k]; ok {
		delete(co.repl.offers, k)
		co.repl.pendingPerPiece[k.piece]--
	}
}

// onChallenge answers a possession challenge for a piece we have persisted.
// Challenges for pieces we lack get no answer; the challenger times out.
func (co *Coordinator) onChallenge(p *peer, m protocol.Challenge) {
	i := int(m.Piece)
	if i >= co.m.NumPieces() || !co.have.Has(i) {
		return
	}
	co.submitProof(proofJob{kind: proofRespond, peer: p.id, piece: i, nonce: m.Nonce}) // dropped if busy
}

func (co *Coordinator) onProof(p *peer, m protocol.Proof, now time.Time) {
	k := replicaKey{p.id, int(m.Piece)}
	c := co.repl.challenges[k]
	if c == nil || !c.ready || c.nonce != m.Nonce {
		return // late or unsolicited
	}
	co.dropChallenge(k)
	if m.Digest == c.expected {
		co.repl.verified[k.piece][p.id] = now
		co.repl.counters.ProofsOK++
		return
	}
	co.repl.counters.ProofsFailed++
	co.log.Warn("possession proof failed: peer claims a piece it does not hold", "peer", p.id.Short(), "piece", k.piece)
	co.emit(events.ProofFailed, fmt.Sprintf("%s failed the possession proof for piece %d; its claim is not counted as a replica",
		p.id.Short(), k.piece), map[string]any{"remote": p.id.Short(), "piece": k.piece})
	co.onInvalid(p, now)
}

// onReplOffer decides whether to store a piece offered as a replica.
func (co *Coordinator) onReplOffer(p *peer, m protocol.ReplOffer) {
	i := int(m.Piece)
	if i >= co.m.NumPieces() {
		co.violation(p, "REPL_OFFER for piece out of range")
		return
	}
	co.repl.counters.OffersReceived++
	rc := co.cfg.Replication
	size := int64(co.m.PieceLen(i))
	accept := co.wanted.Has(i) || (rc.Accept && co.st.Writable() && !co.have.Has(i) && p.has.Has(i) &&
		(rc.Budget == 0 || co.wantedBytes+size <= rc.Budget))
	if accept && !co.wanted.Has(i) {
		co.wanted.Set(i)
		co.wantedBytes += size
		if co.state[i] != stPersisted {
			co.wantedOpen++
		}
	}
	p.c.SendControl(protocol.ReplReply{Piece: m.Piece, Accept: accept})
}

func (co *Coordinator) onReplReply(p *peer, m protocol.ReplReply, now time.Time) {
	k := replicaKey{p.id, int(m.Piece)}
	if _, ok := co.repl.offers[k]; !ok {
		return
	}
	if m.Accept {
		co.repl.counters.OffersAccepted++
		return // the offer stays pending until the peer announces HAVE
	}
	co.repl.counters.OffersDeclined++
	co.dropOffer(k)
	p.declinedUntil = now.Add(co.cfg.Replication.DeclineBackoff)
}

// onReplicaHave resolves a pending offer once its target announces the piece;
// the next sweep then challenges it before counting it.
func (co *Coordinator) onReplicaHave(p *peer, i int) {
	co.dropOffer(replicaKey{p.id, i})
}

func (co *Coordinator) forgetReplicas(id protocol.PeerID) {
	for i := range co.repl.verified {
		delete(co.repl.verified[i], id)
	}
	for k := range co.repl.challenges {
		if k.peer == id {
			co.dropChallenge(k)
		}
	}
	for k := range co.repl.offers {
		if k.peer == id {
			co.dropOffer(k)
		}
	}
}

func (co *Coordinator) replicationTimeouts(now time.Time) {
	rc := co.cfg.Replication
	for k, c := range co.repl.challenges {
		if (c.ready && now.After(c.deadline)) || (!c.ready && now.Sub(c.created) > rc.ProofTimeout) {
			co.dropChallenge(k)
			co.repl.counters.ProofTimeouts++
		}
	}
	for k, sent := range co.repl.offers {
		if now.Sub(sent) > rc.OfferTimeout {
			co.dropOffer(k)
		}
	}
}

// verifiedReplicas counts fresh proofs from connected peers for piece i,
// expiring stale ones.
func (co *Coordinator) verifiedReplicas(i int, now time.Time) int {
	n := 0
	for id, t := range co.repl.verified[i] {
		if now.Sub(t) > co.cfg.Replication.ProofTTL {
			delete(co.repl.verified[i], id)
			continue
		}
		if _, ok := co.peers[id]; ok {
			n++
		}
	}
	return n
}

// replicate tops up every piece we hold to Factor-1 verified copies elsewhere:
// first by challenging peers that claim the piece, then by offering it to
// peers that accept replicas.
func (co *Coordinator) replicate(now time.Time) {
	rc := co.cfg.Replication
	if rc.Factor <= 1 || now.Sub(co.repl.lastSweep) < rc.Interval {
		return
	}
	co.repl.lastSweep = now
	target := rc.Factor - 1
	under := 0
	np := len(co.peerList)
	for i := range co.m.NumPieces() {
		if !co.have.Has(i) {
			continue
		}
		v := co.verifiedReplicas(i, now)
		if v >= target {
			continue
		}
		under++
		need := target - v - co.repl.pendingPerPiece[i]
		// Rotate the starting peer by piece so load spreads across holders and acceptors.
		for k := 0; k < np && need > 0 && len(co.repl.challenges) < rc.MaxChallenges; k++ {
			p := co.peerList[(i+k)%np]
			key := replicaKey{p.id, i}
			if p.closing || !p.gotBitfield || !p.has.Has(i) {
				continue
			}
			if _, ok := co.repl.verified[i][p.id]; ok {
				continue
			}
			if _, ok := co.repl.challenges[key]; ok {
				continue
			}
			c := &challenge{created: now}
			rand.Read(c.nonce[:])
			if !co.submitProof(proofJob{kind: proofExpect, peer: p.id, piece: i, nonce: c.nonce}) {
				break
			}
			co.repl.challenges[key] = c
			co.repl.pendingPerPiece[i]++
			need--
		}
		for k := 0; k < np && need > 0 && len(co.repl.offers) < rc.MaxOffers; k++ {
			p := co.peerList[(i+k)%np]
			key := replicaKey{p.id, i}
			if p.closing || !p.gotBitfield || !p.acceptsReplicas || p.has.Has(i) || now.Before(p.declinedUntil) {
				continue
			}
			if _, ok := co.repl.offers[key]; ok {
				continue
			}
			if p.c.SendControl(protocol.ReplOffer{Piece: uint32(i)}) != nil {
				continue
			}
			co.repl.offers[key] = now
			co.repl.pendingPerPiece[i]++
			co.repl.counters.OffersSent++
			need--
		}
	}
	if under > 0 && co.repl.under == 0 {
		co.log.Info("pieces under-replicated", "pieces", under, "target_copies", rc.Factor)
		co.emit(events.UnderReplicated, fmt.Sprintf("%d pieces have fewer than %d verified copies elsewhere; challenging holders and offering replicas",
			under, target), map[string]any{"pieces": under, "target_copies": rc.Factor})
	} else if under == 0 && co.repl.under > 0 {
		co.log.Info("replication target reached", "target_copies", rc.Factor)
		co.emit(events.ReplicationTarget, fmt.Sprintf("every held piece has %d verified copies elsewhere (target %d including ours)",
			target, rc.Factor), map[string]any{"target_copies": rc.Factor})
	}
	co.repl.under = under
}

// ReplicationStatus summarises replica placement for pieces we hold.
type ReplicationStatus struct {
	Target          int                 `json:"target_copies"`
	Accepting       bool                `json:"accepting_replicas"`
	UnderReplicated int                 `json:"under_replicated_pieces"`
	MinVerified     int                 `json:"min_verified_copies"`  // excluding ours
	MeanVerified    float64             `json:"mean_verified_copies"` // excluding ours
	Challenges      int                 `json:"challenges_in_flight"`
	Offers          int                 `json:"offers_in_flight"`
	ReplicaBytes    int64               `json:"replica_bytes_accepted"`
	Counters        ReplicationCounters `json:"counters"`
}

func (co *Coordinator) replicationStatus(now time.Time) ReplicationStatus {
	rc := co.cfg.Replication
	s := ReplicationStatus{Target: rc.Factor, Accepting: rc.Accept, UnderReplicated: co.repl.under,
		Challenges: len(co.repl.challenges), Offers: len(co.repl.offers), ReplicaBytes: co.wantedBytes,
		Counters: co.repl.counters}
	held, sum := 0, 0
	s.MinVerified = -1
	for i := range co.m.NumPieces() {
		if !co.have.Has(i) {
			continue
		}
		v := co.verifiedReplicas(i, now)
		held++
		sum += v
		if s.MinVerified < 0 || v < s.MinVerified {
			s.MinVerified = v
		}
	}
	if held > 0 {
		s.MeanVerified = float64(sum) / float64(held)
	} else {
		s.MinVerified = 0
	}
	return s
}
