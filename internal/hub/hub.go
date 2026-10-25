// Package hub aggregates the monitoring APIs of all peers known to a tracker
// for dashboards. It finds peers through the tracker, polls their
// /api/status and /api/events, and serves:
//
//	GET /sd                     Prometheus HTTP service discovery
//	GET /api/peers              one row per peer (online, unreachable or recently gone)
//	GET /api/topology/nodes     Grafana Node Graph nodes
//	GET /api/topology/edges     Grafana Node Graph edges: only connections a peer reports
//	GET /api/events?limit=N     merged event log, newest first (optional &type=)
//
// The hub is a separate process: if it fails, peers and transfers are unaffected.
package hub

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/api"
	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/discovery"
)

const (
	maxEvents   = 5000
	keepGone    = 2 * time.Minute // how long peers absent from the tracker stay listed
	goneAfter   = 10 * time.Second
	pollTimeout = 2 * time.Second
)

type peerState struct {
	id, swarm, addr, apiAddr string
	role                     string
	lastOK, lastListed       time.Time
	err                      string
	status                   *coordinator.Status
	nextEvent                uint64
	// rate estimation between consecutive polls
	prevAt           time.Time
	prevDown, prevUp int64
	downBps, upBps   float64
	prevFrom         map[string]int64   // remote → bytes received from it
	linkBps          map[string]float64 // remote → bytes/s received from it
}

// Event is one entry of the merged event log.
type Event struct {
	Time    time.Time `json:"time"`
	Peer    string    `json:"peer"`
	Type    string    `json:"type"`
	Message string    `json:"message"`
}

type Hub struct {
	TrackerURL string
	Poll       time.Duration
	HTTP       *http.Client
	Log        *slog.Logger

	mu     sync.Mutex
	peers  map[string]*peerState
	events []Event
}

func New(trackerURL string, poll time.Duration, log *slog.Logger) *Hub {
	return &Hub{TrackerURL: strings.TrimRight(trackerURL, "/"), Poll: poll, Log: log,
		HTTP: &http.Client{Timeout: pollTimeout}, peers: map[string]*peerState{}}
}

func (h *Hub) Run(ctx context.Context) {
	t := time.NewTicker(h.Poll)
	defer t.Stop()
	for {
		h.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (h *Hub) getJSON(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := h.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(v)
}

func (h *Hub) pollOnce(ctx context.Context) {
	var listed map[string][]discovery.PeerInfo
	if err := h.getJSON(ctx, h.TrackerURL+"/peers", &listed); err != nil {
		h.Log.Warn("tracker unreachable; keeping the last known peers", "err", err)
	}
	now := time.Now()
	h.mu.Lock()
	for swarm, infos := range listed {
		for _, pi := range infos {
			id := pi.PeerID[:min(8, len(pi.PeerID))]
			ps := h.peers[id]
			if ps == nil {
				ps = &peerState{id: id, prevFrom: map[string]int64{}, linkBps: map[string]float64{}}
				h.peers[id] = ps
			}
			ps.swarm, ps.addr, ps.apiAddr, ps.lastListed = swarm[:min(8, len(swarm))], pi.Addr, pi.APIAddr, now
		}
	}
	for id, ps := range h.peers {
		if now.Sub(ps.lastListed) > keepGone {
			delete(h.peers, id)
		}
	}
	var todo []*peerState
	for _, ps := range h.peers {
		if ps.apiAddr != "" && now.Sub(ps.lastListed) < keepGone {
			todo = append(todo, ps)
		}
	}
	h.mu.Unlock()

	var wg sync.WaitGroup
	for _, ps := range todo {
		wg.Go(func() { h.pollPeer(ctx, ps) })
	}
	wg.Wait()
}

func (h *Hub) pollPeer(ctx context.Context, ps *peerState) {
	base := "http://" + ps.apiAddr
	var st api.StatusResponse
	err := h.getJSON(ctx, base+"/api/status", &st)
	var evs api.EventsResponse
	if err == nil {
		h.mu.Lock()
		since := ps.nextEvent
		h.mu.Unlock()
		err = h.getJSON(ctx, base+"/api/events?limit=1000&since="+strconv.FormatUint(since, 10), &evs)
	}
	now := time.Now()
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		ps.err = err.Error()
		return
	}
	ps.err, ps.lastOK, ps.role = "", now, st.Role
	if s := st.Status; s != nil {
		if !ps.prevAt.IsZero() {
			dt := now.Sub(ps.prevAt).Seconds()
			ps.downBps = float64(s.BytesDown-ps.prevDown) / dt
			ps.upBps = float64(s.BytesUp-ps.prevUp) / dt
			for _, p := range s.Peers {
				if prev, ok := ps.prevFrom[p.ID]; ok {
					ps.linkBps[p.ID] = float64(p.BytesFrom-prev) / dt
				}
			}
		}
		ps.prevAt, ps.prevDown, ps.prevUp = now, s.BytesDown, s.BytesUp
		clear(ps.prevFrom)
		for _, p := range s.Peers {
			ps.prevFrom[p.ID] = p.BytesFrom
		}
		ps.status = s
	}
	for _, e := range evs.Events {
		h.events = append(h.events, Event{Time: e.Time, Peer: ps.id, Type: e.Type, Message: e.Message})
	}
	if len(h.events) > maxEvents {
		h.events = append([]Event(nil), h.events[len(h.events)-maxEvents:]...)
	}
	ps.nextEvent = evs.Next
}

func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sd", h.handleSD)
	mux.HandleFunc("GET /api/peers", h.handlePeers)
	mux.HandleFunc("GET /api/topology/nodes", h.handleNodes)
	mux.HandleFunc("GET /api/topology/edges", h.handleEdges)
	mux.HandleFunc("GET /api/events", h.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(v)
}

func (h *Hub) sortedPeers() []*peerState {
	out := make([]*peerState, 0, len(h.peers))
	for _, ps := range h.peers {
		out = append(out, ps)
	}
	slices.SortFunc(out, func(a, b *peerState) int { return cmp.Compare(a.id, b.id) })
	return out
}

func (ps *peerState) state(now time.Time) string {
	switch {
	case now.Sub(ps.lastListed) > goneAfter:
		return "gone" // no longer announced to the tracker
	case ps.err != "":
		return "unreachable"
	case ps.lastOK.IsZero():
		return "pending"
	}
	return "online"
}

type sdTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

func (h *Hub) handleSD(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []sdTarget{}
	for _, ps := range h.sortedPeers() {
		if ps.apiAddr != "" && ps.state(time.Now()) != "gone" {
			out = append(out, sdTarget{Targets: []string{ps.apiAddr}, Labels: map[string]string{"job": "swarmflow", "peer_role": ps.role}})
		}
	}
	writeJSON(w, out)
}

// PeerRow is one line of /api/peers.
type PeerRow struct {
	ID          string    `json:"id"`
	Swarm       string    `json:"swarm"`
	Role        string    `json:"role"`
	State       string    `json:"state"`
	Addr        string    `json:"addr"`
	ProgressPct float64   `json:"progress_pct"`
	DownMiBps   float64   `json:"down_mibps"`
	UpMiBps     float64   `json:"up_mibps"`
	Connections int       `json:"connections"`
	Outstanding int       `json:"outstanding"`
	Timeouts    int       `json:"timeouts"`
	Rejected    int       `json:"rejected_pieces"`
	Unavailable int       `json:"unavailable_pieces"`
	UnderRepl   int       `json:"under_replicated"`
	Endgame     bool      `json:"endgame"`
	LastSeen    time.Time `json:"last_seen"`
	Error       string    `json:"error,omitempty"`
}

func (h *Hub) handlePeers(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	rows := []PeerRow{}
	for _, ps := range h.sortedPeers() {
		r := PeerRow{ID: ps.id, Swarm: ps.swarm, Role: ps.role, State: ps.state(now), Addr: ps.addr,
			DownMiBps: ps.downBps / (1 << 20), UpMiBps: ps.upBps / (1 << 20), LastSeen: ps.lastOK, Error: ps.err}
		if s := ps.status; s != nil {
			r.ProgressPct = 100 * float64(s.Have) / float64(max(s.Pieces, 1))
			r.Connections, r.Outstanding, r.Timeouts, r.Rejected = len(s.Peers), s.Outstanding, s.Counters.Timeouts, s.Rejected
			r.Unavailable, r.UnderRepl, r.Endgame = s.Unavailable, s.Replication.UnderReplicated, s.Endgame
		}
		rows = append(rows, r)
	}
	writeJSON(w, rows)
}

// Node and edge rows follow Grafana's Node Graph field conventions.
type Node struct {
	ID        string  `json:"id"`
	Title     string  `json:"title"`
	Subtitle  string  `json:"subtitle"`
	MainStat  string  `json:"mainstat"`
	Secondary string  `json:"secondarystat"`
	ArcDone   float64 `json:"arc__done"`
	ArcLeft   float64 `json:"arc__left"`
}

type Edge struct {
	ID        string `json:"id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	MainStat  string `json:"mainstat"`
	Secondary string `json:"secondarystat"`
}

func rate(bps float64) string { return fmt.Sprintf("%.2f MiB/s", bps/(1<<20)) }

func (h *Hub) handleNodes(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	nodes := []Node{}
	seen := map[string]bool{}
	for _, ps := range h.sortedPeers() {
		seen[ps.id] = true
		n := Node{ID: ps.id, Title: ps.id, Subtitle: ps.role + " · " + ps.state(now)}
		if s := ps.status; s != nil {
			done := float64(s.Have) / float64(max(s.Pieces, 1))
			n.MainStat = fmt.Sprintf("%.0f%%", 100*done)
			n.Secondary = "↓ " + rate(ps.downBps) + "  ↑ " + rate(ps.upBps)
			n.ArcDone, n.ArcLeft = done, 1-done
		} else {
			n.ArcLeft = 1
		}
		nodes = append(nodes, n)
	}
	// Remote peers that monitored peers report but the hub cannot reach.
	for _, ps := range h.sortedPeers() {
		if ps.status == nil || ps.state(now) != "online" {
			continue
		}
		for _, p := range ps.status.Peers {
			if !seen[p.ID] {
				seen[p.ID] = true
				nodes = append(nodes, Node{ID: p.ID, Title: p.ID, Subtitle: "not monitored", ArcLeft: 1})
			}
		}
	}
	writeJSON(w, nodes)
}

func (h *Hub) handleEdges(w http.ResponseWriter, _ *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := time.Now()
	type pair struct{ a, b string }
	type agg struct {
		reporters  map[string]bool
		aToB, bToA float64
	}
	edges := map[pair]*agg{}
	for _, ps := range h.sortedPeers() {
		if ps.status == nil || ps.state(now) != "online" {
			continue // a connection is only shown while a reporting peer is reachable
		}
		for _, p := range ps.status.Peers {
			k := pair{min(ps.id, p.ID), max(ps.id, p.ID)}
			e := edges[k]
			if e == nil {
				e = &agg{reporters: map[string]bool{}}
				edges[k] = e
			}
			e.reporters[ps.id] = true
			// linkBps is what ps receives from p, i.e. data flowing p → ps.
			if p.ID == k.a {
				e.aToB += ps.linkBps[p.ID]
			} else {
				e.bToA += ps.linkBps[p.ID]
			}
		}
	}
	out := []Edge{}
	for k, e := range edges {
		seenBy := "reported by one side"
		if len(e.reporters) == 2 {
			seenBy = "reported by both sides"
		}
		out = append(out, Edge{ID: k.a + "-" + k.b, Source: k.a, Target: k.b, MainStat: rate(e.aToB + e.bToA),
			Secondary: fmt.Sprintf("%s→%s %s, %s→%s %s; %s", k.a, k.b, rate(e.aToB), k.b, k.a, rate(e.bToA), seenBy)})
	}
	slices.SortFunc(out, func(x, y Edge) int { return cmp.Compare(x.ID, y.ID) })
	writeJSON(w, out)
}

func (h *Hub) handleEvents(w http.ResponseWriter, r *http.Request) {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		limit = 200
	}
	types := map[string]bool{}
	for _, t := range strings.Split(r.URL.Query().Get("type"), ",") {
		if t != "" {
			types[t] = true
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	out := []Event{}
	for i := len(h.events) - 1; i >= 0 && len(out) < limit; i-- {
		if len(types) == 0 || types[h.events[i].Type] {
			out = append(out, h.events[i])
		}
	}
	writeJSON(w, out)
}
