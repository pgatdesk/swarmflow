// Package discovery implements the bootstrap tracker and its client. The
// tracker only maps swarm IDs to recently announced peer addresses; it never
// sees file data. Peers that already know each other keep working without it.
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

const (
	EventStarted = "started"
	EventStopped = "stopped"

	maxAnnounceBody = 4 << 10
	maxReturned     = 50
)

type AnnounceRequest struct {
	SwarmID string `json:"swarm_id"`
	PeerID  string `json:"peer_id"`
	Port    uint16 `json:"port"`
	// IP overrides the address the tracker observes. Used when a peer is
	// reachable at a different address (for example behind the lab's fault proxy).
	IP      string `json:"ip,omitempty"`
	APIPort uint16 `json:"api_port,omitempty"`
	Event   string `json:"event,omitempty"`
}

type PeerInfo struct {
	PeerID  string `json:"peer_id"`
	Addr    string `json:"addr"`
	APIAddr string `json:"api_addr,omitempty"`
}

type AnnounceResponse struct {
	IntervalSec int        `json:"interval"`
	Peers       []PeerInfo `json:"peers"`
}

type entry struct {
	info PeerInfo
	seen time.Time
}

// Tracker is an in-memory peer registry with expiry.
type Tracker struct {
	TTL      time.Duration
	Interval time.Duration
	log      *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	swarms map[protocol.SwarmID]map[protocol.PeerID]*entry
}

func NewTracker(log *slog.Logger) *Tracker {
	return &Tracker{TTL: 90 * time.Second, Interval: 30 * time.Second, log: log, now: time.Now,
		swarms: make(map[protocol.SwarmID]map[protocol.PeerID]*entry)}
}

func (t *Tracker) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /announce", t.handleAnnounce)
	mux.HandleFunc("GET /peers", t.handlePeers)
	return mux
}

func (t *Tracker) handleAnnounce(w http.ResponseWriter, r *http.Request) {
	var req AnnounceRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAnnounceBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad announce: "+err.Error(), http.StatusBadRequest)
		return
	}
	swarm, err := protocol.ParseSwarmID(req.SwarmID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	peer, err := protocol.ParsePeerID(req.PeerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.Port == 0 {
		http.Error(w, "port must be nonzero", http.StatusBadRequest)
		return
	}
	ip, err := announcedIP(r, req.IP)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	info := PeerInfo{PeerID: req.PeerID, Addr: netip.AddrPortFrom(ip, req.Port).String()}
	if req.APIPort != 0 {
		info.APIAddr = netip.AddrPortFrom(ip, req.APIPort).String()
	}

	now := t.now()
	t.mu.Lock()
	t.expireLocked(now)
	peers := t.swarms[swarm]
	if req.Event == EventStopped {
		delete(peers, peer)
	} else {
		if peers == nil {
			peers = make(map[protocol.PeerID]*entry)
			t.swarms[swarm] = peers
		}
		if _, known := peers[peer]; !known {
			t.log.Info("peer joined", "swarm", swarm.Short(), "peer", peer.Short(), "addr", info.Addr)
		}
		peers[peer] = &entry{info: info, seen: now}
	}
	resp := AnnounceResponse{IntervalSec: int(t.Interval / time.Second), Peers: []PeerInfo{}}
	for id, e := range peers {
		if id != peer {
			resp.Peers = append(resp.Peers, e.info)
		}
	}
	t.mu.Unlock()

	if req.Event == EventStopped {
		t.log.Info("peer left", "swarm", swarm.Short(), "peer", peer.Short())
	}
	rand.Shuffle(len(resp.Peers), func(i, j int) { resp.Peers[i], resp.Peers[j] = resp.Peers[j], resp.Peers[i] })
	resp.Peers = resp.Peers[:min(len(resp.Peers), maxReturned)]
	writeJSON(w, resp)
}

// handlePeers lists every live peer, grouped by swarm. The monitoring hub uses it.
func (t *Tracker) handlePeers(w http.ResponseWriter, _ *http.Request) {
	t.mu.Lock()
	t.expireLocked(t.now())
	out := make(map[string][]PeerInfo, len(t.swarms))
	for s, peers := range t.swarms {
		for _, e := range peers {
			out[s.String()] = append(out[s.String()], e.info)
		}
	}
	t.mu.Unlock()
	writeJSON(w, out)
}

func (t *Tracker) expireLocked(now time.Time) {
	for s, peers := range t.swarms {
		for id, e := range peers {
			if now.Sub(e.seen) > t.TTL {
				delete(peers, id)
				t.log.Info("peer expired", "swarm", s.Short(), "peer", id.Short())
			}
		}
		if len(peers) == 0 {
			delete(t.swarms, s)
		}
	}
}

func announcedIP(r *http.Request, override string) (netip.Addr, error) {
	if override != "" {
		ip, err := netip.ParseAddr(override)
		if err != nil {
			return netip.Addr{}, fmt.Errorf("bad ip: %w", err)
		}
		return ip.Unmap(), nil
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return netip.Addr{}, err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, err
	}
	return ip.Unmap(), nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// Client announces to a tracker.
type Client struct {
	URL  string
	HTTP *http.Client
}

func NewClient(url string) *Client {
	return &Client{URL: url, HTTP: &http.Client{Timeout: 5 * time.Second}}
}

func (c *Client) Announce(ctx context.Context, req AnnounceRequest) (AnnounceResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return AnnounceResponse{}, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/announce", bytes.NewReader(body))
	if err != nil {
		return AnnounceResponse{}, err
	}
	hr.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(hr)
	if err != nil {
		return AnnounceResponse{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return AnnounceResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return AnnounceResponse{}, fmt.Errorf("tracker: %s: %s", resp.Status, bytes.TrimSpace(b))
	}
	var out AnnounceResponse
	if err := json.Unmarshal(b, &out); err != nil {
		return AnnounceResponse{}, fmt.Errorf("tracker: bad response: %w", err)
	}
	return out, nil
}
