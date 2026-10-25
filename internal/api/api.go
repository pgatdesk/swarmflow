// Package api serves a peer's local monitoring endpoints:
//
//	GET /metrics             Prometheus metrics
//	GET /api/status          the coordinator's status snapshot
//	GET /api/events?since=N  explained event log (limit=M, default 500, max 2000)
//	GET /healthz             liveness
//
// It runs on its own http.Server; a failure here never affects transfers.
// Responses contain peer IDs, addresses and counters, never file contents or
// local file-system paths.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pgatdesk/swarmflow/internal/coordinator"
	"github.com/pgatdesk/swarmflow/internal/events"
)

// StatusResponse is the body of /api/status.
type StatusResponse struct {
	Peer   string              `json:"peer"`
	Role   string              `json:"role"`
	Status *coordinator.Status `json:"status"`
}

// EventsResponse is the body of /api/events.
type EventsResponse struct {
	Peer    string         `json:"peer"`
	Events  []events.Event `json:"events"`
	Next    uint64         `json:"next"`
	Dropped uint64         `json:"dropped"`
}

type Server struct {
	Peer   string
	Role   string
	Coord  func() *coordinator.Coordinator // may return nil before the coordinator exists
	Events *events.Ring
	Reg    *prometheus.Registry
	Log    *slog.Logger
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.Reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		resp := StatusResponse{Peer: s.Peer, Role: s.Role}
		if co := s.Coord(); co != nil {
			resp.Status = co.Status()
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit <= 0 {
			limit = 500
		}
		evs, next := s.Events.Since(since, min(limit, 2000))
		_, dropped := s.Events.Stats()
		if evs == nil {
			evs = []events.Event{}
		}
		writeJSON(w, EventsResponse{Peer: s.Peer, Events: evs, Next: next, Dropped: dropped})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(v)
}

// Serve runs the API on ln until ctx is done.
func (s *Server) Serve(ctx context.Context, ln net.Listener) {
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.Log.Error("monitoring API stopped; transfers continue", "err", err)
	}
}
