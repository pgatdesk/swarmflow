// Package metrics exposes a coordinator's state in Prometheus format. Values
// are read from the coordinator's published status snapshot at scrape time,
// so metrics never add work to the transfer path. Labels are bounded: the
// local peer and swarm (constant), the remote peer ID (at most MaxPeers), and
// request outcome; there are no per-piece labels.
//
// Counters are cumulative since process start. Gauges ending in _ewma are
// exponentially smoothed estimates; others are instantaneous.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/pgatdesk/swarmflow/internal/coordinator"
)

type desc struct {
	d *prometheus.Desc
	t prometheus.ValueType
}

func newDesc(name, help string, t prometheus.ValueType, labels ...string) desc {
	return desc{prometheus.NewDesc("swarmflow_"+name, help, labels, nil), t}
}

type collector struct {
	co *coordinator.Coordinator
	ds map[string]desc
}

var swarmDescs = map[string]desc{
	"pieces_total":     newDesc("pieces_total", "Pieces in the file.", prometheus.GaugeValue),
	"pieces_have":      newDesc("pieces_have", "Pieces verified and persisted locally.", prometheus.GaugeValue),
	"bytes_received":   newDesc("bytes_received_total", "Payload bytes received in accepted pieces (excludes duplicates).", prometheus.CounterValue),
	"bytes_sent":       newDesc("bytes_sent_total", "Payload bytes uploaded.", prometheus.CounterValue),
	"requests":         newDesc("requests_total", "Piece requests sent.", prometheus.CounterValue),
	"timeouts":         newDesc("request_timeouts_total", "Piece requests that missed their deadline.", prometheus.CounterValue),
	"reassigned":       newDesc("requests_reassigned_total", "Outstanding requests reassigned after a peer disconnected.", prometheus.CounterValue),
	"rejected":         newDesc("rejected_pieces_total", "Received pieces rejected by verification.", prometheus.CounterValue),
	"duplicates":       newDesc("duplicate_pieces_total", "Received copies of pieces already verifying or persisted.", prometheus.CounterValue),
	"endgame_requests": newDesc("endgame_requests_total", "Duplicate requests issued in endgame.", prometheus.CounterValue),
	"busy_sent":        newDesc("busy_sent_total", "REJECT(busy) sent because our upload queue was full.", prometheus.CounterValue),
	"busy_received":    newDesc("busy_received_total", "REJECT(busy) received from peers.", prometheus.CounterValue),
	"banned":           newDesc("peers_banned_total", "Peers banned for invalid data.", prometheus.CounterValue),
	"outstanding":      newDesc("outstanding_requests", "Requests in flight now.", prometheus.GaugeValue),
	"verifying":        newDesc("verifying_pieces", "Pieces being hashed and written now.", prometheus.GaugeValue),
	"upload_queue":     newDesc("upload_queue", "Uploads queued or in progress now, all peers.", prometheus.GaugeValue),
	"connections":      newDesc("connections", "Connected peers now.", prometheus.GaugeValue),
	"unavailable":      newDesc("unavailable_pieces", "Needed pieces with no connected source now.", prometheus.GaugeValue),
	"endgame":          newDesc("endgame", "1 while in endgame mode.", prometheus.GaugeValue),
	"complete":         newDesc("complete", "1 once the file is complete locally.", prometheus.GaugeValue),
	"elapsed":          newDesc("download_elapsed_seconds", "Time from start to completion (or until now if incomplete).", prometheus.GaugeValue),
	"repl_target":      newDesc("replication_target_copies", "Target copies per piece including ours (0 = off).", prometheus.GaugeValue),
	"repl_under":       newDesc("replication_under_replicated_pieces", "Held pieces below the target at the last sweep.", prometheus.GaugeValue),
	"repl_min":         newDesc("replication_min_verified_copies", "Fewest verified copies elsewhere of any held piece.", prometheus.GaugeValue),
	"repl_mean":        newDesc("replication_mean_verified_copies", "Mean verified copies elsewhere per held piece.", prometheus.GaugeValue),
	"proofs_ok":        newDesc("replication_proofs_ok_total", "Possession proofs that verified.", prometheus.CounterValue),
	"proofs_failed":    newDesc("replication_proofs_failed_total", "Possession proofs that did not verify.", prometheus.CounterValue),
	"offers_sent":      newDesc("replication_offers_sent_total", "Replica offers sent.", prometheus.CounterValue),
	"replica_bytes":    newDesc("replica_bytes_accepted", "Bytes accepted to hold as replicas.", prometheus.GaugeValue),
	"peer_rtt":         newDesc("peer_rtt_seconds_ewma", "Smoothed application RTT (PING/PONG on the data connection).", prometheus.GaugeValue, "remote"),
	"peer_throughput":  newDesc("peer_throughput_bytes_per_second_ewma", "Smoothed payload delivery rate from the peer.", prometheus.GaugeValue, "remote"),
	"peer_reliability": newDesc("peer_reliability_ewma", "Smoothed fraction of successful requests.", prometheus.GaugeValue, "remote"),
	"peer_latency_p50": newDesc("peer_request_latency_p50_seconds", "Median latency of the peer's last 200 requests.", prometheus.GaugeValue, "remote"),
	"peer_latency_p95": newDesc("peer_request_latency_p95_seconds", "95th percentile latency of the peer's last 200 requests.", prometheus.GaugeValue, "remote"),
	"peer_outstanding": newDesc("peer_outstanding_requests", "Requests in flight to the peer now.", prometheus.GaugeValue, "remote"),
	"peer_cooldown":    newDesc("peer_cooldown", "1 while the peer is deprioritised after repeated timeouts.", prometheus.GaugeValue, "remote"),
	"peer_has":         newDesc("peer_pieces", "Pieces the peer has announced.", prometheus.GaugeValue, "remote"),
	"source_bytes":     newDesc("source_bytes_total", "Useful bytes received from each peer, including peers since disconnected.", prometheus.CounterValue, "remote"),
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.ds {
		ch <- d.d
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	s := c.co.Status()
	if s == nil {
		return
	}
	put := func(key string, v float64, labels ...string) {
		d := c.ds[key]
		ch <- prometheus.MustNewConstMetric(d.d, d.t, v, labels...)
	}
	put("pieces_total", float64(s.Pieces))
	put("pieces_have", float64(s.Have))
	put("bytes_received", float64(s.BytesDown))
	put("bytes_sent", float64(s.BytesUp))
	put("requests", float64(s.Counters.Requests))
	put("timeouts", float64(s.Counters.Timeouts))
	put("reassigned", float64(s.Counters.Reassigned))
	put("rejected", float64(s.Counters.Rejected))
	put("duplicates", float64(s.Counters.DuplicatePieces))
	put("endgame_requests", float64(s.Counters.EndgameRequests))
	put("busy_sent", float64(s.Counters.BusySent))
	put("busy_received", float64(s.Counters.BusyReceived))
	put("banned", float64(s.Counters.Banned))
	put("outstanding", float64(s.Outstanding))
	put("verifying", float64(s.Verifying))
	put("upload_queue", float64(s.UploadQueue))
	put("connections", float64(len(s.Peers)))
	put("unavailable", float64(s.Unavailable))
	put("endgame", b2f(s.Endgame))
	put("complete", b2f(s.Complete))
	end := s.FinishedAt
	if end.IsZero() {
		end = time.Now()
	}
	if !s.StartedAt.IsZero() {
		put("elapsed", end.Sub(s.StartedAt).Seconds())
	}
	r := s.Replication
	put("repl_target", float64(r.Target))
	put("repl_under", float64(r.UnderReplicated))
	put("repl_min", float64(r.MinVerified))
	put("repl_mean", r.MeanVerified)
	put("proofs_ok", float64(r.Counters.ProofsOK))
	put("proofs_failed", float64(r.Counters.ProofsFailed))
	put("offers_sent", float64(r.Counters.OffersSent))
	put("replica_bytes", float64(r.ReplicaBytes))
	for _, p := range s.Peers {
		if p.RTTKnown {
			put("peer_rtt", p.RTTMillis/1000, p.ID)
		}
		if p.ThroughputKnown {
			put("peer_throughput", p.ThroughputBps, p.ID)
		}
		put("peer_reliability", p.Reliability, p.ID)
		if p.LatencyP95ms > 0 {
			put("peer_latency_p50", p.LatencyP50ms/1000, p.ID)
			put("peer_latency_p95", p.LatencyP95ms/1000, p.ID)
		}
		put("peer_outstanding", float64(p.Outstanding), p.ID)
		put("peer_cooldown", b2f(p.Cooldown), p.ID)
		put("peer_has", float64(p.Has), p.ID)
	}
	for _, src := range s.Sources {
		put("source_bytes", float64(src.Bytes), src.ID)
	}
}

// RequestObserver records request latency by outcome; it implements coordinator.Observer.
type RequestObserver struct{ h *prometheus.HistogramVec }

func (o *RequestObserver) RequestDone(outcome string, d time.Duration) {
	o.h.WithLabelValues(outcome).Observe(d.Seconds())
}

// NewRegistry returns a registry exposing co's metrics, labelled with the
// local peer and swarm, plus Go runtime and process metrics, and the request
// observer to install as the coordinator's Observer before it runs.
func NewRegistry(peer, swarm string, co func() *coordinator.Coordinator) (*prometheus.Registry, *RequestObserver) {
	reg := prometheus.NewRegistry()
	labels := prometheus.Labels{"peer": peer, "swarm": swarm}
	wrapped := prometheus.WrapRegistererWith(labels, reg)
	obs := &RequestObserver{h: prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "swarmflow_request_duration_seconds",
		Help:    "Time from sending a piece request to its outcome (piece received, timeout, or reject).",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
	}, []string{"outcome"})}
	wrapped.MustRegister(obs.h)
	wrapped.MustRegister(&lazyCollector{get: co})
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg, obs
}

// lazyCollector resolves the coordinator at scrape time, because the
// registry (and its observer) must exist before the coordinator is created.
type lazyCollector struct {
	get func() *coordinator.Coordinator
}

func (l *lazyCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range swarmDescs {
		ch <- d.d
	}
}

func (l *lazyCollector) Collect(ch chan<- prometheus.Metric) {
	if co := l.get(); co != nil {
		(&collector{co: co, ds: swarmDescs}).Collect(ch)
	}
}
