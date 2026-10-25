# Monitoring: metrics, events and the Grafana dashboard

```
peer ──/metrics, /api/status, /api/events──▶ swarmflow-hub ──/sd──▶ Prometheus ──▶ Grafana
  ▲ (one HTTP API per peer, 127.0.0.1)         │ polls 1 s              (2 s scrapes)    ▲
  └── tracker /peers lists peers + API addresses┘──── topology, peers, events (Infinity) ┘
```

Each component runs as its own process, and peers never depend on the monitoring stack:
- If the hub, Prometheus or Grafana stops, transfers continue.
- If a peer's API fails, the failure is logged and the transfer continues.

## Quick start (Windows)

1. Install Prometheus 3.15.0, Grafana OSS 13.2.3 and the Infinity datasource plugin 4.1.1 under
   `D:\DevTools`:
   - Prometheus and Grafana are the official zips, extracted as `prometheus-3.15.0.windows-amd64`
     and `grafana-13.2.3`.
   - Install the plugin with:

     ```powershell
     D:\DevTools\grafana-13.2.3\bin\grafana.exe cli --homepath D:\DevTools\grafana-13.2.3 `
       --pluginsDir D:\DevTools\monitoring\grafana\plugins plugins install yesoreyeram-infinity-datasource 4.1.1
     ```

   For another location, pass `-Tools <dir>` to the scripts.
2. Run `./scripts/demo-dashboard.ps1`. It starts the hub, Prometheus, Grafana, a tracker and 7
   peers, then kills the fastest seed and adds a replacement.
3. Open <http://127.0.0.1:3000/d/swarmflow-overview>. Anonymous viewers have read-only access;
   the admin login is `admin` / `admin`, local only.
4. Stop everything with `./scripts/demo-dashboard.ps1 -Stop`.

Use `./scripts/monitoring.ps1` (and `-Stop`) to run the monitoring stack on its own, against peers
you start yourself with `-tracker http://127.0.0.1:7000`.

## Peer API (`-api-listen`, default `127.0.0.1:0`; empty disables)

The API port is announced to the tracker, so the hub can find each peer.

| endpoint | content |
|---|---|
| `GET /metrics` | Prometheus metrics (below) plus Go runtime and process metrics |
| `GET /api/status` | Status snapshot: pieces, counters, per-peer measurements, sources, replication |
| `GET /api/events?since=N&limit=M` | Explained events, oldest first. `next` is the cursor for the next call; `dropped` counts events overwritten in the 10,000-entry ring |
| `GET /healthz` | Liveness |

Responses contain peer IDs, network addresses, counters and measurements. They never contain
file contents or local file-system paths.

## Hub (`bin/hub -tracker URL -listen 127.0.0.1:7100`)

| endpoint | content |
|---|---|
| `GET /sd` | Prometheus HTTP service discovery: API addresses of peers the tracker lists |
| `GET /api/peers` | One row per peer: state (`online`, `unreachable` = listed but its API does not answer, `gone` = no longer announced), progress, rates measured between polls, counters |
| `GET /api/topology/nodes`, `/edges` | Grafana Node Graph frames (see below) |
| `GET /api/events?limit=N&type=a,b` | Merged event log from all peers, newest first |

**Topology rules:**
- An edge is drawn only for a connection that a reachable peer reports, and it is marked as
  reported by one or both sides.
- Peers that others report but the hub cannot reach appear as "not monitored" nodes.
- No links are inferred.

## Metrics

All metrics carry constant labels `peer` (local peer ID) and `swarm`. Per-connection metrics also
carry `remote`. There are no per-piece labels, so cardinality is bounded by the number of
connected peers (at most 50).

**Naming conventions:**
- `_total` metrics are cumulative counters since the process started.
- `_ewma` metrics are exponentially smoothed estimates (weight 0.3 per sample).
- Everything else is an instantaneous value.

| metric | type | meaning |
|---|---|---|
| `swarmflow_pieces_total`, `swarmflow_pieces_have` | gauge | Pieces in the file; pieces verified and persisted |
| `swarmflow_bytes_received_total`, `swarmflow_bytes_sent_total` | counter | Payload bytes in accepted pieces; payload bytes uploaded |
| `swarmflow_requests_total`, `swarmflow_request_timeouts_total`, `swarmflow_requests_reassigned_total` | counter | Piece requests sent; requests that missed their deadline; requests moved after a disconnect |
| `swarmflow_rejected_pieces_total`, `swarmflow_duplicate_pieces_total`, `swarmflow_endgame_requests_total` | counter | Pieces failing verification; redundant copies; endgame duplicate requests |
| `swarmflow_busy_sent_total`, `swarmflow_busy_received_total`, `swarmflow_peers_banned_total` | counter | Backpressure sent and received (decision 0003); bans |
| `swarmflow_request_duration_seconds{outcome}` | histogram | Request to outcome (`ok`, `timeout`, `busy`, `rejected`, `invalid`) |
| `swarmflow_outstanding_requests`, `swarmflow_verifying_pieces`, `swarmflow_upload_queue`, `swarmflow_connections` | gauge | Current load |
| `swarmflow_unavailable_pieces`, `swarmflow_endgame`, `swarmflow_complete`, `swarmflow_download_elapsed_seconds` | gauge | Pieces with no source; endgame flag; completion flag; elapsed time |
| `swarmflow_peer_throughput_bytes_per_second_ewma{remote}` | gauge | Smoothed delivery rate from a peer: application goodput, not link capacity |
| `swarmflow_peer_rtt_seconds_ewma{remote}` | gauge | PING/PONG round-trip time on the data connection, including queueing behind data |
| `swarmflow_peer_reliability_ewma{remote}`, `swarmflow_peer_cooldown{remote}` | gauge | Success ratio; 1 while the peer is deprioritised after repeated timeouts |
| `swarmflow_peer_request_latency_p50_seconds`, `_p95_seconds{remote}` | gauge | Over the peer's last 200 requests |
| `swarmflow_peer_outstanding_requests{remote}`, `swarmflow_peer_pieces{remote}` | gauge | Requests in flight to the peer; pieces it announced |
| `swarmflow_source_bytes_total{remote}` | counter | Useful bytes received from each peer, including peers since disconnected |
| `swarmflow_replication_*` | mixed | Target, under-replicated pieces, min/mean verified copies, proofs ok/failed, offers sent |

## Events

| type | when (the message gives the measured reason) |
|---|---|
| `piece_scheduled` | Every request: the picker's reason (e.g. "rarest-first: held by 1 of 4 connected peers"); the selector's reason with the chosen peer's measured throughput, RTT, queue and reliability; the runner-up; peers skipped for busy back-off or cooldown |
| `endgame_entered`, `endgame_duplicate` | Endgame starts; an overdue request is duplicated (how long it waited vs. its expected time) |
| `peer_connected`, `peer_disconnected` | With direction, the reason, and how many requests were reassigned |
| `request_timeout`, `peer_cooldown`, `piece_rejected`, `peer_banned` | Failures and the health actions they trigger |
| `sources_lost`, `sources_restored` | Needed pieces lost and regained all connected sources |
| `under_replicated`, `replication_target_reached`, `proof_failed` | Replication state changes; a peer failing a possession proof |
| `download_complete` | Size, time, average rate, rejected pieces, endgame duplicates |

The explanation is generated from the candidates and measurements the selector actually used
(`scheduler.Explain`). For adaptive, a choice that does not minimise cost can only come from
exploration, and is labelled as such.

## Dashboard panels

- **Overview stats:** peers scraped; download and upload rates over 10 s; timeouts over 1 min;
  pieces with no source; under-replicated pieces.
- **Peers table** (hub).
- **Topology node graph:** observed connections only, with node arcs showing completion.
- **Per-peer progress**, and per-peer download rate over 10 s.
- **Per-source measurements:** delivery-rate EWMA (the selector's input), and application RTT
  EWMA.
- **Request latency** p50/p95 from the histogram over 30 s, and request outcomes per second.
- **Load and health:** requests in flight and upload queues; connections and peers in
  cooldown.
- **Event tables:** scheduler decisions; failures, recovery and replication events.
- **Replication:** verified copies elsewhere (min/mean), and possession proofs per second.

## Verified on 2026-10-25 (`scripts/demo-dashboard.ps1`)

- **Discovery:** Prometheus discovered every peer via the hub (6 up, 1 down after the kill).
- **Grafana:** health ok; the dashboard and both datasources were provisioned. Infinity queries
  for peers, nodes, edges and events, and Prometheus queries, all returned data through
  Grafana's query API.
- **Topology:** 15 edges for 6 online peers (a full mesh). The killed seed's edges disappeared,
  its row went `unreachable`, then `gone`.
- **Recovery:** each downloader logged "4 outstanding requests reassigned … forcibly closed by
  the remote host" for the killed seed, and all three downloads completed.

## Limits

- **Rates have two sources.** Rates in the hub table are measured between polls, about 1 s
  apart. Prometheus rates use 10 s windows.
- **Local only.** Prometheus and Grafana run on this machine. Peers inside the WSL netem lab's
  namespaces are not reachable from them; the lab writes its own reports.
- **Grafana's Node Graph needs a recent browser,** and the topology refreshes with the dashboard
  every 2 s.
