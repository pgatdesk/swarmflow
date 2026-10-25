# SwarmFlow — Phase 1 Architecture (proposal, pending approval)

Status: **Approved 2026-10-25.** This is the design; see the README and benchmark report for what is implemented and verified.

Approved inputs from Phase 0 are recorded in [`docs/decisions/0001-phase0-decisions.md`](../decisions/0001-phase0-decisions.md).

---

## 1. Component diagram

```
                         ┌──────────────────────────── swarmflow peer (one OS process) ────────────────────────────┐
                         │                                                                                         │
   CLI / config ───────▶ │  Node (lifecycle, config, graceful shutdown via context cancellation)                   │
                         │    │                                                                                    │
                         │    ├── Discovery ──── HTTP announce ─────────────────────────▶  swarmflow-tracker       │
                         │    │      └─ PEX (peer exchange over the wire protocol)            (bootstrap only)      │
                         │    │                                                                                    │
                         │    ├── ConnManager ── per-connection reader + writer goroutines                         │
                         │    │      └─ protocol codec (framing, handshake, size limits)  ◀──── TCP ────▶ peers    │
                         │    │                                                                                    │
                         │    ├── Swarm (one per file)                                                             │
                         │    │     ├─ Coordinator  (single goroutine; sole owner of piece state)                  │
                         │    │     │     ├─ scheduler.PiecePicker   (pure, no I/O)                                │
                         │    │     │     ├─ scheduler.PeerSelector  (pure, no I/O)                                │
                         │    │     │     └─ peerstats.Estimator     (EWMA / rolling windows)                      │
                         │    │     ├─ Uploader     (serves REQUESTs, bounded upload slots)                        │
                         │    │     ├─ Replicator   (verified replica tracking, push offers)                       │
                         │    │     ├─ PieceStore   (preallocated data file, positioned writes, fsync)             │
                         │    │     └─ Verifier     (SHA-256 worker pool, off the coordinator goroutine)           │
                         │    │                                                                                    │
                         │    ├── StateDB (SQLite, WAL mode, transactional piece-state records)                    │
                         │    ├── Events (bounded ring buffer + structured slog JSON log)                          │
                         │    └── API  (HTTP: /metrics Prometheus, /api/status, /api/events, /api/topology)        │
                         └─────────────────────────────────────────────────────────────────────────────────────────┘

   swarmflow-hub  ── polls every peer's /api/* ──▶ aggregated topology + event feed  ──▶ Grafana (Infinity datasource)
                  └─ /sd  (Prometheus HTTP service discovery: list of live peers)  ──▶ Prometheus ──▶ Grafana
   swarmlab       ── spawns tracker / peers / faultproxies as subprocesses, runs scenarios, writes results
   faultproxy     ── TCP proxy in front of each peer: delay, jitter, bandwidth cap, stall, reset, byte corruption
```

All file data flows peer-to-peer over TCP. The tracker only hands out addresses; the hub and
Prometheus only read metrics. Killing the tracker, hub, Prometheus or Grafana must not stop a
transfer between peers that already know each other (tested).

## 2. Data flow (one download)

1. `swarmflow get file.swarm` loads the manifest, computes the swarm ID, opens/creates StateDB rows
   and the `.part` data file, and (on resume) re-verifies previously persisted pieces.
2. Discovery announces to the tracker, receives a peer list; ConnManager dials up to `max_peers`.
3. Handshake (version + swarm ID check), then both sides exchange `BITFIELD`.
4. Coordinator loop, on every event (bitfield, HAVE, piece arrival, timeout, disconnect, tick):
   - PiecePicker chooses pieces; PeerSelector chooses a source for each; the decision and its
     factors are emitted as an event.
   - `REQUEST` is queued on the chosen connection (bounded send queue).
5. `PIECE` arrives → handed to the Verifier pool → result returns to the Coordinator.
   - Hash mismatch → piece back to Missing, peer charged an invalid-response penalty.
   - Hash match → PieceStore writes at offset, `fsync`, then StateDB commits `persisted`;
     only then the Coordinator sends `HAVE` to all peers and cancels duplicate requests.
6. All pieces persisted → final full-file hash check → rename `.part` to final name → seed.

## 3. Wire protocol v1.0

Byte order: big-endian. Every message after the handshake is one frame:

```
u32 length   (length of type + payload; must be ≥ 1 and ≤ MaxFrame = MaxPieceSize (1 MiB) + 64)
u8  type
... payload
```

Handshake (fixed 64 bytes each direction, 5 s deadline):

```
"SWFL" magic (4) | major u8 | minor u8 | flags u16 | swarm_id [32] | peer_id [16] | listen_port u16 | reserved [6]
```

Rules:
- The initiator sends its handshake first.
- The responder rejects by replying with its own handshake (swarm ID all zeros) followed by
  `BYE(reason)`, then closes. Reasons: different major version → `incompatible_version`;
  unknown swarm → `unknown_swarm`; same peer ID → `self`.
- Unknown minor-version features are ignored.
- The first frame on each side after the handshake must be `BITFIELD`, even when empty.
- One TCP connection carries one swarm.
- Simultaneous connections between the same two peers keep the one initiated by the peer with
  the lower peer ID. Both sides apply the same rule, so they agree.

| Type | Name | Payload |
|------|------|---------|
| 0x01 | BITFIELD | ⌈pieces/8⌉ bytes, MSB-first; trailing bits must be zero |
| 0x02 | HAVE | piece u32 |
| 0x03 | REQUEST | req_id u32, piece u32 |
| 0x04 | PIECE | req_id u32, piece u32, data (exactly the piece length) |
| 0x05 | CANCEL | req_id u32 |
| 0x06 | REJECT | req_id u32, reason u8 (not_have, busy, shutting_down) |
| 0x07 | PING | nonce u64 |
| 0x08 | PONG | nonce u64 |
| 0x09 | PEX | count u8 (≤ 50), then entries of {ip 16 bytes (IPv4-mapped), port u16} |
| 0x0A | CHALLENGE | piece u32, nonce [16] |
| 0x0B | PROOF | piece u32, nonce [16], digest [32] = SHA-256(nonce ‖ piece data) |
| 0x0C | REPL_OFFER | piece u32 |
| 0x0D | REPL_REPLY | piece u32, accept u8 |
| 0x0E | KEEPALIVE | — |
| 0x0F | BYE | reason u8 |

Every field is validated (piece index < count, lengths exact, no unknown type → protocol error
→ close with reason). The codec is fuzz-tested with Go's built-in fuzzer.

## 4. Manifest (trusted metadata)

Canonical JSON (sorted keys, no whitespace): `{format_version, name, size, piece_size,
piece_hashes[hex]}`. `swarm_id = SHA-256(canonical bytes)`. Whoever holds the manifest (or just
the swarm ID plus a manifest fetched from any peer/tracker and checked against that ID) can verify
every piece without trusting any peer. Default piece size: 256 KiB (configurable, power of two).

## 5. State model

Per piece, on the downloading side (owned exclusively by the Coordinator goroutine):

```
Missing ──pick──▶ Requested(k in-flight, k ≥ 1) ──first PIECE──▶ Verifying ──ok──▶ Writing ──fsync+commit──▶ Persisted
   ▲                     │ timeout/reject/disconnect (k→0)        │ bad hash                     │ I/O error
   └─────────────────────┴────────────────────────────────────────┴──────────────────────────────┘ (Failed → backoff → Missing)
```

- "Received" and "Verified" are the transient Verifying/Writing steps; only **Persisted** is
  announced via `HAVE`, served to others, or counted as a replica.
- Endgame duplicates: while a piece is Verifying/Writing, any further copies are discarded and
  their requests cancelled — exactly one write per piece.
- After N (default 5) consecutive failures a piece enters `Failed` with exponential backoff; it is
  never abandoned while any source exists.
- Piece with zero sources: `Unavailable` (derived, shown on dashboard); the download stays
  incomplete (not failed) and discovery keeps searching. Optional `--give-up-after`.

Per (piece, remote peer) replica state, owned by the Replicator:
`Unknown → Claimed (BITFIELD/HAVE) → Challenged → VerifiedReplica (proof ok, TTL 60 s) | ProofFailed`,
and `Offered → Accepted → (remote downloads normally) → Claimed → … → VerifiedReplica`.

## 6. Persistence and crash recovery

- Data: `<name>.part`, preallocated to full size, positioned writes (`WriteAt`) per piece.
- Metadata: SQLite (pure-Go driver `modernc.org/sqlite`, no CGO), WAL mode,
  `synchronous=FULL`. Tables: `swarms`, `pieces(swarm_id, idx, state, sha256, updated_at)`,
  `transfers(status, dest, started_at, …)`, `peers_seen` (for re-discovery hints).
- Ordering invariant: **data `WriteAt` → `File.Sync()` → SQLite transaction marks Persisted.**
  - Crash before commit: piece is re-downloaded (wasted work, never corruption).
  - Commit implies the data was flushed (Windows: `FlushFileBuffers`; Linux: `fsync`).
- Restart: load state, re-hash every Persisted piece (`resume_verify=full`, default) or trust the DB
  (`resume_verify=trust`, faster; measured as "resume overhead"). Mismatches → Missing.
- Documented limitations: drive write caches that lie about flushes, and replicas on one machine
  sharing a single disk and power supply.

## 7. Scheduling

**Piece picker** (pure function of a snapshot):
candidates = Missing pieces not in flight with ≥ 1 eligible source.
Policies: `sequential` (baseline), `random`, `rarest-first` (default).
Rarest-first sorts by availability ascending; ties are broken by `hash(seed, piece)`, which is
deterministic with a fixed seed and random-like across seeds.

**Endgame**: entered when every remaining Missing piece is already in flight. A piece may get a
second request (max duplicates `D=2`, configurable) to a different peer only if its oldest
request is older than `1.5 × the peer's expected completion time`. First persisted copy wins and
the rest receive `CANCEL`.

**Peer selector** (eligible = connected, has the piece, not banned, not in cooldown, outstanding <
per-peer cap):

- `random` — uniform over eligible peers.
- `fastest` — highest EWMA payload throughput.
- `adaptive` — minimise the **expected completion time**, inflated by unreliability:

  ```
  ECT(p)   = RTT(p) + (outstanding(p) + 1) × piece_size / throughput(p)
  cost(p)  = ECT(p) / reliability(p)          reliability = EWMA success ratio, floor 0.05
  ```

  This uses no arbitrary weights: every term is a measured quantity in seconds. Load spreading
  comes from the `outstanding + 1` term, so a fast peer that is already loaded starts costing more.
  Cold start: a peer with fewer than 3 samples is assigned the median throughput of the known
  peers (optimistic prior), and 1 in 10 decisions (configurable ε) goes to the least-sampled
  eligible peer, so new peers get measured.

**Peer health**: invalid piece → reliability penalty; 2 invalid pieces in 10 minutes → ban the
peer. 3 consecutive timeouts → cooldown with exponential backoff (5 s → 80 s). A slow peer is
deprioritised, not killed. A connection is declared dead only on TCP close/error or 30 s without
any frame (keepalive every 10 s).

**Bounds (defaults, configurable)**:
- Connections and requests: max 50 connections; 32 outstanding requests per swarm; 4 per peer;
  8 queued frames per connection send queue.
- Uploads: 4 queued uploads per remote peer, and at most 16 queued or in-progress uploads across
  all peers of a swarm (decision 0003). Requests beyond either limit get `REJECT busy`. A busy
  reject does not lower reliability; the requester avoids that peer for 250 ms ± 25 %.
- Request deadline: clamp(4 × that peer's p95 latency, 2 s, 30 s); 10 s before the peer has
  latency history.

## 8. Peer measurement

- RTT: `PING`/`PONG` every 5 s on the same connection, so it includes queueing behind data. It is
  labelled **application RTT**. Timeouts never produce an RTT sample.
- Payload throughput sample: piece bytes divided by the time from REQUEST sent to the PIECE fully
  received. EWMA with α = 0.3, plus a 10 s rolling window for dashboards.
- Request latency: per-peer histogram (p50/p95) over a rolling window of the last 200 requests.
- Throughput is application goodput, never link capacity, and loopback numbers are labelled as
  such.

## 9. Discovery

- Tracker: `POST /announce {swarm_id, peer_id, port, event}` returns up to 50 peers. Peer TTL
  is 90 s and peers re-announce every 30 s. The remote IP is taken from the TCP connection
  (needed for the second machine). An optional `advertise_addr` is used by the lab to route
  through proxies.
- PEX: every 30 s each peer sends up to 50 known-good addresses.
- Failure: if the tracker is unreachable, retry with exponential backoff (1 s → 60 s) plus jitter,
  and keep using PEX and `peers_seen`. This covers scenario 12.

## 10. Replication

- Target `R` (default 3, counting self) per swarm. Only peers started with `--accept-replicas`
  and a storage budget are eligible.
- Every 5 s, for each Persisted piece: `verified = peers whose PROOF is valid and younger than
  the TTL`.
  - If `verified < R`, first challenge peers that claim the piece.
  - If still short, send `REPL_OFFER` to eligible peers lacking it, ordered by adaptive cost.
  - At most 4 offers in flight; retry a failed peer after a backoff.
- The receiver downloads the offered piece through the normal verified path, so a replica is only
  ever created from hash-checked data.
- Replication stops when `verified ≥ R`. If a disconnect drops a piece below R, the shortfall is
  detected on the next sweep and an under-replication event is emitted.
- Guarantee (documented): best-effort, eventually ≥ R verified copies while enough eligible peers
  stay alive. There is no consensus and no durability promise beyond each peer's fsync. On one
  machine, replicas give process-level redundancy only.

## 11. Observability

- **Prometheus metrics** (per peer, `/metrics`), with labels bounded to swarm and remote peer ID
  (no per-piece labels):
  - Counters: `swarmflow_bytes_{sent,received}_total`, `…_requests_total{result}`,
    `…_invalid_pieces_total`, `…_timeouts_total`, `…_replication_offers_total{result}`.
  - Histogram: `…_request_duration_seconds`.
  - Gauges: `…_pieces{state}`, `…_outstanding_requests`, `…_connections`, `…_peer_rtt_seconds`,
    `…_peer_throughput_bytes`, `…_peer_cost_seconds`, `…_replicas_verified`.
- **Events** (ring buffer of 10 000 entries, sequence-numbered, `/api/events?since=N`), also
  written as structured JSON logs:
  - Types: peer_connected, peer_disconnected, piece_scheduled, piece_persisted, piece_rejected,
    request_timeout, endgame_entered, replica_verified, under_replicated, …
  - Example of a `piece_scheduled` event carrying the real decision inputs:
    `{piece:17, availability:1, policy:"rarest", peer:"B", ect_s:0.21, reliability:0.97,
    runner_up:{peer:"C", ect_s:0.48, reason:"cooldown: 3 timeouts"}}`.
- **swarmflow-hub**:
  - Gets peer endpoints from the tracker and polls each peer's `/api/*`.
  - Serves `/sd` (Prometheus HTTP service discovery, so new peers appear automatically).
  - Serves `/api/topology`: edges only for connections that both endpoints report.
  - Serves `/api/events`: a merged event feed.
  - Monitoring failures are isolated. The API runs on its own goroutines, and event emission never
    blocks (on overflow it drops events and counts the drops).
- **Grafana**: provisioned dashboards (JSON in `deploy/grafana/`) built on Prometheus panels. The
  topology panel (Node Graph) and the decision-log table read the hub's JSON through the Infinity
  datasource plugin (decision 0001 #11).

## 12. Test strategy

- Unit tests:
  - codec, including round-trip, malformed input and fuzzing;
  - manifest;
  - picker and selector with deterministic snapshots;
  - EWMA and estimators;
  - the StateDB state machine;
  - verifier;
  - replicator logic with a fake clock.
- **Discrete-event simulator** (`internal/sim`) for repeatable scheduler and policy comparisons:
  thousands of runs, no sockets.
- Integration tests: in-process peers on loopback, plus subprocess tests that kill processes
  for real.
- Race detector: on Windows, Go's `-race` needs a C compiler (CGO). We run `go test -race` inside
  WSL2 with a Linux Go toolchain stored on D: (`/mnt/d/DevTools/go-linux`).
- **swarmlab**:
  - Scenario files cover the 12 required scenarios.
  - Test data is generated deterministically from a seed.
  - Each scenario runs N trials, checks files byte-for-byte, and records metrics as JSON.
  - It reports median, p95, IQR and success rate, and generates `docs/benchmarks.md` from those
    results.
- **faultproxy**:
  - One proxy instance per peer models that peer's access link.
  - Impairments: delay and jitter, a token-bucket bandwidth cap per direction, stalls, connection
    resets after N bytes, and byte corruption at an offset.
  - Settings are changeable live over HTTP.
  - Its results are labelled "application-level simulated impairment".

## 13. Repository layout

Go module path: `github.com/pgatdesk/swarmflow`.

```
cmd/swarmflow      peer CLI: make-manifest, seed, get, serve
cmd/tracker        bootstrap tracker
cmd/hub            monitoring aggregator + Prometheus SD
cmd/swarmlab       experiment runner
cmd/faultproxy     impairment proxy
internal/protocol  internal/manifest  internal/conn  internal/discovery
internal/scheduler internal/peerstats internal/coordinator internal/store
internal/statedb   internal/verify    internal/replication internal/events
internal/metrics   internal/api       internal/sim
deploy/prometheus  deploy/grafana     scenarios/   scripts/   docs/
```

Dependencies are deliberately few: the Go standard library (`net`, `crypto/sha256`, `log/slog`,
`net/http`), `modernc.org/sqlite`, and `github.com/prometheus/client_golang`. Prometheus and
Grafana run as native Windows binaries stored under `D:\DevTools`, with no Docker.

## 14. Milestones and acceptance

| M | Phase | Done when (tested) |
|---|-------|--------------------|
| M1 | 2 Foundation | manifest + protocol + tracker + sequential picker; 1 seeder → 3 downloaders, byte-identical; disconnect handled |
| M2 | 3 Scheduling | rarest-first/endgame/bounds; sim shows rarest-first beats sequential on piece diversity; endgame duplicate test writes once |
| M3 | 4 Fault tolerance | kill-and-resume, corrupt-piece rejection, peer-kill reassignment, replication to R, all-copies-lost behaviour |
| M4 | 5 Peer selection | 3 policies compared under identical faultproxy profiles with N trials; results reported whichever way they fall |
| M5 | 6 Observability | Prometheus + Grafana dashboards show live peers, progress, failures, recovery, decision events |
| M6 | 7 Experiments | all 12 scenarios automated; benchmark report from real runs; second-machine run |
| M7 | 8 Delivery | clean-setup walkthrough, docs, demo script, limitations, resume summary |
