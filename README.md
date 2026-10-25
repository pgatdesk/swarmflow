# SwarmFlow

An adaptive, fault-tolerant peer-to-peer file distribution network written in Go. Peers find
each other, fetch file pieces from many sources at once, verify every byte, survive peer and
process failures, keep verified replicas, and explain every scheduling decision on a live
Grafana dashboard.

**Status:** phases 2–6 are complete and tested: networking, adaptive scheduling, fault tolerance
and replication, network-aware peer selection with real-network experiments, and observability.
Still to come: the full 12-scenario benchmark report, a cross-machine run, and final
documentation. See [what is not done yet](#not-done-yet).

## Highlights

- **Custom wire protocol over TCP:**
  - Versioned 64-byte handshake, length-prefixed frames with size limits.
  - Validated decoding, fuzz-tested with 17.8 M inputs.
- **Integrity:**
  - Every piece is checked against the SHA-256 hashes in a trusted manifest before it is
    written.
  - Peers that send bad data are banned.
- **Adaptive scheduling:**
  - Rarest-first piece selection.
  - Endgame duplicate requests for overdue pieces: 580 ms vs 4.0 s without, in a test with a
    slow peer.
  - Per-peer scoring from measured delivery rate, application RTT, latency percentiles and
    reliability, with cooldown after repeated timeouts.
- **Network-aware peer selection,** measured with real processes. Each peer runs in its own
  Linux network namespace, with its link shaped by `tc netem`:
  - Choosing peers by measurement beats random choice by **35–48%** in all four scenarios.
  - The adaptive selector beats the fastest-peer baseline by 13% under churn, and by 22% when
    the per-peer request cap is raised.
  - It ties fastest elsewhere; the reasons are analysed in
    [docs/results/peer-selection.md](docs/results/peer-selection.md).
- **Crash-safe resume:**
  - Progress is kept in a SQLite state database, and a piece is recorded only after its data is
    flushed to disk.
  - Test: a real downloader process was hard-killed mid-transfer; the restart fetched exactly
    the missing pieces and detected a piece deliberately corrupted on disk.
- **Failure handling:**
  - Timeouts and reassignment of outstanding requests.
  - Peer exchange (PEX), and re-discovery of remembered peers after a restart.
  - An upload admission cap with busy back-off. In simulation it turned 0% completion into
    100% when the only seed left early (decision 0003).
- **Verified replication:**
  - Copies count only after a possession proof, SHA-256(nonce ‖ piece).
  - Lost copies are re-created on other peers: 21 copies with exactly 21 offers in about
    300 ms.
  - Peers that claim pieces they don't hold are detected and banned.
- **Observability:**
  - Prometheus metrics from every peer.
  - A monitoring hub that finds peers through the tracker.
  - A provisioned Grafana dashboard: a topology of observed connections, and an event feed
    explaining why each piece and peer was chosen.

## Quick start (Windows, PowerShell)

Requires Go 1.27+.

```powershell
./scripts/demo-basic.ps1 -Size 256MiB -Downloaders 3   # tracker + seeder + 3 downloaders, SHA-256 checked
./scripts/demo-dashboard.ps1                           # live Grafana demo with a seed failure (see docs/monitoring.md)
```

Manual use:

```powershell
go build -o bin ./cmd/...
bin/swarmflow gendata -out payload.bin -size 64MiB
bin/swarmflow make-manifest -in payload.bin            # writes payload.bin.swarm
bin/tracker -listen 127.0.0.1:7000
bin/swarmflow seed -manifest payload.bin.swarm -file payload.bin -tracker http://127.0.0.1:7000
bin/swarmflow get  -manifest payload.bin.swarm -out dl/payload.bin -tracker http://127.0.0.1:7000
```

## Experiments

```powershell
go run ./cmd/swarmlab sim -trials 20                   # deterministic simulator, policy comparison
# Real network emulation (WSL2, root): one namespace per peer, tc netem links
wsl -d Ubuntu -u root --cd /mnt/d/CryptoProject -e ./bin/linux/swarmlab netem -scenario scenarios/selection-heterogeneous.json
```

Results: [docs/results/](docs/results/).

## Tests

```powershell
go test ./...
go test ./internal/protocol -run '^$' -fuzz FuzzReadMessage -fuzztime 60s
```

The suite includes unit tests and in-process multi-peer integration tests. It also has
process-level tests that hard-kill a real peer: crash/resume, and restart without a tracker.
Fault injection covers stalls, resets, corruption and lying peers.

## Documentation

- [Architecture](docs/design/architecture.md): components, protocol, state model, scheduling,
  replication.
- [Decision log](docs/decisions/).
- [Integrity, durability and availability](docs/durability.md): what is and isn't guaranteed.
- [Monitoring](docs/monitoring.md): API, metrics, events, dashboard.
- [Troubleshooting](docs/troubleshooting.md).

## Not done yet

- The automated run of all 12 required scenarios and the consolidated benchmark report.
- A two-machine run. Replication across processes on one machine is not machine-level fault
  tolerance.
- No transport encryption or peer authentication. Integrity relies on the manifest.
- Not compatible with BitTorrent clients. The protocol is SwarmFlow's own.
