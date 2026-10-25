# SwarmFlow

An adaptive, fault-tolerant peer-to-peer file distribution network written in Go.

**Status: Phases 2–4 complete** (networking, adaptive scheduling, fault tolerance and
replication). Network-aware peer-selection experiments, the Prometheus/Grafana dashboard and the
benchmark report are later phases.

- Approved design: [docs/design/architecture.md](docs/design/architecture.md)
- Decision log: [docs/decisions/](docs/decisions/)
- What is and isn't guaranteed: [docs/durability.md](docs/durability.md)

## What works today (tested)

- Independent peer processes that discover each other through a bootstrap tracker or static
  `-peer` addresses, and exchange 256 KiB pieces over a versioned binary TCP protocol.
- Every piece is checked against the SHA-256 hashes in a trusted `.swarm` manifest before it is
  written. Each write is flushed to disk before the piece counts as done.
- Peer disconnects, abrupt resets and stalled peers are handled: outstanding requests time out
  or are reassigned to other peers.
- Corrupted pieces are rejected and fetched again.
- Partial seeders that together hold the whole file can serve it.
- Rarest-first piece picking, endgame duplicate requests, and peer scoring (delivery rate,
  application RTT, latency percentiles, reliability, cooldown, bans).
- An upload admission cap with busy back-off (decision 0003).
- A deterministic simulator (`swarmlab sim`) that compares the scheduling policies.
- Crash-safe resume from a SQLite state database: tested by hard-killing a real downloader
  process.
- Peer exchange (PEX), and re-discovery of remembered peers after a restart.
- Replication to a target copy count, with copies verified by possession proofs. Lost copies
  are re-replicated, and lying holders are detected and banned.

## Quick start (Windows, PowerShell)

Requires Go 1.27+.

```powershell
./scripts/demo-basic.ps1 -Size 256MiB -Downloaders 3
```

This runs one tracker, one seeder and three downloaders as separate processes, then compares
every downloaded file with the source by SHA-256.

Manual use:

```powershell
go build -o bin ./cmd/...
bin/swarmflow gendata -out payload.bin -size 64MiB
bin/swarmflow make-manifest -in payload.bin            # writes payload.bin.swarm
bin/tracker -listen 127.0.0.1:7000
bin/swarmflow seed -manifest payload.bin.swarm -file payload.bin -tracker http://127.0.0.1:7000
bin/swarmflow get  -manifest payload.bin.swarm -out dl/payload.bin -tracker http://127.0.0.1:7000
```

## Tests

```powershell
go test ./...
go test ./internal/protocol -run '^$' -fuzz FuzzReadMessage -fuzztime 60s
```
