# SwarmFlow

An adaptive, fault-tolerant peer-to-peer file distribution network written in Go.

**Status: Phase 2 (networking foundation) complete.** Adaptive scheduling, crash recovery,
replication, network-aware peer selection and the Prometheus/Grafana dashboard are later
phases. See [docs/design/architecture.md](docs/design/architecture.md) for the approved design and
[docs/decisions/](docs/decisions/) for the decision log.

## What works today (tested)

- Independent peer processes that discover each other through a bootstrap tracker or static
  `-peer` addresses, and exchange 256 KiB pieces over a versioned binary TCP protocol.
- Every piece is checked against the SHA-256 hashes in a trusted `.swarm` manifest before it is
  written. Each write is flushed to disk before the piece counts as done.
- Peer disconnects, abrupt resets and stalled peers are handled: outstanding requests time out
  or are reassigned to other peers.
- Corrupted pieces are rejected and fetched again.
- Partial seeders that together hold the whole file can serve it.

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
