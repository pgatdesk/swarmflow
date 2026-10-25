# Running across two machines

This is the only SwarmFlow test with real machine-level fault tolerance: the peers do not share a
disk, a power supply or a network stack. **Status: planned; not yet run.**

## Setup

- **Machine A** (this laptop) runs the tracker, a seeder and optionally the monitoring stack.
- **Machine B** runs a downloader and/or a replica holder.

Both machines must be on the same LAN or Wi-Fi with no client isolation, so each can reach the
other's IP.

1. **Find A's LAN address.** On Windows, run `ipconfig` and note the IPv4 address of the active
   adapter. Call it `A_IP` (for example `192.168.1.20`).
2. **Allow inbound TCP on both machines.** Ports 7000 (tracker) and 7001 (peer), plus 7002 on B
   if it runs a second peer. On Windows, in an administrator PowerShell:

   ```powershell
   New-NetFirewallRule -DisplayName "SwarmFlow" -Direction Inbound -Protocol TCP -LocalPort 7000-7002 -Action Allow -Profile Private
   ```

3. **Copy the binaries to B.** Use `bin/swarmflow.exe` for Windows, or for Linux build with
   `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/linux/ ./cmd/...` and copy
   `bin/linux/swarmflow`. Also copy the manifest (`*.swarm`). The data file is not needed on B.

## Run

On A:

```powershell
bin/swarmflow gendata -out payload.bin -size 1GiB -seed 1
bin/swarmflow make-manifest -in payload.bin        # copy payload.bin.swarm to B
bin/tracker -listen 0.0.0.0:7000
bin/swarmflow seed -manifest payload.bin.swarm -file payload.bin -listen 0.0.0.0:7001 `
  -tracker http://A_IP:7000 -replicas 2 -stats-out seed-stats.json
```

On B:

```powershell
swarmflow replica -manifest payload.bin.swarm -out replica/payload.bin -listen 0.0.0.0:7001 -tracker http://A_IP:7000
swarmflow get     -manifest payload.bin.swarm -out dl/payload.bin     -listen 0.0.0.0:7002 -tracker http://A_IP:7000 `
  -exit-on-complete -stats-out get-stats.json
```

The tracker records each peer's address as it observes it, so the machines can reach each
other's LAN IPs without extra flags.

## What to record

1. **Integrity:** B's downloaded file matches A's. Compare `Get-FileHash -Algorithm SHA256` on
   both machines.
2. **Throughput:** `get-stats.json` gives `bytes_down`, `started_at`, `first_piece_at` and
   `finished_at`.
3. **Replication across machines:** A's status shows `replication.min_verified_copies ≥ 1` once
   the replica on B holds verified copies.
4. **Machine-level failure:**
   - Unplug or disable A's network, or shut A down, after the replica on B is complete.
   - Start a second downloader on B (or on a third device) pointed at B's tracker. To do that,
     restart the tracker on B or use `-peer B_IP:7001`.
   - It should complete from the replica alone. This is the cross-machine version of required
     scenario 10.
5. **Recovery:** pull the network cable or turn Wi-Fi off and on during a transfer.
   Disconnect, timeout and reassignment events show up in each peer's `-events-out` file.

Results go in `docs/results/two-machine.md`, marked as one real LAN run with its date and
conditions. They are not comparable to the netem lab numbers.
