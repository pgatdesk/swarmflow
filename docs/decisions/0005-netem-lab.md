# 0005 — Network experiments use Linux network namespaces and `tc netem`

Date: 2026-10-25. Status: **Approved by project owner** ("option 3, if it works"). This
supersedes 0001 #4 (TCP fault-injection proxy) for experiments. 0001 #12 (netem for scenarios
8–9) is now the general approach.

## Problem

A proxy in front of a peer only shapes connections made *to* that peer. Once peers dial each
other (tracker results, PEX), traffic between two peers is shaped by whichever side accepted
the connection. A peer's uplink would be slow on some connections and not others, which
invalidates peer-selection experiments.

## Options

1. In-process shaping behind a lab-only flag.
2. A proxy per peer, with peer-to-peer dialing restricted.
3. **Linux network namespaces with `tc netem` (chosen).**

## Design

- **Topology:** a hub namespace (`sf-hub`) holds a bridge. Each peer runs in its own namespace
  (`sf-<name>`), connected to the hub by a veth pair. WSL's own network namespace is not
  modified. The orchestrator never touches namespaces it did not create; other software's
  `cni-*` namespaces exist on this host.
- **Access link per peer:** netem on the peer-side veth shapes uploads, and netem on the
  hub-side veth shapes downloads. Delay, jitter, rate and loss come from the scenario file and
  apply to every connection the peer has, in both directions. Path delay between two peers is
  the sum of their one-way delays.
- **Privileges:** the orchestrator needs root. On this machine it runs via `wsl -u root`,
  because the owner's sudo needs a password.
- **Binaries and data:** Linux binaries are cross-compiled from Windows into `bin/linux/` and
  run from `/mnt/d`. Experiment data lives in `/tmp` inside WSL, removed after each run,
  because file-sync calls through `/mnt/d` would make the disk, not the network, the
  bottleneck.
- **Feasibility check** (`scripts/lab/netem-smoke.sh`, 2026-10-25):
  - Configured RTT 100 ms; measured ping minimum 100.4 ms.
  - 8 MiB at 16 Mbit/s took 5.05 s against a 4.2 s floor.
  - Content matched the source byte-for-byte.

## Consequences

- Results are **real kernel-level impairment** (queueing, loss, TCP dynamics), not simulated
  delays. They are still a single-host emulation: all peers share one CPU and disk, and the
  bridge itself is unconstrained.
- Experiments need WSL2 (or Linux) and root. Unit and integration tests remain native Windows
  and do not need them.
- The in-process `faultnet` package remains for unit and integration tests only.
