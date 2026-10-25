# 0001 — Phase 0 decisions

Date: 2026-10-25. Status: **Approved by project owner.**

| # | Decision | Rationale | Alternatives considered |
|---|----------|-----------|-------------------------|
| 1 | Implementation language: **Go 1.27** | Goroutines and the standard `net` package suit concurrent peer networking; single static binaries make the second-machine deployment trivial; higher throughput ceiling than Python | Python 3.13 asyncio (owner's prior experience; lower throughput ceiling) |
| 2 | Toolchain location: **D:\DevTools** (Go root, GOPATH, build cache) | Owner request — D: has free space | Default C:\Program Files |
| 3 | No Docker for now; native multi-process launcher | Docker not installed; native processes are sufficient for the lab | Docker Compose |
| 4 | Network impairment via **application-level TCP fault-injection proxy** | Cross-platform, deterministic, scriptable mid-run. Limitation: it cannot produce true IP packet loss or reordering, only its effects at the TCP level (delay, stalls, resets). Results are labelled "simulated". | Linux `tc netem` and network namespaces in WSL2 (available later, since the owner has sudo) |
| 5 | Request granularity: **whole pieces, 256 KiB default** | One hash check per request and a simpler state machine | 16 KiB sub-blocks within larger pieces |
| 6 | Replication: **push to target R, replicas confirmed by possession challenge** | Distinguishes claimed copies from verified copies without needing a consensus protocol | Rely on natural swarm replication; quorum or consensus |
| 7 | Monitoring: **Prometheus + Grafana** | Industry-standard and resume-relevant | Custom dashboard as the primary view |
| 8 | Scope: everything in the brief, including a cross-machine run on a second machine (owner will provide) | — | — |
| 9 | Security scope v1: integrity only (SHA-256 manifest); no TLS or peer authentication | The four networking features are the focus | TLS with peer keys |
| 10 | Version control: git, commits at milestones | — | — |
