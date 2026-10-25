# Troubleshooting

## "bind: An attempt was made to access a socket in a way forbidden by its access permissions"

**Seen on:** Windows 11, intermittently, for both listening and outgoing connections, in test
binaries and in `swarmflow.exe` alike.

**Pattern:** it comes in waves, usually right after many processes have opened and closed
sockets (back-to-back test runs, the dashboard demo). It clears on its own. During a wave, the
same bind from PowerShell (.NET) still succeeded.

**Cause:** not established. The likely suspects are Windows dynamic port reservations (Hyper-V
and WSL create "administered port exclusions"; see `netsh interface ipv4 show excludedportrange
protocol=tcp`), or security software rate-limiting new sockets from fresh executables. Port
exhaustion was ruled out: only about 300 ports were in use.

**Mitigations in the code:**
- `netx.Listen` retries a port-0 bind with back-off, then tries random ports from the dynamic
  range.
- Peers retry failed dials on their next discovery round.

**If you hit it:**
- Wait a minute and retry.
- Building test binaries under `D:\DevTools\gotmp` (`go env -w GOTMPDIR=...`) made it rarer
  for tests.

## `go` is not recognised

Go is installed under `D:\DevTools\go`. The scripts add `D:\DevTools\go\bin` to `PATH` when `go`
is missing. A second Go installation in `C:\Program Files\Go` (from an earlier installer) can
shadow it. Uninstall it from Settings → Apps.

## A download never finishes; `unavailable_pieces` > 0

No connected peer has some of the pieces. The download waits and keeps re-discovering peers
through the tracker, PEX and remembered peers. Start a peer that has those pieces, or check
that seeders are reachable (firewall, `-listen 0.0.0.0:PORT` for other machines).

## The netem lab says it needs Linux and root

Run it inside WSL2 as root, for example:

```powershell
wsl -d Ubuntu -u root --cd /mnt/d/CryptoProject -e ./bin/linux/swarmlab netem -scenario scenarios/selection-heterogeneous.json
```

Build the Linux binaries first with `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/linux/ ./cmd/...`.
`scripts/lab/check-netem.sh` lists what the kernel supports.

## Grafana panels say "No data"

- **Peers table, topology and events:** these come from the hub. Check
  <http://127.0.0.1:7100/api/peers>.
- **Metrics panels:** check <http://127.0.0.1:9090/targets>. Peers must run with a monitoring API
  (the default) and announce to the same tracker the hub polls.
- **The "SwarmFlow Hub" datasource errors:** the Infinity plugin must be installed in
  `<Tools>\monitoring\grafana\plugins`.
