#!/bin/sh
# Reproduce scenario 03's downloader memory on loopback (no netem): three
# partial seeders (even, odd, first half) and one downloader under /usr/bin/time.
set -eu
BIN=${1:?usage: mem-repro.sh /path/to/linux/swarmflow [data-dir]}
DATA=${2:-/tmp/sf-lab/data/64MiB-256KiB-31}
W=$(mktemp -d /tmp/sf-mem.XXXXXX)
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$W"' EXIT
"$BIN" tracker 2>/dev/null || true
port=7300
for has in even odd first-50pct; do
  port=$((port+1))
  "$BIN" seed -manifest "$DATA/payload.swarm" -file "$DATA/payload.bin.$has" -listen 127.0.0.1:$port -progress 0 2>"$W/seed-$has.log" &
done
sleep 1.5
case ${3:-partial} in
  full) PEERS="-peer 127.0.0.1:7399"; "$BIN" seed -manifest "$DATA/payload.swarm" -file "$DATA/payload.bin" -listen 127.0.0.1:7399 -progress 0 2>"$W/seed-full.log" & sleep 1 ;;
  *) PEERS="-peer 127.0.0.1:7301 -peer 127.0.0.1:7302 -peer 127.0.0.1:7303" ;;
esac
/usr/bin/time -v "$BIN" get -manifest "$DATA/payload.swarm" -out "$W/d/payload.bin" $PEERS \
  -exit-on-complete -progress 0 -memprofile "${MEMPROF:-/tmp/sf-mem.pprof}" 2>"$W/get.log" || true
grep -v -E '^\s' "$W/get.log" | grep -v '"level":"INFO"' | tail -3
grep -E "Maximum resident|Elapsed" "$W/get.log"
grep -o '"msg":"download complete"[^}]*' "$W/get.log" | head -1 || grep "download complete" "$W/get.log" | head -1
