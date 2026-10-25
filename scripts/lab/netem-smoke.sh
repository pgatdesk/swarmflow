#!/bin/sh
# Feasibility smoke test for the netem lab (run as root inside WSL/Linux):
#   two peer namespaces on a bridge inside a hub namespace, each peer's access
#   link shaped by netem in both directions; checks ping RTT and that a real
#   swarmflow transfer is limited by the configured rate. Cleans up on exit.
set -eu
BIN=${1:?usage: netem-smoke.sh /path/to/linux/swarmflow}
WORK=$(mktemp -d /tmp/sf-smoke.XXXXXX)
HUB=sf-hub

cleanup() {
  for ns in sf-a sf-b "$HUB"; do ip netns del "$ns" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT INT TERM

modprobe sch_netem
ip netns add "$HUB"
ip -n "$HUB" link add br0 type bridge
ip -n "$HUB" link set br0 up
ip -n "$HUB" link set lo up

# peer NAME INDEX DELAY RATE: one-way access delay and rate, applied to both
# directions (peer-side egress = upload, hub-side egress = download).
peer() {
  ns=sf-$1; idx=$2; delay=$3; rate=$4
  ip netns add "$ns"
  ip link add "v$1" netns "$ns" type veth peer name "h$1" netns "$HUB"
  ip -n "$ns" addr add "10.77.0.$idx/24" dev "v$1"
  ip -n "$ns" link set "v$1" up
  ip -n "$ns" link set lo up
  ip -n "$HUB" link set "h$1" master br0 up
  ip netns exec "$ns" tc qdisc add dev "v$1" root netem delay "$delay" rate "$rate" limit 10000
  ip netns exec "$HUB" tc qdisc add dev "h$1" root netem delay "$delay" rate "$rate" limit 10000
}
peer a 1 20ms 16mbit
peer b 2 30ms 16mbit

echo "== ping a -> b (expect RTT ~ 2 x (20 + 30) = 100 ms)"
ip netns exec sf-a ping -c 5 -q 10.77.0.2 | tail -1

echo "== swarmflow transfer of 8 MiB at 16 Mbit/s (expect >= 4.2 s: 8 MiB = 67.1 Mbit)"
"$BIN" gendata -out "$WORK/p.bin" -size 8MiB -seed 1
"$BIN" make-manifest -in "$WORK/p.bin" -out "$WORK/p.swarm" >/dev/null
ip netns exec sf-a "$BIN" seed -manifest "$WORK/p.swarm" -file "$WORK/p.bin" -listen 10.77.0.1:7001 -progress 0 2>"$WORK/seed.log" &
SEED=$!
sleep 0.5
start=$(date +%s.%N)
ip netns exec sf-b "$BIN" get -manifest "$WORK/p.swarm" -out "$WORK/b/p.bin" -listen 10.77.0.2:7002 \
  -peer 10.77.0.1:7001 -exit-on-complete -progress 0 2>"$WORK/get.log"
end=$(date +%s.%N)
kill "$SEED" 2>/dev/null || true
wait "$SEED" 2>/dev/null || true
echo "elapsed: $(echo "$end - $start" | bc) s"
if cmp -s "$WORK/p.bin" "$WORK/b/p.bin"; then echo "content: MATCH"; else echo "content: MISMATCH"; exit 1; fi
