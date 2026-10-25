#!/bin/sh
# Read-only check of what the netem lab needs on this Linux/WSL host.
echo "kernel: $(uname -r)"
for t in ip tc; do printf '%s: ' "$t"; command -v "$t" || echo "MISSING"; done
cfg=""
if [ -r /proc/config.gz ]; then cfg="zcat /proc/config.gz"; elif [ -r "/boot/config-$(uname -r)" ]; then cfg="cat /boot/config-$(uname -r)"; fi
if [ -n "$cfg" ]; then
  $cfg | grep -E '^CONFIG_(NET_SCH_NETEM|NET_SCH_TBF|NET_SCH_HTB|NET_NS|VETH|IFB|NET_CLS_U32|NET_ACT_MIRRED)='
else
  echo "kernel config not readable"
fi
printf 'sudo without password: '; sudo -n true 2>/dev/null && echo yes || echo no
