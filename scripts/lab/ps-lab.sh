#!/bin/sh
# Show the netem lab's namespaces and processes (diagnostics).
echo "namespaces: $(ip netns list | grep -c '^sf-')"
ps -eo pid,etimes,stat,args --sort=etimes | grep -E 'swarmflow|/tracker|swarmlab' | grep -v grep | cut -c1-180
