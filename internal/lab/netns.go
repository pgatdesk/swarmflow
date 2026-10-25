package lab

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// Network layout: namespace sf-hub holds bridge br0 (10.77.0.254/24, where
// the tracker listens); peer i lives in namespace sf-p<i> at 10.77.0.<i>,
// joined to the bridge by veth vp<i> (peer side) / hp<i> (hub side). Only
// namespaces with the sf- prefix are ever created or deleted.
const (
	hubNS     = "sf-hub"
	trackerIP = "10.77.0.254"
	nsPrefix  = "sf-"
)

func peerNS(i int) string { return fmt.Sprintf("sf-p%d", i) }
func peerIP(i int) string { return fmt.Sprintf("10.77.0.%d", i) }

func run(args ...string) error {
	cmd := exec.Command(args[0], args[1:]...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %v: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	return nil
}

// cleanupStale deletes sf-* namespaces left by an interrupted run.
func cleanupStale() error {
	out, err := exec.Command("ip", "netns", "list").Output()
	if err != nil {
		return fmt.Errorf("ip netns list: %w", err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if f := strings.Fields(line); len(f) > 0 && strings.HasPrefix(f[0], nsPrefix) {
			run("ip", "netns", "del", f[0])
		}
	}
	return nil
}

func setupHub() error {
	for _, m := range []string{"sch_netem"} {
		if err := run("modprobe", m); err != nil {
			return err
		}
	}
	for _, c := range [][]string{
		{"ip", "netns", "add", hubNS},
		{"ip", "-n", hubNS, "link", "set", "lo", "up"},
		{"ip", "-n", hubNS, "link", "add", "br0", "type", "bridge"},
		{"ip", "-n", hubNS, "addr", "add", trackerIP + "/24", "dev", "br0"},
		{"ip", "-n", hubNS, "link", "set", "br0", "up"},
	} {
		if err := run(c...); err != nil {
			return err
		}
	}
	return nil
}

func addPeer(i int, l Link) error {
	ns, vp, hp := peerNS(i), fmt.Sprintf("vp%d", i), fmt.Sprintf("hp%d", i)
	for _, c := range [][]string{
		{"ip", "netns", "add", ns},
		{"ip", "link", "add", vp, "netns", ns, "type", "veth", "peer", "name", hp, "netns", hubNS},
		{"ip", "-n", ns, "addr", "add", peerIP(i) + "/24", "dev", vp},
		{"ip", "-n", ns, "link", "set", "lo", "up"},
		{"ip", "-n", ns, "link", "set", vp, "up"},
		{"ip", "-n", hubNS, "link", "set", hp, "master", "br0", "up"},
	} {
		if err := run(c...); err != nil {
			return err
		}
	}
	return setLink(i, l)
}

// setLink (re)applies netem in both directions of peer i's access link.
func setLink(i int, l Link) error {
	up := append([]string{"ip", "netns", "exec", peerNS(i), "tc", "qdisc", "replace", "dev", fmt.Sprintf("vp%d", i), "root", "netem"}, l.netemArgs()...)
	down := append([]string{"ip", "netns", "exec", hubNS, "tc", "qdisc", "replace", "dev", fmt.Sprintf("hp%d", i), "root", "netem"}, l.netemArgs()...)
	if err := run(up...); err != nil {
		return err
	}
	return run(down...)
}

func setLinkState(i int, up bool) error {
	state := "down"
	if up {
		state = "up"
	}
	return run("ip", "-n", peerNS(i), "link", "set", fmt.Sprintf("vp%d", i), state)
}

func teardown(peers int) {
	for i := 1; i <= peers; i++ {
		run("ip", "netns", "del", peerNS(i))
	}
	run("ip", "netns", "del", hubNS)
}
