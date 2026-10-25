package sim

import (
	"reflect"
	"testing"
	"time"
)

func small(peers []PeerSpec, v Variant) Config {
	sc := Scenario{FileSize: 4 * MiB, PieceSize: 256 << 10, Horizon: 5 * time.Minute,
		Peers: func(uint64) []PeerSpec { return peers }}
	return sc.Config(v, 1)
}

func TestDeterministic(t *testing.T) {
	sc := Scenarios()[0]
	for _, v := range sc.Variants {
		a, b := Run(sc.Config(v, 3)), Run(sc.Config(v, 3))
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("%s: two runs with the same seed differ", v)
		}
	}
}

func TestSingleSourceTransferTime(t *testing.T) {
	// One seed at 1 MiB/s and one downloader: 4 MiB takes ~4 s plus latency.
	peers := []PeerSpec{
		{Name: "s", Seed: true, Up: MiB, Latency: 10 * time.Millisecond},
		{Name: "d", Latency: 10 * time.Millisecond},
	}
	r := Run(small(peers, Variant{Picker: "rarest", Selector: "adaptive", Endgame: true}))
	if r.Completed != 1 {
		t.Fatal("download did not complete")
	}
	got := r.Completion[0].Seconds()
	if got < 4 || got > 4.2 {
		t.Fatalf("completion %.3fs, want ~4s at full upload rate", got)
	}
	if r.Uploaded[0] < 4*MiB {
		t.Fatalf("seed uploaded %d bytes", r.Uploaded[0])
	}
}

func TestSeedDepartureWithoutCopiesStrandsDownloaders(t *testing.T) {
	peers := []PeerSpec{
		{Name: "s", Seed: true, Up: MiB, Latency: 10 * time.Millisecond, LeaveAt: time.Second},
		{Name: "d", Latency: 10 * time.Millisecond},
	}
	r := Run(small(peers, Variant{Picker: "rarest", Selector: "adaptive", Endgame: true}))
	if r.Completed != 0 || r.Completion[0] != -1 {
		t.Fatal("downloader completed although the only source left after 1/4 of the file")
	}
}

func TestDownloadersShareWithEachOther(t *testing.T) {
	peers := []PeerSpec{{Name: "s", Seed: true, Up: MiB, Latency: 10 * time.Millisecond}}
	for range 4 {
		peers = append(peers, PeerSpec{Up: MiB, Latency: 10 * time.Millisecond})
	}
	r := Run(small(peers, Variant{Picker: "rarest", Selector: "adaptive", Endgame: true}))
	if r.Completed != 4 {
		t.Fatalf("%d/4 completed", r.Completed)
	}
	var fromPeers int64
	for _, u := range r.Uploaded[1:] {
		fromPeers += u
	}
	if fromPeers == 0 {
		t.Fatal("downloaders never uploaded to each other")
	}
	// Without sharing the seed would need 4 × 4 MiB / 1 MiB/s = 16 s.
	if last := r.Completion[3]; last > 12*time.Second {
		t.Fatalf("last completion %v; sharing should beat the 16 s seed-only bound", last)
	}
}
