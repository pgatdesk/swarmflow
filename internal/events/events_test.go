package events

import (
	"sync"
	"testing"
)

func TestRingSinceAndOverwrite(t *testing.T) {
	r := NewRing(4)
	for i := range 6 {
		r.Add(Event{Type: "t", Message: string(rune('a' + i))})
	}
	evs, next := r.Since(0, 0)
	if len(evs) != 4 || evs[0].Seq != 2 || evs[3].Message != "f" || next != 6 {
		t.Fatalf("got %d events from %d, next %d", len(evs), evs[0].Seq, next)
	}
	if total, dropped := r.Stats(); total != 6 || dropped != 2 {
		t.Fatalf("stats %d/%d", total, dropped)
	}
	evs, next = r.Since(4, 1)
	if len(evs) != 1 || evs[0].Seq != 4 || next != 5 {
		t.Fatalf("limit: %+v next %d", evs, next)
	}
	if evs, next = r.Since(6, 0); len(evs) != 0 || next != 6 {
		t.Fatalf("caught up: %d events, next %d", len(evs), next)
	}
}

func TestNilRingAndConcurrency(t *testing.T) {
	var nilRing *Ring
	nilRing.Add(Event{}) // must not panic
	r := NewRing(1000)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				r.Add(Event{Type: "x"})
			}
		})
	}
	wg.Wait()
	if total, dropped := r.Stats(); total != 4000 || dropped != 3000 {
		t.Fatalf("stats %d/%d", total, dropped)
	}
	evs, _ := r.Since(0, 0)
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatal("sequence gap")
		}
	}
}
