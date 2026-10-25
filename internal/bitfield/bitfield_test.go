package bitfield

import "testing"

func TestSetHasCount(t *testing.T) {
	b := New(10)
	for _, i := range []int{0, 7, 8, 9} {
		b.Set(i)
	}
	if b.Count() != 4 || !b.Has(9) || b.Has(1) || b.Complete() {
		t.Fatalf("unexpected state %08b", b.Bytes())
	}
	if got := b.Bytes(); got[0] != 0x81 || got[1] != 0xC0 {
		t.Fatalf("wire layout %08b, want MSB-first", got)
	}
	b.Clear(0)
	if b.Has(0) || b.Count() != 3 {
		t.Fatal("clear failed")
	}
}

func TestFromBytesValidation(t *testing.T) {
	if _, err := FromBytes(10, []byte{0xFF}); err == nil {
		t.Error("short bitfield accepted")
	}
	if _, err := FromBytes(10, []byte{0xFF, 0xC0, 0}); err == nil {
		t.Error("long bitfield accepted")
	}
	if _, err := FromBytes(10, []byte{0xFF, 0xE0}); err == nil {
		t.Error("nonzero padding accepted")
	}
	b, err := FromBytes(10, []byte{0xFF, 0xC0})
	if err != nil || !b.Complete() {
		t.Fatalf("valid bitfield: %v", err)
	}
	if b, err := FromBytes(0, nil); err != nil || !b.Complete() {
		t.Fatalf("empty bitfield: %v", err)
	}
}

func TestFromBytesCopies(t *testing.T) {
	in := []byte{0x80}
	b, _ := FromBytes(8, in)
	in[0] = 0
	if !b.Has(0) {
		t.Fatal("bitfield aliases its input")
	}
}
