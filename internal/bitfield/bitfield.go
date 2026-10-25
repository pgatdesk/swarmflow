// Package bitfield implements a fixed-length piece set using the wire layout of
// BITFIELD messages: bit i is bit (7 - i%8) of byte i/8, MSB first.
package bitfield

import (
	"fmt"
	"math/bits"
)

type Bitfield struct {
	n    int
	bits []byte
}

func New(n int) Bitfield { return Bitfield{n: n, bits: make([]byte, (n+7)/8)} }

// FromBytes validates a wire bitfield for n pieces: the length must be exact and
// the padding bits after piece n-1 must be zero. The input is copied.
func FromBytes(n int, b []byte) (Bitfield, error) {
	want := (n + 7) / 8
	if len(b) != want {
		return Bitfield{}, fmt.Errorf("bitfield: %d bytes for %d pieces, want %d", len(b), n, want)
	}
	if rem := n % 8; rem != 0 && b[len(b)-1]&(0xFF>>rem) != 0 {
		return Bitfield{}, fmt.Errorf("bitfield: nonzero padding bits")
	}
	return Bitfield{n: n, bits: append([]byte(nil), b...)}, nil
}

func (b *Bitfield) Len() int        { return b.n }
func (b *Bitfield) Has(i int) bool  { return b.bits[i/8]&(0x80>>(i%8)) != 0 }
func (b *Bitfield) Set(i int)       { b.bits[i/8] |= 0x80 >> (i % 8) }
func (b *Bitfield) Clear(i int)     { b.bits[i/8] &^= 0x80 >> (i % 8) }
func (b *Bitfield) Complete() bool  { return b.Count() == b.n }
func (b *Bitfield) Bytes() []byte   { return append([]byte(nil), b.bits...) }
func (b *Bitfield) Clone() Bitfield { return Bitfield{n: b.n, bits: b.Bytes()} }

func (b *Bitfield) Count() int {
	c := 0
	for _, x := range b.bits {
		c += bits.OnesCount8(x)
	}
	return c
}
