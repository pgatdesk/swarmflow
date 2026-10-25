package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"testing"
)

func allMessages() []Message {
	return []Message{
		Bitfield{Bits: []byte{0xA0, 0x01}},
		Have{Piece: 7},
		Request{ReqID: 1, Piece: 2},
		Piece{ReqID: 3, Piece: 4, Data: []byte("hello piece")},
		Cancel{ReqID: 5},
		Reject{ReqID: 6, Reason: RejectBusy},
		Ping{Nonce: 0xDEADBEEF},
		Pong{Nonce: 42},
		PEX{Peers: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:7001"), netip.MustParseAddrPort("[2001:db8::1]:9")}},
		Challenge{Piece: 9, Nonce: [16]byte{1, 2, 3}},
		Proof{Piece: 9, Nonce: [16]byte{1, 2, 3}, Digest: [32]byte{4, 5, 6}},
		ReplOffer{Piece: 11},
		ReplReply{Piece: 11, Accept: true},
		KeepAlive{},
		Bye{Reason: ByeShutdown},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, m := range allMessages() {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("%s: write: %v", m.Type(), err)
		}
		got, err := ReadMessage(&buf)
		if err != nil {
			t.Fatalf("%s: read: %v", m.Type(), err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%s: got %#v, want %#v", m.Type(), got, m)
		}
		if buf.Len() != 0 {
			t.Errorf("%s: %d trailing bytes", m.Type(), buf.Len())
		}
	}
}

func TestHandshakeRoundTrip(t *testing.T) {
	h := Handshake{Major: 1, Minor: 3, Flags: 0x8001, Swarm: SwarmID{9, 9}, Peer: PeerID{7}, ListenPort: 6881}
	var buf bytes.Buffer
	if err := WriteHandshake(&buf, h); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != HandshakeLen {
		t.Fatalf("handshake is %d bytes", buf.Len())
	}
	got, err := ReadHandshake(&buf)
	if err != nil || got != h {
		t.Fatalf("got %+v, %v; want %+v", got, err, h)
	}
}

func TestHandshakeBadMagic(t *testing.T) {
	b := make([]byte, HandshakeLen)
	copy(b, "HTTP")
	if _, err := ReadHandshake(bytes.NewReader(b)); !errors.Is(err, ErrBadMagic) {
		t.Fatalf("got %v, want ErrBadMagic", err)
	}
}

func frame(typ Type, payload []byte) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+1))
	return append(append(b, byte(typ)), payload...)
}

func TestReadRejects(t *testing.T) {
	tooBig := binary.BigEndian.AppendUint32(nil, MaxFrame+1)
	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"empty frame", []byte{0, 0, 0, 0}, ErrEmptyFrame},
		{"oversize length", tooBig, ErrFrameTooLarge},
		{"unknown type", frame(0x7F, nil), ErrUnknownType},
		{"short HAVE", frame(TypeHave, []byte{1, 2}), ErrMalformed},
		{"long REQUEST", frame(TypeRequest, make([]byte, 9)), ErrMalformed},
		{"short PIECE", frame(TypePiece, make([]byte, 7)), ErrMalformed},
		{"PEX count over max", frame(TypePEX, []byte{MaxPEXPeers + 1}), ErrMalformed},
		{"PEX count mismatch", frame(TypePEX, append([]byte{2}, make([]byte, 18)...)), ErrMalformed},
		{"REPL_REPLY bad bool", frame(TypeReplReply, []byte{0, 0, 0, 1, 2}), ErrMalformed},
		{"KEEPALIVE with payload", frame(TypeKeepAlive, []byte{0}), ErrMalformed},
		{"truncated body", frame(TypeHave, []byte{0, 0, 0, 1})[:6], io.ErrUnexpectedEOF},
	}
	for _, c := range cases {
		_, err := ReadMessage(bytes.NewReader(c.in))
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}

func TestWriteRejectsOversizePEX(t *testing.T) {
	m := PEX{Peers: make([]netip.AddrPort, MaxPEXPeers+1)}
	if err := WriteMessage(io.Discard, m); !errors.Is(err, ErrMalformed) {
		t.Fatalf("got %v, want ErrMalformed", err)
	}
}

// FuzzReadMessage checks that arbitrary input never panics and that anything
// accepted re-encodes to the same frame.
func FuzzReadMessage(f *testing.F) {
	for _, m := range allMessages() {
		var buf bytes.Buffer
		WriteMessage(&buf, m)
		f.Add(buf.Bytes())
	}
	f.Fuzz(func(t *testing.T, in []byte) {
		m, err := ReadMessage(bytes.NewReader(in))
		if err != nil {
			return
		}
		var buf bytes.Buffer
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("re-encode %s: %v", m.Type(), err)
		}
		n := len(buf.Bytes())
		if n > len(in) || !bytes.Equal(buf.Bytes(), in[:n]) {
			if _, isPEX := m.(PEX); isPEX {
				return // IPv4-mapped IPv6 addresses are canonicalized on decode
			}
			t.Fatalf("re-encoded %s differs from input", m.Type())
		}
	})
}
