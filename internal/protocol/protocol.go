// Package protocol implements the SwarmFlow v1 wire protocol: a fixed 64-byte
// handshake followed by length-prefixed, typed frames. The specification lives
// in docs/design/architecture.md §3.
package protocol

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
)

const (
	Magic        = "SWFL"
	VersionMajor = 1
	VersionMinor = 0
	HandshakeLen = 64

	// MaxPieceSize is the largest piece a manifest may declare.
	MaxPieceSize = 1 << 20
	// MaxFrame bounds a frame's length field: the largest PIECE message plus headroom
	// for its fixed fields. Anything larger is rejected before allocating.
	MaxFrame = MaxPieceSize + 64
	// MaxPEXPeers bounds the number of addresses in one PEX message.
	MaxPEXPeers = 50
)

var (
	ErrBadMagic      = errors.New("protocol: bad handshake magic")
	ErrEmptyFrame    = errors.New("protocol: empty frame")
	ErrFrameTooLarge = errors.New("protocol: frame exceeds maximum size")
	ErrUnknownType   = errors.New("protocol: unknown message type")
	ErrMalformed     = errors.New("protocol: malformed message")
)

// PeerID identifies a peer process for its lifetime. It is random, not authenticated.
type PeerID [16]byte

func (p PeerID) String() string { return hex.EncodeToString(p[:]) }

// Short returns the first 8 hex digits, for logs and dashboards.
func (p PeerID) Short() string { return hex.EncodeToString(p[:4]) }

func ParsePeerID(s string) (PeerID, error) {
	var id PeerID
	if err := decodeHex(id[:], s); err != nil {
		return id, fmt.Errorf("peer id: %w", err)
	}
	return id, nil
}

// SwarmID is the SHA-256 of a manifest's canonical encoding.
type SwarmID [32]byte

func (s SwarmID) String() string { return hex.EncodeToString(s[:]) }
func (s SwarmID) Short() string  { return hex.EncodeToString(s[:4]) }

func ParseSwarmID(s string) (SwarmID, error) {
	var id SwarmID
	if err := decodeHex(id[:], s); err != nil {
		return id, fmt.Errorf("swarm id: %w", err)
	}
	return id, nil
}

func decodeHex(dst []byte, s string) error {
	if len(s) != 2*len(dst) {
		return fmt.Errorf("want %d hex digits, got %d", 2*len(dst), len(s))
	}
	_, err := hex.Decode(dst, []byte(s))
	return err
}

// Handshake flags.
const (
	// FlagAcceptReplicas advertises that the peer accepts REPL_OFFERs.
	FlagAcceptReplicas uint16 = 1 << 0
)

// Handshake is exchanged once in each direction before any frame.
type Handshake struct {
	Major, Minor uint8
	Flags        uint16
	Swarm        SwarmID
	Peer         PeerID
	ListenPort   uint16
}

func (h Handshake) Marshal() [HandshakeLen]byte {
	var b [HandshakeLen]byte
	copy(b[0:4], Magic)
	b[4] = h.Major
	b[5] = h.Minor
	binary.BigEndian.PutUint16(b[6:8], h.Flags)
	copy(b[8:40], h.Swarm[:])
	copy(b[40:56], h.Peer[:])
	binary.BigEndian.PutUint16(b[56:58], h.ListenPort)
	return b
}

func WriteHandshake(w io.Writer, h Handshake) error {
	b := h.Marshal()
	_, err := w.Write(b[:])
	return err
}

func ReadHandshake(r io.Reader) (Handshake, error) {
	var b [HandshakeLen]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return Handshake{}, err
	}
	if string(b[0:4]) != Magic {
		return Handshake{}, ErrBadMagic
	}
	var h Handshake
	h.Major = b[4]
	h.Minor = b[5]
	h.Flags = binary.BigEndian.Uint16(b[6:8])
	copy(h.Swarm[:], b[8:40])
	copy(h.Peer[:], b[40:56])
	h.ListenPort = binary.BigEndian.Uint16(b[56:58])
	return h, nil
}

// Type is the one-byte message type that follows a frame's length field.
type Type uint8

const (
	TypeBitfield  Type = 0x01
	TypeHave      Type = 0x02
	TypeRequest   Type = 0x03
	TypePiece     Type = 0x04
	TypeCancel    Type = 0x05
	TypeReject    Type = 0x06
	TypePing      Type = 0x07
	TypePong      Type = 0x08
	TypePEX       Type = 0x09
	TypeChallenge Type = 0x0A
	TypeProof     Type = 0x0B
	TypeReplOffer Type = 0x0C
	TypeReplReply Type = 0x0D
	TypeKeepAlive Type = 0x0E
	TypeBye       Type = 0x0F
)

var typeNames = map[Type]string{
	TypeBitfield: "BITFIELD", TypeHave: "HAVE", TypeRequest: "REQUEST", TypePiece: "PIECE",
	TypeCancel: "CANCEL", TypeReject: "REJECT", TypePing: "PING", TypePong: "PONG", TypePEX: "PEX",
	TypeChallenge: "CHALLENGE", TypeProof: "PROOF", TypeReplOffer: "REPL_OFFER",
	TypeReplReply: "REPL_REPLY", TypeKeepAlive: "KEEPALIVE", TypeBye: "BYE",
}

func (t Type) String() string {
	if s, ok := typeNames[t]; ok {
		return s
	}
	return fmt.Sprintf("Type(0x%02x)", uint8(t))
}

// RejectReason explains a REJECT.
type RejectReason uint8

const (
	RejectNotHave      RejectReason = 1
	RejectBusy         RejectReason = 2
	RejectShuttingDown RejectReason = 3
)

func (r RejectReason) String() string {
	switch r {
	case RejectNotHave:
		return "not_have"
	case RejectBusy:
		return "busy"
	case RejectShuttingDown:
		return "shutting_down"
	}
	return fmt.Sprintf("reject(%d)", uint8(r))
}

// ByeReason explains why a connection is being closed.
type ByeReason uint8

const (
	ByeNormal ByeReason = iota
	ByeShutdown
	ByeProtocolError
	ByeIncompatibleVersion
	ByeUnknownSwarm
	ByeDuplicate
	ByeOverloaded
	ByeTimeout
	ByeSelf
	ByeBanned
)

var byeNames = [...]string{"normal", "shutdown", "protocol_error", "incompatible_version",
	"unknown_swarm", "duplicate", "overloaded", "timeout", "self", "banned"}

func (b ByeReason) String() string {
	if int(b) < len(byeNames) {
		return byeNames[b]
	}
	return fmt.Sprintf("bye(%d)", uint8(b))
}

// Message is any decoded frame.
type Message interface{ Type() Type }

type (
	Bitfield struct{ Bits []byte }
	Have     struct{ Piece uint32 }
	Request  struct{ ReqID, Piece uint32 }
	Piece    struct {
		ReqID, Piece uint32
		Data         []byte
	}
	Cancel struct{ ReqID uint32 }
	Reject struct {
		ReqID  uint32
		Reason RejectReason
	}
	Ping      struct{ Nonce uint64 }
	Pong      struct{ Nonce uint64 }
	PEX       struct{ Peers []netip.AddrPort }
	Challenge struct {
		Piece uint32
		Nonce [16]byte
	}
	Proof struct {
		Piece  uint32
		Nonce  [16]byte
		Digest [32]byte
	}
	ReplOffer struct{ Piece uint32 }
	ReplReply struct {
		Piece  uint32
		Accept bool
	}
	KeepAlive struct{}
	Bye       struct{ Reason ByeReason }
)

func (Bitfield) Type() Type  { return TypeBitfield }
func (Have) Type() Type      { return TypeHave }
func (Request) Type() Type   { return TypeRequest }
func (Piece) Type() Type     { return TypePiece }
func (Cancel) Type() Type    { return TypeCancel }
func (Reject) Type() Type    { return TypeReject }
func (Ping) Type() Type      { return TypePing }
func (Pong) Type() Type      { return TypePong }
func (PEX) Type() Type       { return TypePEX }
func (Challenge) Type() Type { return TypeChallenge }
func (Proof) Type() Type     { return TypeProof }
func (ReplOffer) Type() Type { return TypeReplOffer }
func (ReplReply) Type() Type { return TypeReplReply }
func (KeepAlive) Type() Type { return TypeKeepAlive }
func (Bye) Type() Type       { return TypeBye }

var be = binary.BigEndian

// encode appends m's fixed fields to dst and returns any variable-length tail
// separately, so large PIECE payloads are written without an extra copy.
func encode(dst []byte, m Message) (fixed, tail []byte, err error) {
	switch m := m.(type) {
	case Bitfield:
		return dst, m.Bits, nil
	case Have:
		return be.AppendUint32(dst, m.Piece), nil, nil
	case Request:
		return be.AppendUint32(be.AppendUint32(dst, m.ReqID), m.Piece), nil, nil
	case Piece:
		return be.AppendUint32(be.AppendUint32(dst, m.ReqID), m.Piece), m.Data, nil
	case Cancel:
		return be.AppendUint32(dst, m.ReqID), nil, nil
	case Reject:
		return append(be.AppendUint32(dst, m.ReqID), byte(m.Reason)), nil, nil
	case Ping:
		return be.AppendUint64(dst, m.Nonce), nil, nil
	case Pong:
		return be.AppendUint64(dst, m.Nonce), nil, nil
	case PEX:
		if len(m.Peers) > MaxPEXPeers {
			return nil, nil, fmt.Errorf("%w: PEX with %d peers (max %d)", ErrMalformed, len(m.Peers), MaxPEXPeers)
		}
		dst = append(dst, byte(len(m.Peers)))
		for _, ap := range m.Peers {
			ip := ap.Addr().As16()
			dst = be.AppendUint16(append(dst, ip[:]...), ap.Port())
		}
		return dst, nil, nil
	case Challenge:
		return append(be.AppendUint32(dst, m.Piece), m.Nonce[:]...), nil, nil
	case Proof:
		dst = append(be.AppendUint32(dst, m.Piece), m.Nonce[:]...)
		return append(dst, m.Digest[:]...), nil, nil
	case ReplOffer:
		return be.AppendUint32(dst, m.Piece), nil, nil
	case ReplReply:
		var a byte
		if m.Accept {
			a = 1
		}
		return append(be.AppendUint32(dst, m.Piece), a), nil, nil
	case KeepAlive:
		return dst, nil, nil
	case Bye:
		return append(dst, byte(m.Reason)), nil, nil
	}
	return nil, nil, fmt.Errorf("%w: cannot encode %T", ErrUnknownType, m)
}

// WriteMessage writes one frame. Callers should pass a buffered writer.
func WriteMessage(w io.Writer, m Message) error {
	var buf [64]byte
	fixed, tail, err := encode(buf[:5], m)
	if err != nil {
		return err
	}
	n := len(fixed) - 4 + len(tail)
	if n > MaxFrame {
		return fmt.Errorf("%w: %s of %d bytes", ErrFrameTooLarge, m.Type(), n)
	}
	be.PutUint32(fixed[0:4], uint32(n))
	fixed[4] = byte(m.Type())
	if _, err := w.Write(fixed); err != nil {
		return err
	}
	if len(tail) > 0 {
		_, err = w.Write(tail)
	}
	return err
}

// ReadMessage reads and decodes one frame. The length is validated before the
// body is allocated, so a hostile peer cannot force a large allocation.
func ReadMessage(r io.Reader) (Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := be.Uint32(hdr[:])
	if n == 0 {
		return nil, ErrEmptyFrame
	}
	if n > MaxFrame {
		return nil, fmt.Errorf("%w: length %d", ErrFrameTooLarge, n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return Decode(Type(body[0]), body[1:])
}

// Decode parses a frame payload. Fixed-size messages must have exactly the
// expected length; the returned slices alias p.
func Decode(t Type, p []byte) (Message, error) {
	need := func(n int) error {
		if len(p) != n {
			return fmt.Errorf("%w: %s payload is %d bytes, want %d", ErrMalformed, t, len(p), n)
		}
		return nil
	}
	switch t {
	case TypeBitfield:
		return Bitfield{Bits: p}, nil
	case TypeHave:
		if err := need(4); err != nil {
			return nil, err
		}
		return Have{Piece: be.Uint32(p)}, nil
	case TypeRequest:
		if err := need(8); err != nil {
			return nil, err
		}
		return Request{ReqID: be.Uint32(p), Piece: be.Uint32(p[4:])}, nil
	case TypePiece:
		if len(p) < 8 {
			return nil, fmt.Errorf("%w: PIECE payload is %d bytes", ErrMalformed, len(p))
		}
		return Piece{ReqID: be.Uint32(p), Piece: be.Uint32(p[4:]), Data: p[8:]}, nil
	case TypeCancel:
		if err := need(4); err != nil {
			return nil, err
		}
		return Cancel{ReqID: be.Uint32(p)}, nil
	case TypeReject:
		if err := need(5); err != nil {
			return nil, err
		}
		return Reject{ReqID: be.Uint32(p), Reason: RejectReason(p[4])}, nil
	case TypePing, TypePong:
		if err := need(8); err != nil {
			return nil, err
		}
		if t == TypePing {
			return Ping{Nonce: be.Uint64(p)}, nil
		}
		return Pong{Nonce: be.Uint64(p)}, nil
	case TypePEX:
		if len(p) < 1 {
			return nil, fmt.Errorf("%w: empty PEX", ErrMalformed)
		}
		count := int(p[0])
		if count > MaxPEXPeers {
			return nil, fmt.Errorf("%w: PEX with %d peers (max %d)", ErrMalformed, count, MaxPEXPeers)
		}
		if err := need(1 + 18*count); err != nil {
			return nil, err
		}
		peers := make([]netip.AddrPort, count)
		for i := range peers {
			e := p[1+18*i:]
			peers[i] = netip.AddrPortFrom(netip.AddrFrom16([16]byte(e[:16])).Unmap(), be.Uint16(e[16:18]))
		}
		return PEX{Peers: peers}, nil
	case TypeChallenge:
		if err := need(20); err != nil {
			return nil, err
		}
		return Challenge{Piece: be.Uint32(p), Nonce: [16]byte(p[4:20])}, nil
	case TypeProof:
		if err := need(52); err != nil {
			return nil, err
		}
		return Proof{Piece: be.Uint32(p), Nonce: [16]byte(p[4:20]), Digest: [32]byte(p[20:52])}, nil
	case TypeReplOffer:
		if err := need(4); err != nil {
			return nil, err
		}
		return ReplOffer{Piece: be.Uint32(p)}, nil
	case TypeReplReply:
		if err := need(5); err != nil {
			return nil, err
		}
		if p[4] > 1 {
			return nil, fmt.Errorf("%w: REPL_REPLY accept byte %d", ErrMalformed, p[4])
		}
		return ReplReply{Piece: be.Uint32(p), Accept: p[4] == 1}, nil
	case TypeKeepAlive:
		if err := need(0); err != nil {
			return nil, err
		}
		return KeepAlive{}, nil
	case TypeBye:
		if err := need(1); err != nil {
			return nil, err
		}
		return Bye{Reason: ByeReason(p[0])}, nil
	}
	return nil, fmt.Errorf("%w: 0x%02x", ErrUnknownType, uint8(t))
}
