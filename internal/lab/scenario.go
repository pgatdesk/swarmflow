// Package lab runs reproducible network experiments: every peer is a real
// swarmflow process in its own Linux network namespace, with its access link
// shaped by tc netem (decision 0005). It needs Linux and root.
package lab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration is a time.Duration that reads from JSON strings like "1.5s".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// Link is one peer's access link, applied in both directions: upload on the
// peer side of its veth pair, download on the hub side.
type Link struct {
	Rate   string `json:"rate"`             // tc rate, e.g. "20mbit"
	Delay  string `json:"delay"`            // one-way delay, e.g. "15ms"
	Jitter string `json:"jitter,omitempty"` // e.g. "5ms"
	Loss   string `json:"loss,omitempty"`   // percent, e.g. "1"
	// Limit is the netem queue length in packets (default 1000).
	Limit int `json:"limit,omitempty"`
}

// RateBits parses Rate into bits per second.
func (l Link) RateBits() (float64, error) {
	s := strings.ToLower(strings.TrimSpace(l.Rate))
	mult := 1.0
	for _, u := range []struct {
		suffix string
		m      float64
	}{{"gbit", 1e9}, {"mbit", 1e6}, {"kbit", 1e3}, {"bit", 1}} {
		if strings.HasSuffix(s, u.suffix) {
			s, mult = strings.TrimSuffix(s, u.suffix), u.m
			break
		}
	}
	var v float64
	if _, err := fmt.Sscanf(s, "%g", &v); err != nil || v <= 0 {
		return 0, fmt.Errorf("bad rate %q (want e.g. 20mbit)", l.Rate)
	}
	return v * mult, nil
}

func (l Link) netemArgs() []string {
	args := []string{"delay", l.Delay}
	if l.Jitter != "" {
		args = append(args, l.Jitter, "distribution", "normal")
	}
	args = append(args, "rate", l.Rate)
	if l.Loss != "" {
		args = append(args, "loss", l.Loss+"%")
	}
	limit := l.Limit
	if limit == 0 {
		limit = 1000
	}
	return append(args, "limit", fmt.Sprint(limit))
}

type PeerSpec struct {
	Name string `json:"name"`
	// Role is seed, get or replica.
	Role string `json:"role"`
	Link Link   `json:"link"`
	// Has selects which pieces a seed's file contains: "all" (default), "even",
	// "odd", or "first:N%" / "last:N%"; or "corrupt:N%" for a complete file with
	// N% of its pieces damaged, served without startup verification.
	Has     string   `json:"has,omitempty"`
	StartAt Duration `json:"start_at,omitzero"`
	// Manual peers are started only by "start" events.
	Manual bool     `json:"manual,omitempty"`
	Args   []string `json:"args,omitempty"`
}

type Event struct {
	At   Duration `json:"at"`
	Peer string   `json:"peer"`
	// Action is kill (SIGKILL), stop (SIGINT), start (start again, or for the
	// first time if manual; a restarted downloader resumes from its state DB),
	// link-down, link-up or set-link.
	Action string `json:"action"`
	Link   *Link  `json:"link,omitempty"`
}

type Variant struct {
	Name string   `json:"name"`
	Args []string `json:"args"` // added to every downloader's command line
	// RoleArgs adds arguments to peers by role ("seed", "get", "replica", or "*" for all).
	RoleArgs map[string][]string `json:"role_args,omitempty"`
}

// argsFor returns the variant's extra arguments for a peer role.
func (v Variant) argsFor(role string) []string {
	var out []string
	if role == "get" {
		out = append(out, v.Args...)
	}
	out = append(out, v.RoleArgs["*"]...)
	return append(out, v.RoleArgs[role]...)
}

type Scenario struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	FileSize    string     `json:"file_size"`
	PieceSize   string     `json:"piece_size"`
	DataSeed    uint64     `json:"data_seed"`
	Timeout     Duration   `json:"timeout"`
	CommonArgs  []string   `json:"common_args,omitempty"` // added to every peer
	Peers       []PeerSpec `json:"peers"`
	Events      []Event    `json:"events,omitempty"`
	Variants    []Variant  `json:"variants"`
}

func LoadScenario(path string) (*Scenario, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var sc Scenario
	if err := dec.Decode(&sc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := sc.validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &sc, nil
}

func (sc *Scenario) validate() error {
	if sc.Name == "" || sc.FileSize == "" || len(sc.Peers) == 0 || len(sc.Variants) == 0 {
		return fmt.Errorf("name, file_size, peers and variants are required")
	}
	if sc.PieceSize == "" {
		sc.PieceSize = "256KiB"
	}
	if sc.Timeout.Duration == 0 {
		sc.Timeout.Duration = 5 * time.Minute
	}
	if len(sc.Peers) > 200 {
		return fmt.Errorf("at most 200 peers")
	}
	names := map[string]bool{}
	gets := 0
	for _, p := range sc.Peers {
		if p.Name == "" || names[p.Name] {
			return fmt.Errorf("peer names must be unique and non-empty (%q)", p.Name)
		}
		names[p.Name] = true
		switch p.Role {
		case "get":
			gets++
		case "seed", "replica":
		default:
			return fmt.Errorf("peer %s: role must be seed, get or replica", p.Name)
		}
		if _, err := p.Link.RateBits(); err != nil {
			return fmt.Errorf("peer %s: %w", p.Name, err)
		}
		if _, err := time.ParseDuration(p.Link.Delay); err != nil {
			return fmt.Errorf("peer %s: delay: %w", p.Name, err)
		}
	}
	if gets == 0 {
		return fmt.Errorf("at least one peer must have role get")
	}
	for _, e := range sc.Events {
		if !names[e.Peer] {
			return fmt.Errorf("event for unknown peer %q", e.Peer)
		}
		switch e.Action {
		case "kill", "stop", "start", "link-down", "link-up":
		case "set-link":
			if e.Link == nil {
				return fmt.Errorf("set-link event needs link")
			}
		default:
			return fmt.Errorf("unknown event action %q", e.Action)
		}
	}
	return nil
}
