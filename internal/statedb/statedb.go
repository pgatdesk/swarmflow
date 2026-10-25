// Package statedb stores a peer's durable transfer metadata in SQLite: which
// swarms it is downloading or seeding, and which pieces are persisted.
//
// Only the Persisted state is durable; Missing, Requested and Verifying are
// in-memory states rebuilt on restart. A piece row is inserted only after the
// piece's data was written and flushed (store.WritePiece), so after a crash a
// row always refers to data that reached stable storage, while a piece written
// but not yet recorded is simply downloaded again.
//
// The database runs in WAL mode with synchronous=FULL: each commit syncs the
// WAL before returning, so a committed row survives power loss as far as the
// OS and drive honour flushes.
package statedb

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/pgatdesk/swarmflow/internal/protocol"
)

const schemaVersion = 1

const schema = `
CREATE TABLE IF NOT EXISTS swarms (
	swarm_id     TEXT PRIMARY KEY,
	name         TEXT    NOT NULL,
	size         INTEGER NOT NULL,
	piece_size   INTEGER NOT NULL,
	pieces       INTEGER NOT NULL,
	manifest     BLOB    NOT NULL,
	part_path    TEXT    NOT NULL,
	final_path   TEXT    NOT NULL,
	status       TEXT    NOT NULL,
	created_at   INTEGER NOT NULL,
	updated_at   INTEGER NOT NULL,
	completed_at INTEGER
);
CREATE TABLE IF NOT EXISTS pieces (
	swarm_id     TEXT    NOT NULL REFERENCES swarms(swarm_id) ON DELETE CASCADE,
	idx          INTEGER NOT NULL,
	sha256       BLOB    NOT NULL,
	persisted_at INTEGER NOT NULL,
	PRIMARY KEY (swarm_id, idx)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS peers_seen (
	swarm_id  TEXT    NOT NULL,
	addr      TEXT    NOT NULL,
	peer_id   TEXT    NOT NULL,
	last_seen INTEGER NOT NULL,
	PRIMARY KEY (swarm_id, addr)
) WITHOUT ROWID;
`

const (
	StatusDownloading = "downloading"
	StatusComplete    = "complete"
)

type DB struct{ db *sql.DB }

type Swarm struct {
	ID        protocol.SwarmID
	Name      string
	Size      int64
	PieceSize int
	Pieces    int
	Manifest  []byte // canonical manifest bytes
	PartPath  string
	FinalPath string
	Status    string
	Created   time.Time
	Updated   time.Time
	Completed time.Time
}

// Open opens or creates the database at path.
func Open(path string) (*DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	dsn := "file:" + filepath.ToSlash(path) +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection serialises writers inside this process; other processes
	// (tests, tools) may still read concurrently thanks to WAL.
	db.SetMaxOpenConns(1)
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		db.Close()
		return nil, fmt.Errorf("statedb: %s: %w", path, err)
	}
	if v > schemaVersion {
		db.Close()
		return nil, fmt.Errorf("statedb: %s has schema version %d, newer than supported %d", path, v, schemaVersion)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("statedb: creating schema: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func unix(t time.Time) int64 { return t.UnixMilli() }

func fromUnix(ms sql.NullInt64) time.Time {
	if !ms.Valid {
		return time.Time{}
	}
	return time.UnixMilli(ms.Int64)
}

// UpsertSwarm creates or updates a swarm record, keeping its creation time.
func (d *DB) UpsertSwarm(s Swarm) error {
	now := unix(time.Now())
	_, err := d.db.Exec(`
		INSERT INTO swarms (swarm_id, name, size, piece_size, pieces, manifest, part_path, final_path, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (swarm_id) DO UPDATE SET
			part_path = excluded.part_path, final_path = excluded.final_path,
			status = excluded.status, updated_at = excluded.updated_at`,
		s.ID.String(), s.Name, s.Size, s.PieceSize, s.Pieces, s.Manifest, s.PartPath, s.FinalPath, s.Status, now, now)
	return err
}

// Swarm returns the record for id, if any.
func (d *DB) Swarm(id protocol.SwarmID) (Swarm, bool, error) {
	var s Swarm
	var created, updated, completed sql.NullInt64
	err := d.db.QueryRow(`
		SELECT name, size, piece_size, pieces, manifest, part_path, final_path, status, created_at, updated_at, completed_at
		FROM swarms WHERE swarm_id = ?`, id.String()).
		Scan(&s.Name, &s.Size, &s.PieceSize, &s.Pieces, &s.Manifest, &s.PartPath, &s.FinalPath, &s.Status,
			&created, &updated, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return Swarm{}, false, nil
	}
	if err != nil {
		return Swarm{}, false, err
	}
	s.ID, s.Created, s.Updated, s.Completed = id, fromUnix(created), fromUnix(updated), fromUnix(completed)
	return s, true, nil
}

// MarkPersisted records that piece idx is durably stored. The caller must have
// flushed the piece's data first. It is idempotent.
func (d *DB) MarkPersisted(id protocol.SwarmID, idx int, hash [32]byte) error {
	_, err := d.db.Exec(`INSERT OR REPLACE INTO pieces (swarm_id, idx, sha256, persisted_at) VALUES (?, ?, ?, ?)`,
		id.String(), idx, hash[:], unix(time.Now()))
	return err
}

// PersistedPiece is one durable piece record.
type PersistedPiece struct {
	Index int
	Hash  [32]byte
}

func (d *DB) Persisted(id protocol.SwarmID) ([]PersistedPiece, error) {
	rows, err := d.db.Query(`SELECT idx, sha256 FROM pieces WHERE swarm_id = ? ORDER BY idx`, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PersistedPiece
	for rows.Next() {
		var p PersistedPiece
		var h []byte
		if err := rows.Scan(&p.Index, &h); err != nil {
			return nil, err
		}
		if len(h) != 32 {
			return nil, fmt.Errorf("statedb: piece %d has a %d-byte hash", p.Index, len(h))
		}
		p.Hash = [32]byte(h)
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountPersisted returns the number of persisted pieces of a swarm.
func (d *DB) CountPersisted(id protocol.SwarmID) (int, error) {
	var n int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM pieces WHERE swarm_id = ?`, id.String()).Scan(&n)
	return n, err
}

// Unmark removes piece records (for pieces found invalid on resume) in one transaction.
func (d *DB) Unmark(id protocol.SwarmID, idxs []int) error {
	if len(idxs) == 0 {
		return nil
	}
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, i := range idxs {
		if _, err := tx.Exec(`DELETE FROM pieces WHERE swarm_id = ? AND idx = ?`, id.String(), i); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ClearPieces removes all piece records of a swarm (its data file is gone).
func (d *DB) ClearPieces(id protocol.SwarmID) error {
	_, err := d.db.Exec(`DELETE FROM pieces WHERE swarm_id = ?`, id.String())
	return err
}

func (d *DB) SetStatus(id protocol.SwarmID, status string) error {
	now := unix(time.Now())
	var completed any
	if status == StatusComplete {
		completed = now
	}
	_, err := d.db.Exec(`UPDATE swarms SET status = ?, updated_at = ?, completed_at = COALESCE(?, completed_at) WHERE swarm_id = ?`,
		status, now, completed, id.String())
	return err
}

// RecordPeer remembers a peer address that completed a handshake, for re-discovery after restarts.
func (d *DB) RecordPeer(id protocol.SwarmID, addr string, peer protocol.PeerID, seen time.Time) error {
	_, err := d.db.Exec(`INSERT OR REPLACE INTO peers_seen (swarm_id, addr, peer_id, last_seen) VALUES (?, ?, ?, ?)`,
		id.String(), addr, peer.String(), unix(seen))
	return err
}

// RecentPeers returns up to limit addresses seen since cutoff, most recent first.
func (d *DB) RecentPeers(id protocol.SwarmID, since time.Time, limit int) ([]string, error) {
	rows, err := d.db.Query(`SELECT addr FROM peers_seen WHERE swarm_id = ? AND last_seen >= ? ORDER BY last_seen DESC LIMIT ?`,
		id.String(), unix(since), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
