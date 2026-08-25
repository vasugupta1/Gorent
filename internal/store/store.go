package store

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"

	"github.com/vasugupta1/gorent/internal/bitfield"
	"github.com/vasugupta1/gorent/internal/p2p"
)

type Store struct {
	db *sql.DB
}

func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	createSettingsTableSQL := `CREATE TABLE IF NOT EXISTS settings (
		key TEXT PRIMARY KEY,
		value TEXT
	);`

	if _, err := db.Exec(createSettingsTableSQL); err != nil {
		return nil, fmt.Errorf("failed to create an settings table : %w", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS torrents (
		info_hash TEXT PRIMARY KEY,
		name TEXT,
		length INTEGER,
		piece_length INTEGER,
		piece_hashes BLOB,
		announce TEXT,
		status TEXT,
		bytes_downloaded INTEGER,
		bitfield BLOB,
		state INTEGER DEFAULT 0, 
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	log.Println("Database initialized successfully.")
	return &Store{db: db}, nil
}

func (s *Store) AddTorrent(t *p2p.Torrent) error {
	var hashesBlob bytes.Buffer
	for _, hash := range t.PieceHashes {
		hashesBlob.Write(hash[:])
	}
	bitfield := t.OurBitfield
	if bitfield == nil {
		bitfield = make([]byte, (len(t.PieceHashes)+7)/8)
	}
	t.Mu.Lock()
	t.Status = "Stopped"
	t.State = p2p.StatePaused
	status := t.Status
	state := t.State
	bytesDownloaded := t.BytesDownloaded
	t.Mu.Unlock()

	_, err := s.db.Exec(`
		INSERT INTO torrents (info_hash, name, length, piece_length, piece_hashes, announce, status, bytes_downloaded, bitfield, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(info_hash) DO NOTHING;`, // ON CONFLICT ensures we don't overwrite existing torrents
		fmt.Sprintf("%x", t.InfoHash), t.Name, t.Length, t.PieceLength, hashesBlob.Bytes(),
		t.Announce, status, bytesDownloaded, bitfield, state) // Pass t.State as the value

	return err
}

func (s *Store) LoadTorrents() (map[string]*p2p.Torrent, error) {
	rows, err := s.db.Query(`
		SELECT info_hash, name, length, piece_length, piece_hashes, announce, 
		       status, bytes_downloaded, bitfield 
		FROM torrents ORDER BY updated_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("failed to query torrents: %w", err)
	}
	defer rows.Close()

	torrents := make(map[string]*p2p.Torrent)
	log.Println("Loading torrents from database...")

	for rows.Next() {
		var t p2p.Torrent
		var infoHashStr, loadedStatus string
		var hashesBlob, bitfieldBlob []byte

		if err := rows.Scan(
			&infoHashStr, &t.Name, &t.Length, &t.PieceLength, &hashesBlob, &t.Announce,
			&loadedStatus, &t.BytesDownloaded, &bitfieldBlob,
		); err != nil {
			log.Printf("Error scanning torrent row: %v", err)
			continue
		}

		infoHashBytes, err := hex.DecodeString(infoHashStr)
		if err != nil {
			log.Printf("Error decoding info hash '%s' from DB: %v", infoHashStr, err)
			continue
		}
		copy(t.InfoHash[:], infoHashBytes)

		const hashLen = 20
		numHashes := len(hashesBlob) / hashLen
		t.PieceHashes = make([][20]byte, numHashes)
		for i := 0; i < numHashes; i++ {
			copy(t.PieceHashes[i][:], hashesBlob[i*hashLen:(i+1)*hashLen])
		}

		t.OurBitfield = make(bitfield.Bitfield, len(bitfieldBlob))
		copy(t.OurBitfield, bitfieldBlob)

		if loadedStatus == "Completed" {
			t.Status = "Completed"
			t.State = p2p.StateSeeding // A completed torrent is ready to seed
		} else {
			t.Status = "Stopped"
			t.State = p2p.StatePaused
		}

		log.Printf("  -> Loaded '%s' (Status: %s, Progress: %.2fMB)", t.Name, loadedStatus, float64(t.BytesDownloaded)/(1024*1024))
		torrents[infoHashStr] = &t
	}

	return torrents, nil
}

func (s *Store) UpdateTorrentProgress(t *p2p.Torrent) error {
	torrentSnapShot := t.GetStatus()
	t.Mu.RLock()
	bitfield := t.OurBitfield
	state := t.State
	t.Mu.RUnlock()

	_, err := s.db.Exec(`
		UPDATE torrents SET status = ?, bytes_downloaded = ?, bitfield = ?, state = ?, updated_at = CURRENT_TIMESTAMP
		WHERE info_hash = ?`,
		torrentSnapShot.Status, torrentSnapShot.BytesDownloaded, bitfield, state, fmt.Sprintf("%x", t.InfoHash))

	return err
}

func (s *Store) RemoveTorrent(infoHashStr string) error {
	res, err := s.db.Exec("DELETE FROM torrents WHERE info_hash = ?", infoHashStr)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return errors.New("torrent not found in database")
	}
	return nil
}

func (s *Store) GetTotalDownloaded() (int64, error) {
	var total int64
	err := s.db.QueryRow("SELECT COALESCE(SUM(bytes_downloaded), 0) FROM torrents").Scan(&total)
	return total, err
}

func (s *Store) GetTotalRows() (int64, error) {
	var total int64
	err := s.db.QueryRow("SELECT COUNT(*) FROM torrents").Scan(&total)
	return total, err
}

func (s *Store) GetSettings(key string) (string, error) {
	var value string
	err := s.db.QueryRow("SELECT value FROM settings WHERE key = ?", key).Scan(&value)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	return value, nil
}

func (s *Store) SetSetting(key, value string) error {
	_, err := s.db.Exec(`
	INSERT INTO settings (key,value) VALUES (?,?)
	ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	return err
}

func (s *Store) ClearHistory() error {
	// Using DELETE FROM is safer than DROP TABLE as it just removes data.
	_, err := s.db.Exec("DELETE FROM torrents")
	if err != nil {
		return fmt.Errorf("failed to clear torrent history from database: %w", err)
	}
	// SQLite's VACUUM command reclaims space. Optional but good practice.
	_, err = s.db.Exec("VACUUM")
	return err
}

// NEW: A function to delete all records from the settings table.
func (s *Store) ResetSettings() error {
	_, err := s.db.Exec("DELETE FROM settings")
	return err
}

// Close gracefully closes the database connection.
func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
