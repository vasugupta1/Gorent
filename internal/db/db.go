package db

import (
	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	conn *sql.DB
}

type TorrentState struct {
	InfoHash         string
	Name             string
	MagnetURI        string
	Status           string
	DownloadedPieces []byte
	TotalPieces      int
}

func InitDB(filepath string) (*DB, error) {
	conn, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}

	createTableQuery := `
	CREATE TABLE IF NOT EXISTS torrents (
		info_hash TEXT PRIMARY KEY,
		name TEXT,
		magnet_uri TEXT,
		status TEXT,
		downloaded_pieces BLOB,
		total_pieces INTEGER
	);`
	if _, err := conn.Exec(createTableQuery); err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (db *DB) SaveTorrent(state TorrentState) error {
	query := `
	INSERT INTO torrents (info_hash, name, magnet_uri, status, downloaded_pieces, total_pieces)
	VALUES (?, ?, ?, ?, ?, ?)
	ON CONFLICT(info_hash) DO UPDATE SET
		name=excluded.name,
		status=excluded.status,
		downloaded_pieces=excluded.downloaded_pieces,
		total_pieces=excluded.total_pieces;
	`
	_, err := db.conn.Exec(query, state.InfoHash, state.Name, state.MagnetURI, state.Status, state.DownloadedPieces, state.TotalPieces)
	return err
}

func (db *DB) GetAllTorrents() ([]TorrentState, error) {
	rows, err := db.conn.Query("SELECT info_hash, name, magnet_uri, status, downloaded_pieces, total_pieces FROM torrents")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var torrents []TorrentState
	for rows.Next() {
		var ts TorrentState
		if err := rows.Scan(&ts.InfoHash, &ts.Name, &ts.MagnetURI, &ts.Status, &ts.DownloadedPieces, &ts.TotalPieces); err != nil {
			return nil, err
		}
		torrents = append(torrents, ts)
	}
	return torrents, nil
}

func (db *DB) DeleteTorrent(infoHash string) error {
	_, err := db.conn.Exec("DELETE FROM torrents WHERE info_hash = ?", infoHash)
	return err
}
