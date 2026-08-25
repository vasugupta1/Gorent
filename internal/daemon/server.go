package daemon

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/vasugupta1/gorent/internal/p2p"
	"github.com/vasugupta1/gorent/internal/store"
)

type AddTorrentArgs struct {
	PathOrMagnet string
}

type AddTorrentReply struct {
	InfoHash string
	Name     string
}

type PauseResumeArgs struct {
	InfoHash string
}

type TorrentStatus struct {
	Name            string
	InfoHash        string
	TotalLength     int
	BytesDownloaded int64
	ActivePeers     int
	Status          string
	DownloadSpeed   float64
	UploadSpeed     float64
	Updated_at      time.Time
	ETA             int
}

type ListTorrentsReply struct {
	Torrents map[string]TorrentStatus
}

type RemoveTorrentArgs struct {
	InfoHash   string
	DeleteData bool
}

type Server struct {
	torrents     map[string]*p2p.Torrent
	store        *store.Store
	mu           sync.Mutex
	dht          *dht.Server
	dhtEnabled   bool
	downloadPath string
	broadcaster  Broadcaster // Use the interface type
}

type Broadcaster interface {
	BroadcastProgress()
}

type GlobalStats struct {
	TotalDownloadSpeed float64
	TotalUploadSpeed   float64
	TotalDownloaded    int64
	ActivePeers        int
}

type QuickStatsReply struct {
	TotalDownloaded    int64   `json:"totalDownloaded"`
	ActiveDownloads    int     `json:"activeDownloads"`
	TotalDownloadSpeed float64 `json:"totalDownloadSpeed"`
	TotalRows          int64   `json:"totalRows"`
}

type SetDownloadPathArgs struct {
	Path string
}

type GetDownloadPathReply struct {
	Path string
}

func NewServer(dbPath string, b Broadcaster) (*Server, error) {
	store, err := store.NewStore(dbPath)
	if err != nil {
		return nil, err
	}
	path, err := store.GetSettings("download_path") // Corrected typo: GetSetting
	if err != nil {
		return nil, fmt.Errorf("failed to get download path: %w", err)
	}
	if path == "" {
		path = "./downloads"
		log.Printf("No download path set. Using default: %s", path)
		if err := store.SetSetting("download_path", path); err != nil {
			return nil, fmt.Errorf("failed to save default path: %w", err)
		}
	}
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create download directory: %w", err)
	}
	log.Printf("Using download path: %s", path)

	dhtEnabledStr, err := store.GetSettings("dht_enabled")
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get dht setting: %w", err)
	}
	// Default to 'true' if the setting doesn't exist yet
	dhtEnabled := dhtEnabledStr != "false"

	var dhtServer *dht.Server

	if dhtEnabled {
		conn, err := net.ListenPacket("udp", ":0")
		if err != nil {
			return nil, fmt.Errorf("failed to create UDP connection for DHT: %w", err)
		}
		dhtConfig := dht.ServerConfig{Conn: conn, StartingNodes: dht.GlobalBootstrapAddrs}
		dhtServer, err = dht.NewServer(&dhtConfig)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to create DHT server: %w", err)
		}
		log.Println("✅ DHT is ENABLED and server has started.")
	} else {
		log.Println("DHT is DISABLED by user setting.")
	}

	loadedTorrents, err := store.LoadTorrents()
	if err != nil {
		dhtServer.Close()
		return nil, fmt.Errorf("failed to load torrents from database: %w", err)
	}
	log.Printf("Loaded %d torrents from the database.", len(loadedTorrents))

	server := &Server{
		torrents:     loadedTorrents,
		store:        store,
		dht:          dhtServer,
		dhtEnabled:   dhtEnabled,
		downloadPath: path,
		broadcaster:  b,
	}

	return server, nil
}
