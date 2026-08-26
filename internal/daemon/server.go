package daemon

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	dht "github.com/anacrolix/dht/v2"
	"github.com/vasugupta1/gorent/internal/p2p"
	"github.com/vasugupta1/gorent/internal/peers"
	"github.com/vasugupta1/gorent/internal/store"
	"github.com/vasugupta1/gorent/internal/torrent"
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
		startingNodes := func() ([]dht.Addr, error) {
			return dht.GlobalBootstrapAddrs("udp")
		}
		dhtConfig := dht.ServerConfig{Conn: conn, StartingNodes: startingNodes}
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

func (s *Server) AddTorrent(args *AddTorrentArgs, reply *AddTorrentReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Daemon: Received request to add torrent: %s", args.PathOrMagnet)

	var tf torrent.TorrentFile
	var err error

	if strings.HasPrefix(args.PathOrMagnet, "magnet:") {
		return errors.New("magnet links not yet supported")
	} else {
		tf, err = torrent.Open(args.PathOrMagnet)
		if err != nil {
			return fmt.Errorf("failed to process input: %w", err)
		}
	}

	infoHashStr := fmt.Sprintf("%x", tf.InfoHash)
	if _, ok := s.torrents[infoHashStr]; ok {
		return errors.New("torrent is already being managed")
	}

	var peerId [20]byte
	if _, err := rand.Read(peerId[:]); err != nil {
		return err
	}

	peers, err := findPeers(tf, peerId)
	if err != nil {
		return err
	}

	if len(peers) <= 0 {
		return errors.New("failed to get any peers for this torrent")
	}

	torrentJob := &p2p.Torrent{
		Peers:        peers,
		PeerID:       peerId,
		InfoHash:     tf.InfoHash,
		PieceHashes:  tf.PieceHashes,
		PieceLength:  tf.PieceLength,
		Length:       tf.Length,
		Name:         tf.Name,
		Announce:     tf.Announce,
		DownloadPath: s.downloadPath,
	}

	if err := s.store.AddTorrent(torrentJob); err != nil {
		return fmt.Errorf("failed to save torrent to database: %w", err)
	}
	s.torrents[infoHashStr] = torrentJob

	go torrentJob.DownloadTheFile(s.store)

	reply.InfoHash = infoHashStr
	reply.Name = torrentJob.Name

	return nil
}

func findPeers(tf torrent.TorrentFile, peerId [20]byte) ([]peers.Peer, error) {
	peers, err := findTrackerPeers(tf, peerId)
	if err != nil {
		return nil, err
	}

	if s.dhtEnabled && s.dht != nil {
		dhtPeers, err := findTrackersPeersDHT(tf)
	}
	return peers, nil
}

func findTrackersPeersDHT(tf torrent.TorrentFile) ([]peers.Peer, error) {

}

func findTrackerPeers(tf torrent.TorrentFile, peerId [20]byte) ([]peers.Peer, error) {

	var trackerList []string
	if len(tf.AnnounceList) > 0 {
		trackerList = tf.AnnounceList
	} else if tf.Announce != "" {
		trackerList = []string{tf.Announce}
	} else {
		return nil, errors.New("no tracker URLs available for this torrent")
	}

	peerChan := make(chan peers.Peer)
	var wg sync.WaitGroup

	for _, trackerURL := range trackerList {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			tempTF := tf
			tempTF.Announce = url
			trackerPeers, err := torrent.RequestPeers(&tempTF, peerId)
			if err != nil {
				log.Printf("Tracker %s failed: %v", url, err)
				return
			}
			if len(trackerPeers) <= 0 {
				log.Printf("No Trackers Peers %s found: %v", url, err)
				return
			}
			for _, tp := range trackerPeers {
				peerChan <- tp
			}

		}(trackerURL)
	}

	go func() {
		wg.Wait()
		close(peerChan)
	}()

	var deDupPeers []peers.Peer
	peerMap := make(map[string]bool)
	for p := range peerChan {
		key := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
		if !peerMap[key] {
			peerMap[key] = true
			deDupPeers = append(deDupPeers, p)
		}
	}

	if len(deDupPeers) <= 0 {
		return nil, errors.New("No peers found for any trackers")
	}

	return deDupPeers, nil
}
