package engine

import (
	"log"
	"sync"
	"time"
	"encoding/hex"

	"github.com/vasugupta1/Gorent/internal/client"
	"github.com/vasugupta1/Gorent/internal/db"
	"github.com/vasugupta1/Gorent/internal/magnet"
	"github.com/vasugupta1/Gorent/internal/peer"
)

func (m *TorrentManager) saveLoop() {
	for {
		time.Sleep(2 * time.Second)
		m.mu.Lock()
		for _, c := range m.clients {
			c.Mu.Lock()
			state := db.TorrentState{
				InfoHash:         hex.EncodeToString(c.InfoHash[:]),
				Name:             c.Name,
				MagnetURI:        c.MagnetURI,
				Status:           c.Status,
				DownloadedPieces: append([]byte(nil), c.Bitfield...), // copy
				TotalPieces:      c.TotalPieces,
			}
			c.Mu.Unlock()
			m.db.SaveTorrent(state)
		}
		m.mu.Unlock()
	}
}

type TorrentManager struct {
	mu      sync.Mutex
	clients []*client.Client
	db      *db.DB
	downDir string
}

func NewTorrentManager(dbPath, downloadsDir string) (*TorrentManager, error) {
	database, err := db.InitDB(dbPath)
	if err != nil {
		return nil, err
	}

	m := &TorrentManager{
		clients: make([]*client.Client, 0),
		db:      database,
		downDir: downloadsDir,
	}
	
	// Load existing torrents from db
	states, err := database.GetAllTorrents()
	if err == nil {
		for _, s := range states {
			// Restore client
			mag, err := magnet.Parse(s.MagnetURI)
			if err != nil {
				continue
			}
			myPeerID, _ := peer.GeneratePeerID()
			c := &client.Client{
				Name:        s.Name,
				InfoHash:    mag.InfoHash,
				PeerID:      myPeerID,
				Trackers:    mag.Trackers,
				Port:        6881,
				MagnetURI:   s.MagnetURI,
				Status:      s.Status,
				Bitfield:    s.DownloadedPieces,
				TotalPieces: s.TotalPieces,
			}
			m.clients = append(m.clients, c)
			
			// We only want to resume if it wasn't paused, or wait for explicit resume?
			// The prompt says "read my from the sqllite to understand the state and resume when resume command is given"
			// This means they should start as Paused.
			c.Status = "Paused"
			
			go func(c *client.Client) {
				err := c.Start(downloadsDir)
				if err != nil {
					c.Mu.Lock()
					c.Status = "Error: " + err.Error()
					c.Mu.Unlock()
					log.Printf("Torrent failed: %v", err)
				}
			}(c)
		}
	}
	
	go m.saveLoop()
	return m, nil
}

func (m *TorrentManager) AddMagnet(magnetURI string) error {
	mag, err := magnet.Parse(magnetURI)
	if err != nil {
		return err
	}

	myPeerID, err := peer.GeneratePeerID()
	if err != nil {
		return err
	}

	c := &client.Client{
		Name:      mag.DisplayName,
		InfoHash:  mag.InfoHash,
		PeerID:    myPeerID,
		Trackers:  mag.Trackers,
		Port:      6881, // Default port, we might want to randomize if running many
		MagnetURI: magnetURI,
		Status:    "Queued",
	}

	m.mu.Lock()
	m.clients = append(m.clients, c)
	m.mu.Unlock()

	go func() {
		err := c.Start(m.downDir)
		if err != nil {
			c.Mu.Lock()
			c.Status = "Error: " + err.Error()
			c.Mu.Unlock()
			log.Printf("Torrent failed: %v", err)
		}
	}()

	return nil
}

func (m *TorrentManager) SetDownloadDir(dir string) error {
	m.mu.Lock()
	m.downDir = dir
	m.mu.Unlock()
	return m.db.SaveSetting("download_dir", dir)
}


func (m *TorrentManager) GetClients() []*client.Client {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Return a copy of the slice to avoid race conditions when iterating
	clientsCopy := make([]*client.Client, len(m.clients))
	copy(clientsCopy, m.clients)
	return clientsCopy
}

func (m *TorrentManager) PauseTorrent(index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index >= 0 && index < len(m.clients) {
		m.clients[index].Pause()
	}
}

func (m *TorrentManager) ResumeTorrent(index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index >= 0 && index < len(m.clients) {
		m.clients[index].Resume()
	}
}

func (m *TorrentManager) RemoveTorrent(index int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if index >= 0 && index < len(m.clients) {
		c := m.clients[index]
		c.Remove()
		
		infoHashStr := hex.EncodeToString(c.InfoHash[:])
		m.db.DeleteTorrent(infoHashStr)

		// Remove from slice
		m.clients = append(m.clients[:index], m.clients[index+1:]...)
	}
}
