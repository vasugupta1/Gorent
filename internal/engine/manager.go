package engine

import (
	"log"
	"sync"
	
	"github.com/vasugupta1/Gorent/internal/client"
	"github.com/vasugupta1/Gorent/internal/magnet"
	"github.com/vasugupta1/Gorent/internal/peer"
)

type TorrentManager struct {
	mu      sync.Mutex
	clients []*client.Client
}

func NewTorrentManager() *TorrentManager {
	return &TorrentManager{
		clients: make([]*client.Client, 0),
	}
}

func (m *TorrentManager) AddMagnet(magnetURI string, downloadsDir string) error {
	mag, err := magnet.Parse(magnetURI)
	if err != nil {
		return err
	}

	myPeerID, err := peer.GeneratePeerID()
	if err != nil {
		return err
	}

	c := &client.Client{
		Name:     mag.DisplayName,
		InfoHash: mag.InfoHash,
		PeerID:   myPeerID,
		Trackers: mag.Trackers,
		Port:     6881, // Default port, we might want to randomize if running many
		Status:   "Queued",
	}

	m.mu.Lock()
	m.clients = append(m.clients, c)
	m.mu.Unlock()

	go func() {
		err := c.Start(downloadsDir)
		if err != nil {
			c.Mu.Lock()
			c.Status = "Error: " + err.Error()
			c.Mu.Unlock()
			log.Printf("Torrent failed: %v", err)
		}
	}()

	return nil
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
		// Remove from slice
		m.clients = append(m.clients[:index], m.clients[index+1:]...)
	}
}
