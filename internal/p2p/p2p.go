package p2p

import (
	"sync"
	"time"

	"github.com/vasugupta1/gorent/internal/bitfield"
	"github.com/vasugupta1/gorent/internal/client"
	"github.com/vasugupta1/gorent/internal/peers"
)

const MaxBlockSize = 16384
const MaxBacklog = 5

const (
	StatePaused uint8 = iota
	StateDownloading
	StateSeeding
	StateFailed
	StateStopping
)

type Torrent struct {
	Peers               []peers.Peer
	PeerID              [20]byte
	InfoHash            [20]byte
	PieceHashes         [][20]byte
	PieceLength         int
	Length              int
	Name                string
	Announce            string
	Mu                  sync.RWMutex
	BytesDownloaded     int64
	Status              string
	ActivePeers         int
	State               uint8
	stateCh             chan uint8
	lastBytesDownloaded int64
	DownloadSpeed       float64
	OurBitfield         bitfield.Bitfield
	DownloadPath        string
}

type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

type pieceResult struct {
	index int
	buf   []byte
}

type pieceProgress struct {
	index      int
	client     *client.Client
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

type TorrentSnapshot struct {
	Status          string
	BytesDownloaded int64
	ActivePeers     int
	DownloadSpeed   float64
}

type StoreInterface interface {
	UpdateTorrentProgress(t *Torrent) error
}

func (t *Torrent) GetStatus() TorrentSnapshot {
	t.Mu.RLock()
	defer t.Mu.RUnlock()

	return TorrentSnapshot{
		Status:          t.Status,
		BytesDownloaded: t.BytesDownloaded,
		ActivePeers:     t.ActivePeers,
		DownloadSpeed:   t.DownloadSpeed,
	}
}

func (t *Torrent) startSpeedCalculator() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		t.Mu.Lock()
		if t.Status != "Downloading" {
			t.DownloadSpeed = 0
			t.lastBytesDownloaded = t.BytesDownloaded
			t.Mu.Unlock()
			continue
		}

		bytesSinceLastTick := t.BytesDownloaded - t.lastBytesDownloaded
		t.DownloadSpeed = float64(bytesSinceLastTick) / 2
		t.lastBytesDownloaded = t.BytesDownloaded
		t.Mu.Unlock()
	}
}

func (t *Torrent) calculatePieceSize(index int) int {
	begin, end := t.calculateBoundsForPiece(index)
	return end - begin
}

func (t *Torrent) calculateBoundsForPiece(index int) (begin, end int) {
	begin = index * t.PieceLength
	end = begin + t.PieceLength
	if end > t.Length {
		end = t.Length
	}
	return begin, end
}
