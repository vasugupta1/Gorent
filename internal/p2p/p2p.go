package p2p

import (
	"sync"

	"github.com/vasugupta1/gorent/internal/bitfield"
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
