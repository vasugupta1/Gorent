package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vasugupta1/Gorent/internal/metadata"
	"github.com/vasugupta1/Gorent/internal/peer"
	"github.com/vasugupta1/Gorent/internal/storage"
	"github.com/vasugupta1/Gorent/internal/tracker"
)

type Client struct {
	InfoHash [20]byte
	PeerID   [20]byte
	Trackers []string
	Port     uint16

	TorrentInfo *metadata.TorrentInfo
	Storage     *storage.Storage
}

// Start begins the torrent download process.
func (c *Client) Start(downloadsDir string) error {
	// 1. Get Peers from Trackers concurrently
	var peers []peer.Peer
	var peerMutex sync.Mutex
	var trWg sync.WaitGroup

	for _, trURL := range c.Trackers {
		trWg.Add(1)
		go func(url string) {
			defer trWg.Done()
			t, err := tracker.NewTracker(url)
			if err != nil {
				return
			}
			p, err := t.Announce(c.InfoHash, c.PeerID, c.Port, 0, 0, 0)
			if err == nil {
				peerMutex.Lock()
				peers = append(peers, p...)
				peerMutex.Unlock()
			}
		}(trURL)
	}

	// Wait for a reasonable time for trackers, don't wait for all if some hang
	// We'll wait up to 5 seconds.
	done := make(chan struct{})
	go func() {
		trWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All trackers responded.")
	case <-time.After(5 * time.Second):
		log.Println("Timeout waiting for trackers. Proceeding with found peers...")
	}
	log.Printf("Found %d peers. Attempting to fetch metadata...", len(peers))

	metaChan := make(chan []byte)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(pr peer.Peer) {
			defer wg.Done()

			// Check if we've already found metadata before trying
			select {
			case <-ctx.Done():
				return
			default:
			}

			meta, err := metadata.FetchMetadata(pr, c.InfoHash, c.PeerID)
			if err == nil {
				select {
				case metaChan <- meta:
					log.Printf("Successfully fetched metadata from %s", pr.String())
				case <-ctx.Done():
					// Another goroutine already succeeded
				}
			}
		}(p)
	}

	// Close the channel once all workers finish (if they all fail)
	go func() {
		wg.Wait()
		close(metaChan)
	}()

	var rawMetadata []byte
	select {
	case meta, ok := <-metaChan:
		if ok && meta != nil {
			rawMetadata = meta
			cancel() // Cancel other metadata fetchers
		}
	}

	if rawMetadata == nil {
		return fmt.Errorf("failed to fetch metadata from any peer")
	}

	// 3. Parse Metadata and Initialize Storage
	var err error
	c.TorrentInfo, err = metadata.ParseTorrentInfo(rawMetadata)
	if err != nil {
		return fmt.Errorf("failed to parse metadata: %v", err)
	}

	log.Printf("Parsed Metadata: Name=%s, Length=%d, PieceLength=%d", c.TorrentInfo.Name, c.TorrentInfo.Length, c.TorrentInfo.PieceLength)

	c.Storage, err = storage.NewStorage(downloadsDir, c.TorrentInfo)
	if err != nil {
		return fmt.Errorf("failed to initialize storage: %v", err)
	}
	defer c.Storage.Close()

	// 4. Download Pieces Concurrently
	hashes := c.TorrentInfo.PieceHashes()
	numPieces := len(hashes)
	log.Printf("Starting concurrent download of %d pieces...", numPieces)

	workQueue := make(chan pieceWork, numPieces)
	results := make(chan pieceResult)

	// Populate the work queue
	for i, hash := range hashes {
		pieceLen := c.TorrentInfo.PieceLength
		if i == numPieces-1 {
			pieceLen = c.TorrentInfo.Length % c.TorrentInfo.PieceLength
			if pieceLen == 0 {
				pieceLen = c.TorrentInfo.PieceLength
			}
		}
		workQueue <- pieceWork{index: i, hash: hash, length: pieceLen}
	}

	// Start workers for each peer
	log.Printf("Launching %d workers...", len(peers))
	for _, p := range peers {
		go peerWorker(p, c.InfoHash, c.PeerID, workQueue, results)
	}

	// Collect results
	donePieces := 0
	for donePieces < numPieces {
		res := <-results

		// Verify hash
		err := c.Storage.WritePiece(res.index, c.TorrentInfo.PieceLength, res.buf, hashes[res.index])
		if err != nil {
			log.Printf("Piece %d failed hash check: %v. Requeueing...", res.index, err)
			workQueue <- pieceWork{index: res.index, hash: hashes[res.index], length: len(res.buf)}
			continue
		}

		donePieces++
		log.Printf("Downloaded piece %d/%d (%.2f%%)", donePieces, numPieces, float64(donePieces)/float64(numPieces)*100)
	}

	close(workQueue)
	log.Println("Download complete!")
	return nil
}
