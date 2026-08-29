package p2p

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vasugupta1/gorent/internal/bitfield"
	"github.com/vasugupta1/gorent/internal/client"
	"github.com/vasugupta1/gorent/internal/peers"
)

func (t *Torrent) DownloadTheFile(store StoreInterface) error {

	filePath := filepath.Join(t.DownloadPath, t.Name)
	log.Printf("Preparing to download file to: %s", filePath)

	outFile, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return fmt.Errorf("could not open output file: %w", err)
	}

	if err := outFile.Truncate(int64(t.Length)); err != nil {
		outFile.Close() // Close the file on error
		return fmt.Errorf("could not set file size: %w", err)
	}
	go t.startSpeedCalculator()
	downloadErr := t.download(store, outFile)

	if closeErr := outFile.Close(); closeErr != nil {
		log.Printf("Warning: failed to close the output file: %v", closeErr)
	}

	if downloadErr != nil {
		return downloadErr
	}

	log.Println("Download finished successfully and file is now unlocked.")

	return nil
}

func (t *Torrent) download(store StoreInterface, file *os.File) error {
	log.Printf("Starting download for: %s\n", t.Name)
	t.stateCh = make(chan uint8)

	t.Mu.Lock()
	if t.OurBitfield == nil {
		t.OurBitfield = make(bitfield.Bitfield, (len(t.PieceHashes)+7)/8)
	}
	actualBytesDownloaded := int64(0)
	completedPieces := 0
	for i := 0; i < len(t.PieceHashes); i++ {
		if t.OurBitfield.HasPiece(i) {
			pieceSize := t.calculatePieceSize(i)
			actualBytesDownloaded += int64(pieceSize)
			completedPieces++
		}
	}
	t.BytesDownloaded = actualBytesDownloaded
	t.State = StateDownloading
	t.Status = "Downloading"
	t.Mu.Unlock()

	go store.UpdateTorrentProgress(t)

	log.Printf("Resume state: %d/%d pieces completed, %.2f MB downloaded",
		completedPieces, len(t.PieceHashes), float64(actualBytesDownloaded)/(1024*1024))

	if completedPieces == len(t.PieceHashes) {
		log.Printf("All pieces already downloaded for: %s", t.Name)
		return nil
	}
	if len(t.Peers) == 0 {
		return fmt.Errorf("no peers available for download")
	}

	workQueue := make(chan *pieceWork, len(t.PieceHashes))

	go func() {
		defer close(workQueue)
		for index, hash := range t.PieceHashes {
			if !t.OurBitfield.HasPiece(index) {
				length := t.calculatePieceSize(index)
				workQueue <- &pieceWork{index, hash, length}
			}
		}
	}()

	results := make(chan *pieceResult, len(t.PieceHashes))
	done := make(chan struct{}, len(t.Peers))

	activeWorkers := 0
	var wg sync.WaitGroup
	for _, peer := range t.Peers {
		wg.Add(1)
		go func(p peers.Peer) {
			defer wg.Done()
			activeWorkers++
			t.Mu.Lock()
			t.ActivePeers++
			t.Mu.Unlock()
			t.startDownloadWorker(p, workQueue, results, done)
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var resultsChan <-chan *pieceResult = results
	var isPaused bool = false

	donePieces := completedPieces
	totalPieces := len(t.PieceHashes)
	downloadTimeout := time.After(10 * time.Minute)

	progressUpdates := make(chan struct{}, 1)
	var dbUpdateWg sync.WaitGroup
	dbUpdateWg.Add(1)

	go func() {
		defer dbUpdateWg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case _, ok := <-progressUpdates:
				if !ok {
					if err := store.UpdateTorrentProgress(t); err != nil {
						log.Printf("CRITICAL: Failed to save final progress to DB: %v", err)
					}
					return
				}
			case <-ticker.C:
				if err := store.UpdateTorrentProgress(t); err != nil {
					log.Printf("ERROR: Failed to save progress to DB: %v", err)

				}
			}
		}
	}()

	for donePieces < totalPieces && activeWorkers > 0 {
		select {
		case newState := <-t.stateCh:
			if newState == StateStopping {
				log.Printf("Stopping Download For: %s", t.Name)
				return nil
			}
			if newState == StatePaused && !isPaused {
				t.Mu.Lock()
				t.Status = "Paused"
				t.Mu.Unlock()
				isPaused = true
				resultsChan = nil // Disable the results case by setting its channel to nil
				log.Printf("Pausing Download for: %s", t.Name)
				store.UpdateTorrentProgress(t)
			} else if newState == StateDownloading && isPaused {
				t.Mu.Lock()
				t.Status = "Downloading"
				t.Mu.Unlock()
				isPaused = false
				resultsChan = results // Re-enable the results case
				log.Printf("Resuming download for: %s", t.Name)
				store.UpdateTorrentProgress(t)
			}

		case res := <-resultsChan:
			beginOffset := int64(res.index) * int64(t.PieceLength)
			_, err := file.WriteAt(res.buf, beginOffset)
			if err != nil {
				log.Printf("ERROR: Failed to write piece #%d to disk: %v", res.index, err)
				workQueue <- &pieceWork{res.index, t.PieceHashes[res.index], len(res.buf)}
				continue
			}

			t.Mu.Lock()
			if !t.OurBitfield.HasPiece(res.index) {
				t.OurBitfield.SetPiece(res.index)
				t.BytesDownloaded += int64(len(res.buf))
				donePieces++
			}
			t.Mu.Unlock()
			percent := float64(t.BytesDownloaded) / float64(t.Length) * 100
			log.Printf("(%.2f%%) Downloaded piece #%d / %d for '%s'", percent, donePieces, totalPieces, t.Name)

			select {
			case progressUpdates <- struct{}{}:
			default:
			}
		case <-done:
			activeWorkers--
			t.Mu.Lock()
			t.ActivePeers--
			if t.ActivePeers < 0 {
				t.ActivePeers = 0
			}
			t.Mu.Unlock()
			log.Printf("Worker finished, %d workers remaining", activeWorkers)
			if activeWorkers == 0 {
				return fmt.Errorf("all workers finished but only downloaded %d/%d pieces", donePieces, totalPieces)
			}

		case <-downloadTimeout:
			return fmt.Errorf("download timeout: only downloaded %d/%d pieces", donePieces, totalPieces)

		case <-time.After(30 * time.Second):
			t.Mu.RLock()
			currentDownloaded := t.BytesDownloaded
			t.Mu.RUnlock()
			if currentDownloaded == actualBytesDownloaded {
				return fmt.Errorf("no progress after 30 seconds")
			}
			actualBytesDownloaded = currentDownloaded
		}
	}
	dbUpdateWg.Wait()

	if donePieces < totalPieces {
		return fmt.Errorf("incomplete download: %d/%d pieces", donePieces, totalPieces)
	}

	t.Mu.Lock()
	t.Status = "Completed"
	t.State = StateSeeding
	t.Mu.Unlock()

	log.Printf("Download completed for '%s'. Performing final save to DB.", t.Name)
	if err := store.UpdateTorrentProgress(t); err != nil {
		log.Printf("CRITICAL: Failed to save final 'Completed' status to database: %v", err)
		return err
	}
	return nil
}

func (t *Torrent) startDownloadWorker(peer peers.Peer, workQueue chan *pieceWork, results chan *pieceResult, done chan struct{}) {
	defer func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}()

	c, err := client.New(peer, t.PeerID, t.InfoHash)
	if err != nil {
		log.Printf("Worker failed to connect to peer %s: %v", peer.IP, err)
		return
	}
	defer c.Conn.Close()

	log.Printf("Worker connected to %s, waiting for initial messages...", peer.IP)

	c.SendUnchoke()
	c.SendInterested()

	for {
		select {
		case pw, ok := <-workQueue:
			if !ok {
				log.Printf("Work queue closed, worker for %s exiting", peer.IP)
				return
			}

			t.Mu.RLock()
			alreadyHave := t.OurBitfield.HasPiece(pw.index)
			t.Mu.RUnlock()

			if alreadyHave {
				// log.Printf("Piece #%d already completed, skipping", pw.index)
				continue
			}

			if !c.Bitfield.HasPiece(pw.index) {
				// Put work back and continue
				select {
				case workQueue <- pw:
				case <-time.After(1 * time.Second):
				}
				continue
			}

			buf, err := attemptDownloadPiece(c, pw)
			if err != nil {
				log.Printf("Worker for %s failed to download piece #%d: %v", peer.IP, pw.index, err)
				select {
				case workQueue <- pw:
				case <-time.After(1 * time.Second):
				}
				return
			}

			err = checkIntegrity(pw, buf)
			if err != nil {
				log.Printf("Worker for %s failed integrity check for piece #%d", peer.IP, pw.index)
				// Put work back and continue
				select {
				case workQueue <- pw:
				case <-time.After(1 * time.Second):
				}
				continue
			}

			c.SendHave(pw.index)

			select {
			case results <- &pieceResult{pw.index, buf}:
				// log.Printf("Successfully downloaded piece #%d from %s (%d bytes)", pw.index, peer.IP, len(buf))
			case <-time.After(5 * time.Second):
				log.Printf("Worker for %s timed out sending result for piece #%d", peer.IP, pw.index)
				return
			}

		case <-time.After(30 * time.Second):
			log.Printf("Worker for %s timed out waiting for work", peer.IP)
			return
		}
	}
}

func attemptDownloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
	state := pieceProgress{
		index:  pw.index,
		client: c,
		buf:    make([]byte, pw.length),
	}

	c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	defer c.Conn.SetDeadline(time.Time{})

	for state.downloaded < pw.length {
		if !state.client.Choked {
			for state.backlog < MaxBacklog && state.requested < pw.length {
				blockSize := MaxBlockSize

				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}

				err := c.SendRequest(pw.index, state.requested, blockSize)
				if err != nil {
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
		}

		err := state.readMessage()
		if err != nil {
			return nil, err
		}
	}
	return state.buf, nil
}

func (state *pieceProgress) readMessage() error {
	msg, err := state.client.Read()
	if err != nil {
		return err
	}
	if msg == nil {
		return nil
	}

	switch msg.ID {
	case client.MsgUnChoke:
		state.client.Choked = false
	case client.MsgChoke:
		state.client.Choked = true
	case client.MsgHave:
		index, err := client.ParseHave(msg)
		if err != nil {
			return err
		}
		state.client.Bitfield.SetPiece(index)
	case client.MsgPiece:
		n, err := client.ParsePiece(state.index, state.buf, msg)
		if err != nil {
			return err
		}
		state.downloaded += n
		state.backlog--
	}
	return nil
}

func checkIntegrity(pw *pieceWork, buf []byte) error {
	hash := sha1.Sum(buf)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("piece %d failed integrity check", pw.index)
	}
	return nil
}
