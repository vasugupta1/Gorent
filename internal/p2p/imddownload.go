package p2p

import (
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

func (t *Torrent) ImprovedDownloadTheFile(store StoreInterface) error {
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
	downloadErr := t.im_download(store, outFile)

	if closeErr := outFile.Close(); closeErr != nil {
		log.Printf("Warning: failed to close the output file: %v", closeErr)
	}

	if downloadErr != nil {
		return downloadErr
	}

	log.Println("Download finished successfully and file is now unlocked.")

	return nil
}

func (t *Torrent) im_download(store StoreInterface, file *os.File) error {
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

	workQueue := t.populateWorkers()
	results := make(chan *pieceResult, len(t.PieceHashes))
	t.startPeerDownload(workQueue, results)

	progressCh := make(chan struct{}, 1)
	var dbWg sync.WaitGroup
	dbWg.Add(1)
	go t.progressAggregator(store, progressCh, &dbWg)

	defer func() {
		close(progressCh)
		dbWg.Wait()
	}()

	err := t.coordinateDownload(store, file, results, progressCh, completedPieces)
	if err != nil {
		return err
	}

	t.Mu.Lock()
	t.Status = "Completed"
	t.State = StateSeeding
	t.Mu.Unlock()

	store.UpdateTorrentProgress(t)
	return nil
}

func (t *Torrent) coordinateDownload(
	store StoreInterface,
	file *os.File,
	results <-chan *pieceResult,
	progressCh chan<- struct{},
	donePieces int,
) error {
	var resultsChan <-chan *pieceResult = results
	var isPaused bool

	totalPieces := len(t.PieceHashes)

	stallTimer := time.NewTimer(30 * time.Second)
	defer stallTimer.Stop()

	downloadTimer := time.NewTimer(10 * time.Minute)
	defer downloadTimer.Stop()

	for donePieces < totalPieces {
		select {
		case newState := <-t.stateCh:
			if newState == StateStopping {
				log.Printf("Stopping download for: %s", t.Name)
				return nil
			}
			if newState == StatePaused && !isPaused {
				t.Mu.Lock()
				t.Status = "Paused"
				t.Mu.Unlock()
				isPaused = true
				resultsChan = nil // block result processing
				stallTimer.Stop() // don't stall-out while paused
				log.Printf("Pausing download for: %s", t.Name)
				store.UpdateTorrentProgress(t)
			} else if newState == StateDownloading && isPaused {
				t.Mu.Lock()
				t.Status = "Downloading"
				t.Mu.Unlock()
				isPaused = false
				resultsChan = results // re-enable result processing
				stallTimer.Reset(30 * time.Second)
				log.Printf("Resuming download for: %s", t.Name)
				store.UpdateTorrentProgress(t)
			}

		case res, ok := <-resultsChan:
			if !ok {
				// all workers finished but download incomplete
				return fmt.Errorf("all workers finished but only downloaded %d/%d pieces",
					donePieces, totalPieces)
			}

			// reset stall timer — we're making progress
			if !stallTimer.Stop() {
				select {
				case <-stallTimer.C:
				default:
				}
			}
			stallTimer.Reset(30 * time.Second)

			// write to disk
			beginOffset := int64(res.index) * int64(t.PieceLength)
			if _, err := file.WriteAt(res.buf, beginOffset); err != nil {
				log.Printf("ERROR: Failed to write piece #%d to disk: %v", res.index, err)
				continue // piece stays unset in bitfield → retried on restart
			}

			// update progress
			t.Mu.Lock()
			if !t.OurBitfield.HasPiece(res.index) {
				t.OurBitfield.SetPiece(res.index)
				t.BytesDownloaded += int64(len(res.buf))
				donePieces++
			}
			t.Mu.Unlock()

			log.Printf("(%.2f%%) Downloaded piece #%d / %d for '%s'",
				float64(donePieces)/float64(totalPieces)*100, donePieces, totalPieces, t.Name)

			// signal aggregator (non-blocking)
			select {
			case progressCh <- struct{}{}:
			default:
			}

		case <-stallTimer.C:
			return fmt.Errorf("no progress after 30 seconds")

		case <-downloadTimer.C:
			return fmt.Errorf("download timeout: only downloaded %d/%d pieces",
				donePieces, totalPieces)
		}
	}

	return nil
}

// used to understand what work needs to be done, this is what we are going to ask our peers
func (t *Torrent) populateWorkers() chan *pieceWork {
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
	return workQueue
}

// used to limit amount of peers we are going to use and create go routines with them
func (t *Torrent) startPeerDownload(workQueue chan *pieceWork, results chan *pieceResult) {
	numWorkers := len(t.Peers)
	if numWorkers > t.MaxWorkers {
		numWorkers = t.MaxWorkers
	}
	t.Mu.Lock()
	t.ActivePeers = numWorkers
	t.Mu.Unlock()

	sem := make(chan struct{}, numWorkers)
	var wg sync.WaitGroup

	for _, peer := range t.Peers {
		wg.Add(1)
		sem <- struct{}{}
		go func(p peers.Peer) {
			defer wg.Done()
			defer func() { <-sem }()
			defer func() {
				t.Mu.Lock()
				t.ActivePeers--
				if t.ActivePeers < 0 {
					t.ActivePeers = 0
				}
				t.Mu.Unlock()
			}()
			t.startDownloaderWorker(p, workQueue, results)
		}(peer)
	}

	go func() {
		wg.Wait()
		close(results)
	}()
}

func (t *Torrent) startDownloaderWorker(peer peers.Peer, workQueue chan *pieceWork, results chan *pieceResult) {
	c, err := client.New(peer, t.PeerID, t.InfoHash)
	if err != nil {
		log.Printf("Worker failed to connect to peer %s: %v", peer.IP, err)
		return
	}
	defer c.Conn.Close()

	log.Printf("Worker connected to %s, waiting for initial messages...", peer.IP)

	err = c.SendUnchoke()
	if err != nil {
		log.Printf("Send unchoke failed")
	}

	err = c.SendInterested()
	if err != nil {
		log.Printf("Send intereste for work failed")
	}

	idleTimer := time.NewTimer(30 * time.Second)
	defer idleTimer.Stop()

	putBackTimer := time.NewTimer(time.Second)
	if !putBackTimer.Stop() {
		<-putBackTimer.C
	}
	defer putBackTimer.Stop()

	resultTimer := time.NewTimer(5 * time.Second)
	if !resultTimer.Stop() {
		<-resultTimer.C
	}
	defer resultTimer.Stop()

	for {
		select {
		case pw, ok := <-workQueue:
			if !idleTimer.Stop() {
				select {
				case <-idleTimer.C:
				default:
				}
			}
			idleTimer.Reset(30 * time.Second)

			if !ok {
				log.Printf("Work queue closed, worker for %s exiting", peer.IP)
				return
			}

			t.Mu.RLock()
			alreadyHave := t.OurBitfield.HasPiece(pw.index)
			t.Mu.RUnlock()

			if alreadyHave {
				continue
			}

			if !c.Bitfield.HasPiece(pw.index) {
				// Put work back and continue
				putBackTimer.Reset(1 * time.Second)
				select {
				case workQueue <- pw:
				case <-putBackTimer.C:
				}
				continue
			}

			buf, err := attemptDownloadPiece(c, pw)
			if err != nil {
				log.Printf("Worker for %s failed to download piece #%d: %v", peer.IP, pw.index, err)
				putBackTimer.Reset(1 * time.Second)
				select {
				case workQueue <- pw:
				case <-putBackTimer.C:
				}
				return
			}

			err = checkIntegrity(pw, buf)
			if err != nil {
				log.Printf("Worker for %s failed integrity check for piece #%d", peer.IP, pw.index)
				// Put work back and continue
				putBackTimer.Reset(1 * time.Second)
				select {
				case workQueue <- pw:
				case <-putBackTimer.C:
				}
				continue
			}

			c.SendHave(pw.index)

			resultTimer.Reset(5 * time.Second)
			select {
			case results <- &pieceResult{pw.index, buf}:
				if !resultTimer.Stop() {
					<-resultTimer.C
				}
			case <-resultTimer.C:
				log.Printf("Worker for %s timed out sending result for piece #%d", peer.IP, pw.index)
				return
			}

		case <-idleTimer.C:
			log.Printf("Worker for %s timed out waiting for work", peer.IP)
			return
		}
	}
}

func (t *Torrent) progressAggregator(store StoreInterface, progressCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case _, ok := <-progressCh:
			if !ok {
				if err := store.UpdateTorrentProgress(t); err != nil {
					log.Printf("CRITICAL: Failed final progress save : %v", err)
				}
				return
			}
		case <-ticker.C:
			if err := store.UpdateTorrentProgress(t); err != nil {
				log.Printf("CRITICAL: Failed save progress : %v", err)
			}
		}
	}
}
