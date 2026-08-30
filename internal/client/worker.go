package client

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/vasugupta1/Gorent/internal/p2p"
	"github.com/vasugupta1/Gorent/internal/peer"
)

const MaxBlockSize = 16384 // 16KB
const MaxBacklog = 5

type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

type pieceResult struct {
	index int
	buf   []byte
}

type PeerState struct {
	Choked     bool
	Interested bool
}

// peerWorker connects to a peer and continuously processes piece work from the workQueue.
func peerWorker(ctx context.Context, c *Client, p peer.Peer, infoHash, peerID [20]byte, workQueue chan pieceWork, results chan pieceResult) {
	conn, err := net.DialTimeout("tcp", p.String(), 5*time.Second)
	if err != nil {
		return
	}
	defer conn.Close()

	// Handshake
	hs := p2p.NewHandshake(infoHash, peerID)
	conn.Write(hs.Serialize())
	resHs, err := p2p.ReadHandshake(conn)
	if err != nil || resHs.InfoHash != infoHash {
		return
	}

	// We are connected and handshaked. Process work queue.
	for {
		var pw pieceWork
		var ok bool
		
		c.Mu.Lock()
		for c.paused && !c.IsRemoved {
			c.pauseCond.Wait()
		}
		if c.IsRemoved {
			c.Mu.Unlock()
			return
		}
		c.Mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case pw, ok = <-workQueue:
			if !ok {
				return
			}
		}

		buf, err := attemptDownloadPiece(conn, pw)
		if err != nil {
			log.Printf("Worker %s failed piece %d: %v", p.String(), pw.index, err)
			select {
			case <-ctx.Done():
				return
			case workQueue <- pw: // Put the work back on the queue
			}
			return          // Disconnect and exit worker
		}

		// Success!
		select {
		case <-ctx.Done():
			return
		case results <- pieceResult{index: pw.index, buf: buf}:
		}
	}
}

func attemptDownloadPiece(conn net.Conn, pw pieceWork) ([]byte, error) {
	state := PeerState{
		Choked:     true,
		Interested: false,
	}

	// Send Interested
	interestedMsg := &p2p.Message{ID: p2p.MsgInterested}
	conn.Write(interestedMsg.Serialize())
	state.Interested = true

	buf := make([]byte, pw.length)
	downloaded := 0
	requested := 0
	backlog := 0

	// Set a deadline for the entire piece download
	conn.SetDeadline(time.Now().Add(60 * time.Second))

	for downloaded < pw.length {
		if !state.Choked {
			// Pipeline requests
			for backlog < MaxBacklog && requested < pw.length {
				blockSize := MaxBlockSize
				if pw.length-requested < blockSize {
					blockSize = pw.length - requested
				}
				req := p2p.FormatRequest(pw.index, requested, blockSize)
				conn.Write(req.Serialize())
				requested += blockSize
				backlog++
			}
		}

		msg, err := p2p.ReadMessage(conn)
		if err != nil {
			return nil, err
		}

		if msg == nil {
			continue // keep-alive
		}

		switch msg.ID {
		case p2p.MsgChoke:
			state.Choked = true
		case p2p.MsgUnchoke:
			state.Choked = false
		case p2p.MsgHave:
			// Ignore for now, assume peer has everything
		case p2p.MsgPiece:
			n, err := p2p.ParsePiece(pw.index, buf, msg)
			if err != nil {
				return nil, err
			}
			downloaded += n
			backlog--
		}
	}
	return buf, nil
}
