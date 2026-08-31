package client

import (
	"context"
	"encoding/binary"
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

// peerWorker connects to a peer and continuously processes piece work using PieceManager.
func peerWorker(ctx context.Context, c *Client, p peer.Peer, infoHash, peerID [20]byte, pm *PieceManager, results chan pieceResult) {
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

	// Read initial bitfield (optional but common) or other messages
	conn.SetDeadline(time.Now().Add(1 * time.Second))
	msg, err := p2p.ReadMessage(conn)
	
	var peerBitfield []byte
	state := PeerState{
		Choked:     true,
		Interested: false,
	}
	
	if err == nil && msg != nil {
		if msg.ID == p2p.MsgBitfield {
			peerBitfield = msg.Payload
			pm.AddPeerAvailability(peerBitfield)
		} else if msg.ID == p2p.MsgHave {
			// Just in case it sends HAVE right away
			idx := int(binary.BigEndian.Uint32(msg.Payload))
			pm.UpdateAvailability(idx)
			peerBitfield = make([]byte, (pm.numPieces+7)/8)
			peerBitfield[idx/8] |= (1 << (idx % 8))
		} else if msg.ID == p2p.MsgUnchoke {
			state.Choked = false
			peerBitfield = make([]byte, (pm.numPieces+7)/8)
		} else {
			peerBitfield = make([]byte, (pm.numPieces+7)/8)
		}
	} else {
		// Initialize empty bitfield
		peerBitfield = make([]byte, (pm.numPieces+7)/8)
	}
	
	// Reset deadline for normal operations
	conn.SetDeadline(time.Time{})

	// We are connected and handshaked. Process pieces.
	for {
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
		default:
		}

		idx, ok := pm.NextPiece(peerBitfield)
		if !ok {
			// No pieces available from this peer right now. Wait for a message (e.g. HAVE)
			conn.SetDeadline(time.Now().Add(10 * time.Second))
			msg, err := p2p.ReadMessage(conn)
			conn.SetDeadline(time.Time{})
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Just a timeout, keep waiting
					continue
				}
				// Actual connection error, disconnect
				return 
			}
			if msg != nil {
				if msg.ID == p2p.MsgHave {
					idx := int(binary.BigEndian.Uint32(msg.Payload))
					pm.UpdateAvailability(idx)
					if idx/8 < len(peerBitfield) {
						peerBitfield[idx/8] |= (1 << (idx % 8))
					}
				} else if msg.ID == p2p.MsgUnchoke {
					state.Choked = false
				} else if msg.ID == p2p.MsgChoke {
					state.Choked = true
				} else if msg.ID == p2p.MsgBitfield {
					peerBitfield = msg.Payload
					pm.AddPeerAvailability(peerBitfield)
				}
			}
			continue
		}

		// Calculate piece length
		pieceLen := c.TorrentInfo.PieceLength
		if idx == pm.numPieces-1 {
			pieceLen = c.TorrentInfo.Length % c.TorrentInfo.PieceLength
			if pieceLen == 0 {
				pieceLen = c.TorrentInfo.PieceLength
			}
		}

		pw := pieceWork{
			index:  idx,
			hash:   c.TorrentInfo.PieceHashes()[idx],
			length: pieceLen,
		}

		buf, err := attemptDownloadPiece(conn, pw, pm, peerBitfield, &state)
		if err != nil {
			log.Printf("Worker %s failed piece %d: %v", p.String(), pw.index, err)
			pm.SetFailed(pw.index)
			return // Disconnect and exit worker
		}

		// Success!
		pm.SetDone(pw.index)
		select {
		case <-ctx.Done():
			return
		case results <- pieceResult{index: pw.index, buf: buf}:
		}
	}
}

func attemptDownloadPiece(conn net.Conn, pw pieceWork, pm *PieceManager, peerBitfield []byte, state *PeerState) ([]byte, error) {
	if !state.Interested {
		// Send Interested
		interestedMsg := &p2p.Message{ID: p2p.MsgInterested}
		conn.Write(interestedMsg.Serialize())
		state.Interested = true
	}

	buf := make([]byte, pw.length)
	downloaded := 0
	requested := 0
	backlog := 0

	// Set a deadline for the entire piece download (5 minutes for large pieces)
	conn.SetDeadline(time.Now().Add(5 * time.Minute))

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
			idx := int(binary.BigEndian.Uint32(msg.Payload))
			pm.UpdateAvailability(idx)
			if idx/8 < len(peerBitfield) {
				peerBitfield[idx/8] |= (1 << (idx % 8))
			}
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
