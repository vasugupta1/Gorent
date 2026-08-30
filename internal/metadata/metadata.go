package metadata

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/jackpal/bencode-go"
	"github.com/vasugupta1/Gorent/internal/p2p"
	"github.com/vasugupta1/Gorent/internal/peer"
)

const MetadataPieceSize = 16384

// FetchMetadata attempts to download the metadata info dictionary from a peer.
func FetchMetadata(p peer.Peer, infoHash, peerID [20]byte) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", p.String(), 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Set an initial deadline for the handshake and metadata exchange
	conn.SetDeadline(time.Now().Add(10 * time.Second))

	// 1. Handshake
	hs := p2p.NewHandshake(infoHash, peerID)
	if _, err := conn.Write(hs.Serialize()); err != nil {
		return nil, err
	}

	resHs, err := p2p.ReadHandshake(conn)
	if err != nil {
		return nil, err
	}
	if resHs.InfoHash != infoHash {
		return nil, fmt.Errorf("infohash mismatch")
	}

	// 2. Check for Extension Protocol support
	if resHs.Reserved[5]&0x10 == 0 {
		return nil, fmt.Errorf("peer does not support extension protocol")
	}

	// 3. Send Extended Handshake
	extHsMsg, err := p2p.FormatExtHandshake()
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(extHsMsg.Serialize()); err != nil {
		return nil, err
	}

	var metadataSize int
	var utMetadataID int
	var metadataPieces [][]byte

	// 4. Message Loop
	for {
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		msg, err := p2p.ReadMessage(conn)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			continue // Keep-alive
		}

		if msg.ID == p2p.MsgExtended {
			if len(msg.Payload) < 1 {
				continue
			}
			extID := msg.Payload[0]
			extPayload := msg.Payload[1:]

			if extID == 0 { // Extended Handshake response
				extHs, err := p2p.ParseExtHandshake(msg.Payload)
				if err != nil {
					return nil, err
				}
				metadataSize = extHs.MetadataSize
				utMetadataID = extHs.M["ut_metadata"]
				if utMetadataID == 0 || metadataSize == 0 {
					return nil, fmt.Errorf("peer does not have metadata")
				}

				log.Printf("Received extension handshake. Metadata size: %d, ut_metadata ID: %d", metadataSize, utMetadataID)

				numPieces := (metadataSize + MetadataPieceSize - 1) / MetadataPieceSize
				metadataPieces = make([][]byte, numPieces)

				// Request all pieces
				for i := 0; i < numPieces; i++ {
					req, _ := p2p.FormatMetadataRequest(utMetadataID, i)
					conn.Write(req.Serialize())
				}
				log.Printf("Requested %d pieces", numPieces)
			} else if int(extID) == 1 { // 1 is the ID we assigned to ut_metadata in FormatExtHandshake
				// Find where the bencoded dictionary ends to split out the raw piece data
				dictEnd := 0
				depth := 0
				i := 0
				for i < len(extPayload) {
					c := extPayload[i]
					if c == 'd' || c == 'l' {
						depth++
						i++
					} else if c == 'i' {
						end := bytes.IndexByte(extPayload[i:], 'e')
						if end == -1 {
							break
						}
						i += end + 1
					} else if c >= '0' && c <= '9' {
						colon := bytes.IndexByte(extPayload[i:], ':')
						if colon == -1 {
							break
						}
						var strLen int
						fmt.Sscanf(string(extPayload[i:i+colon]), "%d", &strLen)
						i += colon + 1 + strLen
					} else if c == 'e' {
						depth--
						i++
						if depth == 0 {
							dictEnd = i
							break
						}
					} else {
						break
					}
				}

				if dictEnd == 0 {
					continue
				}

				var extMsg p2p.ExtMetadataMsg
				err := bencode.Unmarshal(bytes.NewReader(extPayload[:dictEnd]), &extMsg)
				if err != nil {
					continue
				}

				if extMsg.MsgType == 1 { // Data
					pieceData := extPayload[dictEnd:]
					metadataPieces[extMsg.Piece] = pieceData
					log.Printf("Received piece %d (size: %d)", extMsg.Piece, len(pieceData))

					// Check if we have all pieces
					allDone := true
					for _, p := range metadataPieces {
						if p == nil {
							allDone = false
							break
						}
					}

					if allDone {
						var fullMetadata []byte
						for _, p := range metadataPieces {
							fullMetadata = append(fullMetadata, p...)
						}

						// Verify SHA1
						hash := sha1.Sum(fullMetadata)
						if hash != infoHash {
							return nil, fmt.Errorf("metadata hash mismatch")
						}
						log.Println("Metadata successfully downloaded and verified")
						return fullMetadata, nil
					}
				} else if extMsg.MsgType == 2 {
					return nil, fmt.Errorf("peer rejected metadata request")
				}
			}
		}
	}
}
