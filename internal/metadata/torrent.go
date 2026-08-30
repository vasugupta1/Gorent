package metadata

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/jackpal/bencode-go"
)

// TorrentInfo represents the parsed info dictionary.
type TorrentInfo struct {
	Name        string
	PieceLength int
	Pieces      []byte
	Length      int        // Single file mode
	Files       []FileInfo // Multiple file mode
	InfoHash    [20]byte
}

type FileInfo struct {
	Length int
	Path   []string
}

// ParseTorrentInfo parses the raw bencoded info dictionary.
func ParseTorrentInfo(rawInfo []byte) (*TorrentInfo, error) {
	var infoDict struct {
		Name        string `bencode:"name"`
		PieceLength int    `bencode:"piece length"`
		Pieces      string `bencode:"pieces"`
		Length      int    `bencode:"length,omitempty"`
		Files       []struct {
			Length int      `bencode:"length"`
			Path   []string `bencode:"path"`
		} `bencode:"files,omitempty"`
	}

	if err := bencode.Unmarshal(bytes.NewReader(rawInfo), &infoDict); err != nil {
		return nil, err
	}

	hash := sha1.Sum(rawInfo)

	ti := &TorrentInfo{
		Name:        infoDict.Name,
		PieceLength: infoDict.PieceLength,
		Pieces:      []byte(infoDict.Pieces),
		Length:      infoDict.Length,
		InfoHash:    hash,
	}

	if len(infoDict.Files) > 0 {
		for _, f := range infoDict.Files {
			ti.Files = append(ti.Files, FileInfo{
				Length: f.Length,
				Path:   f.Path,
			})
		}
	} else if ti.Length == 0 {
		return nil, fmt.Errorf("invalid torrent info: length and files missing")
	}

	if len(ti.Pieces)%20 != 0 {
		return nil, fmt.Errorf("invalid pieces length")
	}

	return ti, nil
}

// PieceHashes returns an array of 20-byte piece hashes.
func (t *TorrentInfo) PieceHashes() [][20]byte {
	numPieces := len(t.Pieces) / 20
	hashes := make([][20]byte, numPieces)
	for i := 0; i < numPieces; i++ {
		copy(hashes[i][:], t.Pieces[i*20:(i+1)*20])
	}
	return hashes
}
