package torrent

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"os"

	"github.com/jackpal/bencode-go"
)

type bencodeInfo struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

type bencodeTorrent struct {
	Announce     string      `bencode:"announce"`
	AnnounceList [][]string  `bencode:"announce-list"`
	Info         bencodeInfo `bencode:"info"`
}

type TorrentFile struct {
	Announce     string
	InfoHash     [20]byte
	PieceHashes  [][20]byte
	AnnounceList []string
	PieceLength  int
	Length       int
	Name         string
}

func (bi *bencodeInfo) hash() ([20]byte, error) {
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, *bi)
	if err != nil {
		return [20]byte{}, err
	}
	hash := sha1.Sum(buf.Bytes())
	return hash, nil
}

func (bi *bencodeInfo) pieceHashes() ([][20]byte, error) {
	hashlen := 20
	buf := []byte(bi.Pieces)
	// every piece must have exactly 20 byte hash hence the total length must be a multiple of 20
	if len(buf)%hashlen != 0 {
		return nil, fmt.Errorf("not correct length pieces, torrent file not valid")
	}

	numOfHashes := len(buf) / hashlen

	hashes := make([][20]byte, numOfHashes)

	for i := 0; i < numOfHashes; i++ {
		//start from 0 :20 then 20:40 then 40:60
		currHash := buf[i*hashlen : ((i + 1) * hashlen)]
		copiedlen := copy(hashes[i][:], currHash)
		if copiedlen != hashlen {
			return nil, fmt.Errorf("not correct length pieces, torrent file not valid")
		}
	}

	return hashes, nil
}

func (b *bencodeTorrent) toTorrentFile() (t TorrentFile, err error) {
	infoHash, err := b.Info.hash()
	if err != nil {
		return t, err
	}

	pieceHashes, err := b.Info.pieceHashes()
	if err != nil {
		return t, err
	}

	//Map is to make sure we don't get duplicate trakcers
	trackersSet := make(map[string]struct{})
	if b.Announce != "" {
		trackersSet[b.Announce] = struct{}{}
	}

	for _, tier := range b.AnnounceList {
		for _, t := range tier {
			if t != "" {
				trackersSet[t] = struct{}{}
			}
		}
	}

	trackers := []string{}
	for k := range trackersSet {
		trackers = append(trackers, k)
	}

	return TorrentFile{
		Announce:     b.Announce,
		PieceLength:  b.Info.PieceLength,
		Length:       b.Info.Length,
		Name:         b.Info.Name,
		InfoHash:     infoHash,
		PieceHashes:  pieceHashes,
		AnnounceList: trackers,
	}, nil
}

func Open(path string) (t TorrentFile, err error) {
	file, err := os.Open(path)
	if err != nil {
		return t, err
	}

	defer file.Close()

	bto := bencodeTorrent{}
	err = bencode.Unmarshal(file, &bto)

	if err != nil {
		return t, err
	}

	return bto.toTorrentFile()
}
