package storage

import (
	"crypto/sha1"
	"fmt"
	"os"
	"path/filepath"

	"github.com/vasugupta1/Gorent/internal/metadata"
)

type Storage struct {
	baseDir   string
	files     []fileEntry
	totalSize int
}

type fileEntry struct {
	path   string
	length int
	offset int
	file   *os.File
}

// NewStorage initializes the storage backend for a torrent.
func NewStorage(baseDir string, info *metadata.TorrentInfo) (*Storage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	s := &Storage{
		baseDir: baseDir,
	}

	offset := 0
	if len(info.Files) > 0 {
		// Multi-file
		base := filepath.Join(baseDir, info.Name)
		if err := os.MkdirAll(base, 0755); err != nil {
			return nil, err
		}
		for _, f := range info.Files {
			path := filepath.Join(append([]string{base}, f.Path...)...)
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return nil, err
			}
			file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return nil, err
			}
			// Pre-allocate (optional, but good for sparseness)
			file.Truncate(int64(f.Length))
			s.files = append(s.files, fileEntry{path, f.Length, offset, file})
			offset += f.Length
		}
	} else {
		// Single-file
		path := filepath.Join(baseDir, info.Name)
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		file.Truncate(int64(info.Length))
		s.files = append(s.files, fileEntry{path, info.Length, offset, file})
		offset += info.Length
	}
	s.totalSize = offset

	return s, nil
}

// WritePiece writes a verified piece to the corresponding files.
func (s *Storage) WritePiece(pieceIndex, pieceLength int, data []byte, expectedHash [20]byte) error {
	hash := sha1.Sum(data)
	if hash != expectedHash {
		return fmt.Errorf("piece hash mismatch")
	}

	globalOffset := pieceIndex * pieceLength
	remaining := len(data)
	dataOffset := 0

	for _, f := range s.files {
		// Does this piece overlap with this file?
		if globalOffset < f.offset+f.length && globalOffset+remaining > f.offset {
			// Calculate intersection
			fileStart := globalOffset
			if fileStart < f.offset {
				fileStart = f.offset
			}
			fileEnd := globalOffset + remaining
			if fileEnd > f.offset+f.length {
				fileEnd = f.offset + f.length
			}

			writeLen := fileEnd - fileStart
			fileOffset := fileStart - f.offset

			_, err := f.file.WriteAt(data[dataOffset:dataOffset+writeLen], int64(fileOffset))
			if err != nil {
				return err
			}

			dataOffset += writeLen
			remaining -= writeLen
			globalOffset += writeLen

			if remaining == 0 {
				break
			}
		}
	}
	return nil
}

// Close closes all open file handles.
func (s *Storage) Close() {
	for _, f := range s.files {
		f.file.Close()
	}
}
