package torrent

import (
	"testing"
)

func TestOpen(t *testing.T) {
	torrentPath := "../../test.torrent"
	tf, err := Open(torrentPath)
	if err != nil {
		t.Fatalf("Failed to open torrent file: %v", err)
	}

	expectedAnnounce := "http://localhost:8000/announce"
	if tf.Announce != expectedAnnounce {
		t.Errorf("Expected Announce %q, got %q", expectedAnnounce, tf.Announce)
	}

	expectedName := "test.txt"
	if tf.Name != expectedName {
		t.Errorf("Expected Name %q, got %q", expectedName, tf.Name)
	}

	expectedLength := 299
	if tf.Length != expectedLength {
		t.Errorf("Expected Length %d, got %d", expectedLength, tf.Length)
	}

	expectedPieceLength := 64
	if tf.PieceLength != expectedPieceLength {
		t.Errorf("Expected PieceLength %d, got %d", expectedPieceLength, tf.PieceLength)
	}

	// 299 bytes divided by 64 bytes/piece = 5 pieces (4 full, 1 partial)
	expectedPiecesCount := 5
	if len(tf.PieceHashes) != expectedPiecesCount {
		t.Errorf("Expected %d piece hashes, got %d", expectedPiecesCount, len(tf.PieceHashes))
	}
}
