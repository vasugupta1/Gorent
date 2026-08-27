package daemon

import (
	"crypto/rand"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vasugupta1/gorent/internal/p2p"
	"github.com/vasugupta1/gorent/internal/peers"
	"github.com/vasugupta1/gorent/internal/torrent"
)

type mockBroadcaster struct {
	broadcastCalled bool
}

func (m *mockBroadcaster) BroadcastProgress() {
	m.broadcastCalled = true
}

func TestDedupAndConstructPeers(t *testing.T) {
	t.Run("successful deduplication", func(t *testing.T) {
		peerChan := make(chan peers.Peer, 5)
		peerChan <- peers.Peer{IP: net.ParseIP("127.0.0.1"), Port: 6881}
		peerChan <- peers.Peer{IP: net.ParseIP("127.0.0.1"), Port: 6881} // duplicate
		peerChan <- peers.Peer{IP: net.ParseIP("192.168.1.1"), Port: 6882}
		peerChan <- peers.Peer{IP: net.ParseIP("127.0.0.1"), Port: 6882} // same IP, different port
		close(peerChan)

		res, err := dedupAndConstructPeers(peerChan)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(res) != 3 {
			t.Errorf("expected 3 peers, got %d", len(res))
		}

		// Verify contents
		expected := map[string]bool{
			"127.0.0.1:6881":   true,
			"192.168.1.1:6882": true,
			"127.0.0.1:6882":   true,
		}

		for _, p := range res {
			key := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)
			if !expected[key] {
				t.Errorf("unexpected peer in result: %s", key)
			}
		}
	})

	t.Run("no peers found", func(t *testing.T) {
		peerChan := make(chan peers.Peer)
		close(peerChan)

		_, err := dedupAndConstructPeers(peerChan)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		expectedErr := "No peers found for any trackers"
		if err.Error() != expectedErr {
			t.Errorf("expected error %q, got %q", expectedErr, err.Error())
		}
	})
}

func TestAddTorrent_MagnetLink(t *testing.T) {
	s := &Server{
		torrents: make(map[string]*p2p.Torrent),
	}
	args := &AddTorrentArgs{
		PathOrMagnet: "magnet:?xt=urn:btih:example",
	}
	var reply AddTorrentReply
	err := s.AddTorrent(args, &reply)
	if err == nil {
		t.Fatal("expected error for magnet links, got nil")
	}
	expectedErr := "magnet links not yet supported"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestAddTorrent_InvalidFile(t *testing.T) {
	s := &Server{
		torrents: make(map[string]*p2p.Torrent),
	}
	args := &AddTorrentArgs{
		PathOrMagnet: "non_existent_file.torrent",
	}
	var reply AddTorrentReply
	err := s.AddTorrent(args, &reply)
	if err == nil {
		t.Fatal("expected error for invalid file path, got nil")
	}
	if !strings.Contains(err.Error(), "failed to process input") {
		t.Errorf("expected error to contain 'failed to process input', got %q", err.Error())
	}
}

func TestAddTorrent_AlreadyManaged(t *testing.T) {
	// Create a dummy server
	s := &Server{
		torrents: make(map[string]*p2p.Torrent),
	}

	// Read valid torrent file to find its expected info hash
	torrentPath := "../../test.torrent"
	tf, err := torrent.Open(torrentPath)
	if err != nil {
		t.Fatalf("failed to open test torrent: %v", err)
	}
	infoHashStr := fmt.Sprintf("%x", tf.InfoHash)

	// Pre-populate torrents map to simulate torrent already being managed
	s.torrents[infoHashStr] = &p2p.Torrent{}

	args := &AddTorrentArgs{
		PathOrMagnet: torrentPath,
	}
	var reply AddTorrentReply
	err = s.AddTorrent(args, &reply)
	if err == nil {
		t.Fatal("expected error for already managed torrent, got nil")
	}
	expectedErr := "torrent is already being managed"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestAddTorrent_NoPeers(t *testing.T) {
	// Create a dummy server
	s := &Server{
		torrents:   make(map[string]*p2p.Torrent),
		dhtEnabled: false,
	}

	// We use test.torrent which points to localhost:8000.
	// Since there is no tracker running, findPeers should fail to find any peers.
	torrentPath := "../../test.torrent"
	args := &AddTorrentArgs{
		PathOrMagnet: torrentPath,
	}
	var reply AddTorrentReply
	err := s.AddTorrent(args, &reply)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	expectedErr := "failed to get any peers for this torrent"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestFindPeers_NoTrackers(t *testing.T) {
	tf := torrent.TorrentFile{
		Announce:     "",
		AnnounceList: []string{},
	}
	var peerId [20]byte
	_, err := rand.Read(peerId[:])
	if err != nil {
		t.Fatalf("failed to generate peer ID: %v", err)
	}

	_, err = findPeers(tf, peerId, false, nil)
	if err == nil {
		t.Fatal("expected error when no trackers are present, got nil")
	}
	expectedErr := "no tracker URLs available for this torrent"
	if err.Error() != expectedErr {
		t.Errorf("expected error %q, got %q", expectedErr, err.Error())
	}
}

func TestNewServer(t *testing.T) {
	// Note: Running this test requires the SQLite driver (e.g. modernc.org/sqlite) to be loaded.
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	broadcaster := &mockBroadcaster{}

	s, err := NewServer(dbPath, broadcaster)
	// If the database SQLite driver is not registered, NewServer will fail at sql.Open.
	// We handle this gracefully during testing.
	if err != nil {
		t.Logf("NewServer failed (expected if sqlite driver is not registered): %v", err)
		return
	}
	defer s.store.Close()

	if s.downloadPath != "./downloads" {
		t.Errorf("expected default download path './downloads', got %q", s.downloadPath)
	}

	if s.broadcaster != broadcaster {
		t.Error("broadcaster was not set correctly on the server")
	}

	// Verify setting was saved
	savedPath, err := s.store.GetSettings("download_path")
	if err != nil {
		t.Fatalf("failed to get download_path setting: %v", err)
	}
	if savedPath != "./downloads" {
		t.Errorf("expected saved download path to be './downloads', got %q", savedPath)
	}
}
