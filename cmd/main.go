package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/vasugupta1/gorent/internal/daemon"
	_ "modernc.org/sqlite" // SQLite driver
)

// DummyBroadcaster is a simple implementation of the Broadcaster interface
type DummyBroadcaster struct{}

func (d *DummyBroadcaster) BroadcastProgress() {
	// Future: Implement WebSocket or SSE broadcast logic here
}

func main() {
	fmt.Println("Starting Gorent daemon...")

	// Define the database path for the store
	dbPath := "gorent.db"
	
	broadcaster := &DummyBroadcaster{}

	// Initialize the server
	srv, err := daemon.NewServer(dbPath, broadcaster)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	fmt.Println("Gorent Daemon started successfully!")
	
	// Add the Big Buck Bunny torrent
	torrentPath := "big-buck-bunny.torrent"
	if _, err := os.Stat(torrentPath); err == nil {
		fmt.Printf("Adding torrent: %s\n", torrentPath)
		args := &daemon.AddTorrentArgs{
			PathOrMagnet: torrentPath,
		}
		var reply daemon.AddTorrentReply
		if err := srv.AddTorrent(args, &reply); err != nil {
			log.Printf("Failed to add torrent: %v", err)
		} else {
			fmt.Printf("Successfully added torrent: %s (InfoHash: %s)\n", reply.Name, reply.InfoHash)
		}
	} else {
		fmt.Printf("Torrent file %s not found in the current directory.\n", torrentPath)
	}

	// TODO: In a complete application, you would attach an HTTP or RPC server
	// to the daemon server to handle incoming requests (like AddTorrent).

	// Wait for an interrupt signal (Ctrl+C) to exit cleanly
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	fmt.Println("\nShutting down daemon...")
}
