package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/vasugupta1/Gorent/internal/client"
	"github.com/vasugupta1/Gorent/internal/magnet"
	"github.com/vasugupta1/Gorent/internal/peer"
)

func main() {
	magnetURI := flag.String("magnet", "", "Magnet URI to download")
	flag.Parse()

	if *magnetURI == "" {
		fmt.Println("Usage: gorent -magnet <magnet-uri>")
		os.Exit(1)
	}

	fmt.Println("Gorent: Starting up...")

	// 1. Parse Magnet URI
	mag, err := magnet.Parse(*magnetURI)
	if err != nil {
		log.Fatalf("Failed to parse magnet link: %v", err)
	}
	fmt.Printf("InfoHash: %x\n", mag.InfoHash)
	fmt.Printf("Name: %s\n", mag.DisplayName)
	fmt.Printf("Trackers: %v\n", mag.Trackers)

	// 2. Generate Peer ID
	myPeerID, err := peer.GeneratePeerID()
	if err != nil {
		log.Fatalf("Failed to generate peer ID: %v", err)
	}
	fmt.Printf("My Peer ID: %s\n", string(myPeerID[:]))

	c := &client.Client{
		InfoHash: mag.InfoHash,
		PeerID:   myPeerID,
		Trackers: mag.Trackers,
		Port:     6881,
	}

	err = c.Start("downloads")
	if err != nil {
		log.Fatalf("Client failed: %v", err)
	}
}
