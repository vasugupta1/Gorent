package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("Starting Gorent")
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "Usage: %s <torrent-file> <output-dir>", os.Args[0])
	}

	torrentFile := os.Args[1]
	outputDir := os.Args[2]

	fmt.Printf("Downloading %s to %s...\n", torrentFile, outputDir)
}
