package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/vasugupta1/Gorent/internal/engine"
	"github.com/vasugupta1/Gorent/internal/tui"
)

func main() {
	// Redirect standard log output to a file so it doesn't mess up the TUI
	f, err := os.OpenFile("gorent.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err == nil {
		log.SetOutput(f)
		defer f.Close()
	}

	magnetURI := flag.String("magnet", "", "Optional: Magnet URI to start downloading immediately")
	flag.Parse()

	// Initialize the download directory
	os.MkdirAll("downloads", 0755)

	manager := engine.NewTorrentManager()

	if *magnetURI != "" {
		err := manager.AddMagnet(*magnetURI, "downloads")
		if err != nil {
			log.Fatalf("Failed to add initial magnet link: %v", err)
		}
	}

	p := tea.NewProgram(tui.InitialModel(manager), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}
}
