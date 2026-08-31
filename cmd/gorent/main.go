package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/vasugupta1/Gorent/internal/db"
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

	dbPath := "torrents.db"
	var downloadDir string
	dbExists := true

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		dbExists = false
	}

	database, err := db.InitDB(dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	if dbExists {
		downloadDir, err = database.GetSetting("download_dir")
		if err != nil || downloadDir == "" {
			downloadDir = "downloads"
		}
	}
	database.Close()

	if downloadDir != "" {
		os.MkdirAll(downloadDir, 0755)
	}

	manager, err := engine.NewTorrentManager(dbPath, downloadDir)
	if err != nil {
		log.Fatalf("Failed to initialize manager: %v", err)
	}

	p := tea.NewProgram(tui.InitialModel(manager, !dbExists, *magnetURI), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Alas, there's been an error: %v", err)
		os.Exit(1)
	}
}
