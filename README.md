# Gorent

Gorent is a fast, Terminal User Interface (TUI) based Torrent Manager written in Go. It provides an intuitive, keyboard-driven interface to manage, monitor, and configure your torrent downloads directly from your terminal. 

## Features
- **TUI Powered**: Built with [Bubble Tea](https://github.com/charmbracelet/bubbletea) for a seamless, visually pleasing terminal interface.
- **Magnet Link Support**: Easily add and parse magnet URIs to start downloading.
- **Persistent State**: Utilizes an embedded SQLite database (`torrents.db`) to safely persist your torrent statuses, settings, and progress.
- **Customizable Download Paths**: On your first run, Gorent will prompt you to set your default download directory.
- **Pause & Resume**: Start, stop, and manage multiple torrents at once.

## Prerequisites
- **Go**: Version 1.27.0 or higher.
- Cgo enabled (required for the SQLite driver).

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/vasugupta1/Gorent.git
   cd Gorent
   ```

2. **Build the application:**
   ```bash
   go build -o gorent ./cmd/gorent
   ```

3. **(Optional) Move the binary to your PATH** for easy access globally:
   ```bash
   sudo mv gorent /usr/local/bin/
   ```

## Usage

Launch Gorent by simply running the built binary in your terminal:
```bash
./gorent
```
*(On the very first launch, the CLI will prompt you to type in the directory path where you want to store your downloaded files.)*

You can also jump right into a download by passing a magnet link directly through the command line:
```bash
./gorent -magnet "magnet:?xt=urn:btih:..."
```

### Controls
Once inside the TUI, use the following keyboard shortcuts to manage your torrents:

- `a` : **Add** a new torrent (opens a prompt at the bottom to paste a magnet link)
- `p` : **Pause** the currently selected torrent
- `s` : **Start/Resume** the currently selected torrent
- `r` : **Remove** the currently selected torrent
- `↑ / ↓` : **Navigate** through your list of torrents
- `Enter` : **Submit** your magnet link when adding a new torrent
- `q` / `Esc` : **Quit** the application

## Troubleshooting
Application logs are safely written to `gorent.log` in the current working directory to avoid interfering with the TUI. If you encounter any unexpected behaviors or failed torrents, check this file for details.
