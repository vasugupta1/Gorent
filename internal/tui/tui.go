package tui

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/vasugupta1/Gorent/internal/engine"
)

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(time.Second/2, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

type model struct {
	manager       *engine.TorrentManager
	textInput     textinput.Model
	dirInput      textinput.Model
	adding        bool
	promptingDir  bool
	err           error
	selectedIndex int
	initMagnetURI string
}

func InitialModel(manager *engine.TorrentManager, needsDirPrompt bool, magnetURI string) model {
	ti := textinput.New()
	ti.Placeholder = "Paste magnet link here"
	ti.CharLimit = 500
	ti.Width = 50

	di := textinput.New()
	di.Placeholder = "e.g., downloads/"
	di.CharLimit = 200
	di.Width = 50

	if needsDirPrompt {
		di.Focus()
	}

	return model{
		manager:       manager,
		textInput:     ti,
		dirInput:      di,
		adding:        false,
		promptingDir:  needsDirPrompt,
		err:           nil,
		selectedIndex: 0,
		initMagnetURI: magnetURI,
	}
}

func (m model) Init() tea.Cmd {
	if m.initMagnetURI != "" && !m.promptingDir {
		m.manager.AddMagnet(m.initMagnetURI)
	}
	return tea.Batch(tickCmd(), textinput.Blink)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			if m.promptingDir {
				return m, tea.Quit
			}
			if m.adding {
				m.adding = false
				m.textInput.SetValue("")
				return m, nil
			}
			return m, tea.Quit
		case tea.KeyEnter:
			if m.promptingDir {
				dir := m.dirInput.Value()
				dir = strings.TrimSpace(dir)
				if dir == "" {
					dir = "downloads"
				}
				os.MkdirAll(dir, 0755)
				m.manager.SetDownloadDir(dir)
				m.promptingDir = false
				
				if m.initMagnetURI != "" {
					m.manager.AddMagnet(m.initMagnetURI)
					m.initMagnetURI = ""
				}
				return m, nil
			}
			if m.adding {
				uri := m.textInput.Value()
				if uri != "" {
					err := m.manager.AddMagnet(uri)
					if err != nil {
						m.err = err
					} else {
						m.err = nil
					}
				}
				m.adding = false
				m.textInput.SetValue("")
				return m, nil
			}
		case tea.KeyUp:
			if !m.adding && !m.promptingDir {
				m.selectedIndex--
				if m.selectedIndex < 0 {
					m.selectedIndex = 0
				}
			}
		case tea.KeyDown:
			if !m.adding && !m.promptingDir {
				m.selectedIndex++
				clients := m.manager.GetClients()
				if m.selectedIndex >= len(clients) {
					m.selectedIndex = len(clients) - 1
					if m.selectedIndex < 0 {
						m.selectedIndex = 0
					}
				}
			}
		}

		if !m.adding && !m.promptingDir {
			switch msg.String() {
			case "q":
				return m, tea.Quit
			case "a":
				m.adding = true
				m.textInput.Focus()
				return m, nil
			case "p":
				m.manager.PauseTorrent(m.selectedIndex)
			case "s":
				m.manager.ResumeTorrent(m.selectedIndex)
			case "r":
				m.manager.RemoveTorrent(m.selectedIndex)
				// Adjust selection if we removed the last item
				clients := m.manager.GetClients()
				if m.selectedIndex >= len(clients) && len(clients) > 0 {
					m.selectedIndex = len(clients) - 1
				} else if len(clients) == 0 {
					m.selectedIndex = 0
				}
			}
		}

	case tickMsg:
		return m, tickCmd()
	}

	if m.promptingDir {
		m.dirInput, cmd = m.dirInput.Update(msg)
		return m, cmd
	}

	if m.adding {
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m model) View() string {
	if m.promptingDir {
		var s strings.Builder
		s.WriteString(lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205")).Render("Gorent - Initial Setup"))
		s.WriteString("\n\nWhere do you want to put downloaded files?\n\n")
		s.WriteString(m.dirInput.View())
		s.WriteString("\n\n(Press Enter to confirm, leave empty for 'downloads')")
		return s.String()
	}

	var s strings.Builder

	s.WriteString(lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205")).Render("Gorent - Torrent Manager"))
	s.WriteString("\n\n")

	clients := m.manager.GetClients()
	if len(clients) == 0 {
		s.WriteString("No active torrents.\n")
	} else {
		for i, c := range clients {
			c.Mu.Lock()
			name := c.Name
			if name == "" {
				name = "Unknown"
			}
			status := c.Status
			done := c.DonePieces
			total := c.TotalPieces
			c.Mu.Unlock()

			progress := 0.0
			if total > 0 {
				progress = float64(done) / float64(total) * 100
			}

			prefix := "  "
			style := lipgloss.NewStyle()
			if i == m.selectedIndex {
				prefix = "> "
				style = style.Foreground(lipgloss.Color("42")).Bold(true)
			}

			s.WriteString(fmt.Sprintf("%s%d. %s\n", prefix, i+1, style.Render(name)))
			s.WriteString(fmt.Sprintf("     Status: %s | Progress: %.2f%% (%d/%d pieces)\n", status, progress, done, total))
			s.WriteString("\n")
		}
	}

	if m.err != nil {
		s.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render(fmt.Sprintf("\nError: %v\n", m.err)))
	}

	if m.adding {
		s.WriteString("\nAdd Torrent:\n")
		s.WriteString(m.textInput.View())
		s.WriteString("\n\n(Press Esc to cancel, Enter to submit)")
	} else {
		s.WriteString("\nControls: [a] Add • [p] Pause • [s] Start/Resume • [r] Remove • [q] Quit\nNav:      [↑/↓] Select torrent")
	}

	return s.String()
}
