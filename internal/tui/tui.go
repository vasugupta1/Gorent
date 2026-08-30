package tui

import (
	"fmt"
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
	adding        bool
	err           error
	selectedIndex int
}

func InitialModel(manager *engine.TorrentManager) model {
	ti := textinput.New()
	ti.Placeholder = "Paste magnet link here"
	ti.Focus()
	ti.CharLimit = 500
	ti.Width = 50

	return model{
		manager:       manager,
		textInput:     ti,
		adding:        false,
		err:           nil,
		selectedIndex: 0,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(tickCmd(), textinput.Blink)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			if m.adding {
				m.adding = false
				m.textInput.SetValue("")
				return m, nil
			}
			return m, tea.Quit
		case tea.KeyEnter:
			if m.adding {
				uri := m.textInput.Value()
				if uri != "" {
					err := m.manager.AddMagnet(uri, "downloads")
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
			if !m.adding {
				m.selectedIndex--
				if m.selectedIndex < 0 {
					m.selectedIndex = 0
				}
			}
		case tea.KeyDown:
			if !m.adding {
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

		if !m.adding {
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

	if m.adding {
		m.textInput, cmd = m.textInput.Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m model) View() string {
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
