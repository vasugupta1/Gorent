package magnet

import (
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"net/url"
	"strings"
)

// Magnet represents a parsed magnet link.
type Magnet struct {
	InfoHash    [20]byte
	DisplayName string
	Trackers    []string
}

// Parse parses a magnet URI and returns a Magnet struct.
func Parse(uri string) (*Magnet, error) {
	// If the user pasted an HTML tag, try to extract the href
	if strings.Contains(uri, "href=\"") {
		start := strings.Index(uri, "href=\"") + 6
		end := strings.Index(uri[start:], "\"")
		if end != -1 {
			uri = uri[start : start+end]
		}
	}

	// Unescape HTML entities like &amp;
	uri = html.UnescapeString(uri)

	// Remove any accidental newlines or tabs from multi-line pasting
	uri = strings.ReplaceAll(uri, "\n", "")
	uri = strings.ReplaceAll(uri, "\r", "")
	uri = strings.ReplaceAll(uri, "\t", "")
	uri = strings.ReplaceAll(uri, " ", "")

	// Trim leading/trailing spaces
	uri = strings.TrimSpace(uri)

	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	if u.Scheme != "magnet" {
		return nil, errors.New("not a magnet link")
	}

	q := u.Query()

	xt := q.Get("xt")
	if !strings.HasPrefix(xt, "urn:btih:") {
		return nil, errors.New("unsupported or missing xt parameter")
	}

	infoHashStr := strings.TrimPrefix(xt, "urn:btih:")
	var infoHash [20]byte

	if len(infoHashStr) == 40 {
		hashBytes, err := hex.DecodeString(infoHashStr)
		if err != nil {
			return nil, fmt.Errorf("invalid infohash hex: %w", err)
		}
		copy(infoHash[:], hashBytes)
	} else if len(infoHashStr) == 32 {
		hashBytes, err := base32.StdEncoding.DecodeString(strings.ToUpper(infoHashStr))
		if err != nil {
			return nil, fmt.Errorf("invalid infohash base32: %w", err)
		}
		copy(infoHash[:], hashBytes)
	} else {
		return nil, errors.New("unsupported infohash length (must be 40-char hex or 32-char base32)")
	}

	m := &Magnet{
		InfoHash:    infoHash,
		DisplayName: q.Get("dn"),
		Trackers:    q["tr"], // url.Values handles multiple "tr" keys as a slice of strings
	}

	return m, nil
}
