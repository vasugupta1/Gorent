package torrent

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"time"

	"github.com/vasugupta1/gorent/internal/peers"
)

const (
	protocolID        = 0x41727101980
	actionConnect     = 0
	actionAnnounce    = 1
	actionScrape      = 2
	actionError       = 3
	announceEventNone = 0
	maxRetries        = 3
)

func generateTransactionID() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(b[:])
}

func generateKey() uint32 {
	return generateTransactionID()
}

func buildConnectRequest(txID uint32) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], protocolID)
	binary.BigEndian.PutUint32(buf[8:12], actionConnect)
	binary.BigEndian.PutUint32(buf[12:16], txID)
	return buf
}

func sendAndReceive(conn *net.UDPConn, req []byte, buf []byte) (int, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		timeout := time.Duration(5*(1<<uint(attempt))) * time.Second
		conn.SetDeadline(time.Now().Add(timeout))

		if _, err := conn.Write(req); err != nil {
			lastErr = err
			continue
		}

		n, err := conn.Read(buf)
		if err == nil {
			conn.SetDeadline(time.Time{})
			return n, nil
		}
		lastErr = err
	}

	return 0, fmt.Errorf("all %d UDP attempts failed: %w", maxRetries, lastErr)
}

func parseConnectResponse(data []byte, expectedTxID uint32) (uint64, error) {
	if len(data) < 8 {
		return 0, errors.New("response too short")
	}

	action := binary.BigEndian.Uint32(data[0:4])
	txID := binary.BigEndian.Uint32(data[4:8])

	if action == actionError {
		if len(data) > 8 {
			return 0, fmt.Errorf("tracker error: %s", string(data[8:]))
		}
		return 0, errors.New("tracker returned unknown error")
	}

	if action != actionConnect || txID != expectedTxID {
		return 0, errors.New("action or transaction ID mismatch")
	}

	if len(data) < 16 {
		return 0, errors.New("missing connection ID")
	}

	return binary.BigEndian.Uint64(data[8:16]), nil
}

func buildAnnounceRequest(connID uint64, txID uint32, tf *TorrentFile, peerID [20]byte, port uint16) []byte {
	buf := make([]byte, 98)

	binary.BigEndian.PutUint64(buf[0:8], connID)
	binary.BigEndian.PutUint32(buf[8:12], actionAnnounce)
	binary.BigEndian.PutUint32(buf[12:16], txID)
	copy(buf[16:36], tf.InfoHash[:])
	copy(buf[36:56], peerID[:])
	binary.BigEndian.PutUint64(buf[56:64], 0)                 // downloaded
	binary.BigEndian.PutUint64(buf[64:72], uint64(tf.Length)) // left
	binary.BigEndian.PutUint64(buf[72:80], 0)                 // uploaded
	binary.BigEndian.PutUint32(buf[80:84], announceEventNone)
	binary.BigEndian.PutUint32(buf[84:88], 0) // default IP
	binary.BigEndian.PutUint32(buf[88:92], generateKey())
	binary.BigEndian.PutUint32(buf[92:96], math.MaxUint32) // num_want (-1 for default)
	binary.BigEndian.PutUint16(buf[96:98], port)

	return buf
}

func parseAnnounceResponse(data []byte, expectedTxID uint32) ([]peers.Peer, error) {
	if len(data) < 8 {
		return nil, errors.New("response too short")
	}

	action := binary.BigEndian.Uint32(data[0:4])
	txID := binary.BigEndian.Uint32(data[4:8])

	if action == actionError {
		if len(data) > 8 {
			return nil, fmt.Errorf("tracker error: %s", string(data[8:]))
		}
		return nil, errors.New("tracker returned unknown error")
	}

	if action != actionAnnounce || txID != expectedTxID {
		return nil, errors.New("action or transaction ID mismatch")
	}

	if len(data) < 20 {
		return nil, errors.New("missing announce metadata")
	}

	return peers.UnmarshalPeers(data[20:])
}

func requestPeersUdp(tf *TorrentFile, peerId [20]byte) ([]peers.Peer, error) {
	base, err := url.Parse(tf.Announce)
	if err != nil {
		return nil, err
	}

	if base.Scheme != "udp" {
		return nil, fmt.Errorf("Expected udp scheme but didn't get it once parsed hence something is wrong please check")
	}

	addr, err := net.ResolveUDPAddr("udp", base.Host)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	txID := generateTransactionID()
	connectReq := buildConnectRequest(txID)
	buf := make([]byte, 2048)
	n, err := sendAndReceive(conn, connectReq, buf)
	if err != nil {
		return nil, err
	}
	connID, err := parseConnectResponse(buf[:n], txID)
	if err != nil {
		return nil, fmt.Errorf("connect response failed: %w", err)
	}
	txID = generateTransactionID()
	announceReq := buildAnnounceRequest(connID, txID, tf, peerId, 6881)

	n, err = sendAndReceive(conn, announceReq, buf)
	if err != nil {
		return nil, fmt.Errorf("announce phase failed: %w", err)
	}

	return parseAnnounceResponse(buf[:n], txID)
}
