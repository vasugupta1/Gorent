package tracker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"net/url"
	"time"

	"github.com/vasugupta1/Gorent/internal/peer"
)

type UDPTracker struct {
	URL string
}

const (
	protocolID     uint64 = 0x41727101980
	actionConnect  uint32 = 0
	actionAnnounce uint32 = 1
)

func (t *UDPTracker) Announce(infoHash, peerID [20]byte, port uint16, uploaded, downloaded, left int) ([]peer.Peer, error) {
	u, err := url.Parse(t.URL)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("udp", u.Host, 15*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(15 * time.Second))

	// 1. Connect
	transactionID := rand.Uint32()
	connectReq := new(bytes.Buffer)
	binary.Write(connectReq, binary.BigEndian, protocolID)
	binary.Write(connectReq, binary.BigEndian, actionConnect)
	binary.Write(connectReq, binary.BigEndian, transactionID)

	if _, err := conn.Write(connectReq.Bytes()); err != nil {
		return nil, err
	}

	connectResBuf := make([]byte, 16)
	if _, err := conn.Read(connectResBuf); err != nil {
		return nil, err
	}

	if binary.BigEndian.Uint32(connectResBuf[0:4]) != actionConnect {
		return nil, fmt.Errorf("invalid connect response action")
	}
	if binary.BigEndian.Uint32(connectResBuf[4:8]) != transactionID {
		return nil, fmt.Errorf("transaction ID mismatch in connect")
	}
	connectionID := binary.BigEndian.Uint64(connectResBuf[8:16])

	// 2. Announce
	announceReq := new(bytes.Buffer)
	binary.Write(announceReq, binary.BigEndian, connectionID)
	binary.Write(announceReq, binary.BigEndian, actionAnnounce)
	transactionID = rand.Uint32()
	binary.Write(announceReq, binary.BigEndian, transactionID)
	announceReq.Write(infoHash[:])
	announceReq.Write(peerID[:])
	binary.Write(announceReq, binary.BigEndian, int64(downloaded))
	binary.Write(announceReq, binary.BigEndian, int64(left))
	binary.Write(announceReq, binary.BigEndian, int64(uploaded))
	binary.Write(announceReq, binary.BigEndian, uint32(0))     // event (0=none)
	binary.Write(announceReq, binary.BigEndian, uint32(0))     // IP (default)
	binary.Write(announceReq, binary.BigEndian, rand.Uint32()) // key
	binary.Write(announceReq, binary.BigEndian, int32(-1))     // num_want (-1 default)
	binary.Write(announceReq, binary.BigEndian, port)

	if _, err := conn.Write(announceReq.Bytes()); err != nil {
		return nil, err
	}

	announceResBuf := make([]byte, 2048)
	n, err := conn.Read(announceResBuf)
	if err != nil {
		return nil, err
	}

	if n < 20 {
		return nil, fmt.Errorf("announce response too short")
	}

	action := binary.BigEndian.Uint32(announceResBuf[0:4])
	if action == 3 {
		return nil, fmt.Errorf("tracker returned error: %s", string(announceResBuf[8:n]))
	}
	if action != actionAnnounce {
		return nil, fmt.Errorf("invalid announce response action")
	}

	return unmarshalPeers(announceResBuf[20:n])
}
