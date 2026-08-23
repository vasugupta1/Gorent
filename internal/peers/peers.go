package peers

import (
	"encoding/binary"
	"fmt"
	"net"
)

type Peer struct {
	IP   net.IP
	Port uint16
}

func UnmarshalPeers(peerBytes []byte) ([]Peer, error) {
	peerSize := 6

	if len(peerBytes)%peerSize != 0 {
		return nil, fmt.Errorf("Malformed Peers")
	}

	numOfPeers := len(peerBytes) / peerSize

	peers := make([]Peer, numOfPeers)

	for i := 0; i < numOfPeers; i++ {
		offSet := i * peerSize
		peers[i].IP = net.IP(peerBytes[offSet : offSet+4])
		peers[i].Port = binary.BigEndian.Uint16([]byte(peerBytes[offSet+4 : offSet+6]))
	}

	return peers, nil
}
