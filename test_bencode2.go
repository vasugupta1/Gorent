package main
import (
	"bytes"
	"fmt"
	"github.com/jackpal/bencode-go"
)
type ExtMetadataMsg struct {
	MsgType   int `bencode:"msg_type"`
	Piece     int `bencode:"piece"`
	TotalSize int `bencode:"total_size,omitempty"`
}
func main() {
	req := ExtMetadataMsg{
		MsgType: 0,
		Piece:   0,
	}
	var buf bytes.Buffer
	err := bencode.Marshal(&buf, req)
	fmt.Printf("%q %v\n", buf.String(), err)
}
