package main
import (
	"errors"
	"fmt"
	"io"
	"github.com/jackpal/bencode-go"
)
type byteScanner struct {
	data []byte
	pos  int
}
func (b *byteScanner) ReadByte() (byte, error) {
	if b.pos >= len(b.data) { return 0, io.EOF }
	c := b.data[b.pos]
	b.pos++
	return c, nil
}
func (b *byteScanner) UnreadByte() error {
	if b.pos > 0 { b.pos--; return nil }
	return errors.New("cannot unread")
}
func (b *byteScanner) Read(p []byte) (int, error) {
	if len(p) == 0 { return 0, nil }
	c, err := b.ReadByte()
	if err != nil { return 0, err }
	p[0] = c
	return 1, nil
}
func main() {
	raw := []byte("d8:msg_typei1e5:piecei0eeRAW_DATA_HERE")
	scanner := &byteScanner{data: raw}
	val, err := bencode.Decode(scanner)
	fmt.Printf("%+v %v %d %s\n", val, err, scanner.pos, string(raw[scanner.pos:]))
}
