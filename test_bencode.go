package main
import (
	"bytes"
	"fmt"
	"github.com/jackpal/bencode-go"
)
type ExtHandshake struct {
	M            map[string]int `bencode:"m"`
	MetadataSize int            `bencode:"metadata_size,omitempty"`
}
func main() {
	raw := []byte("d1:md11:ut_metadatai1ee13:metadata_sizei29462ee")
	var ext ExtHandshake
	err := bencode.Unmarshal(bytes.NewReader(raw), &ext)
	fmt.Printf("%+v %v\n", ext, err)
}
