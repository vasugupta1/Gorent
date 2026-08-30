package main
import (
	"bytes"
	"fmt"
)

func findBencodeDictEnd(data []byte) int {
	i := 0
	depth := 0
	for i < len(data) {
		c := data[i]
		if c == 'd' || c == 'l' {
			depth++
			i++
		} else if c == 'i' {
			// skip until 'e'
			end := bytes.IndexByte(data[i:], 'e')
			if end == -1 { return 0 }
			i += end + 1
		} else if c >= '0' && c <= '9' {
			colon := bytes.IndexByte(data[i:], ':')
			if colon == -1 { return 0 }
			var strLen int
			fmt.Sscanf(string(data[i:i+colon]), "%d", &strLen)
			i += colon + 1 + strLen
		} else if c == 'e' {
			depth--
			i++
			if depth == 0 {
				return i
			}
		} else {
			return 0 // invalid
		}
	}
	return 0
}

func main() {
	raw := []byte("d8:msg_typei1e5:piecei0eeRAW_DATA_HERE")
	end := findBencodeDictEnd(raw)
	fmt.Printf("%d %s\n", end, string(raw[end:]))
}
