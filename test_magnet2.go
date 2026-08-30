package main

import (
	"fmt"
	"github.com/vasugupta1/Gorent/internal/magnet"
)

func main() {
	m, err := magnet.Parse("magnet:?xt=urn:btih:E4A1CB88CF975F6011744B38647AB692D28D8866&dn=Martha%20Wells%20-%20Platform%20Decay%20(The%20Murderbot%20Diaries%20%238)&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337")
	fmt.Println(m, err)
}
