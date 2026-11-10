// Command pbfheader prints the replication fields of a PBF header.
package main

import (
	"fmt"
	"os"

	"github.com/johanjk/imposm-operator/internal/osm"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: pbfheader FILE")
		os.Exit(2)
	}
	h, err := osm.ReadPBFHeaderFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("base_url=%s\nsequence=%d\ntimestamp=%s\n", h.BaseURL, h.Sequence, h.Timestamp.Format("2006-01-02T15:04:05Z"))
}
