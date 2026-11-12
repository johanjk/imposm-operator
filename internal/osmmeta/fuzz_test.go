package osmmeta_test

import (
	"strings"
	"testing"

	"github.com/johanjk/imposm-operator/internal/osmmeta"
)

func FuzzReadChange(f *testing.F) {
	f.Add(`<osmChange><create><node id="3" version="1" timestamp="2024-01-01T00:00:00Z" lat="1" lon="2"><tag k="a" v="b"/></node></create>` +
		`<delete><node id="3" version="2" timestamp="2024-01-02T00:00:00Z"/></delete></osmChange>`)
	f.Add(`<osmChange><modify><way id="5" version="2" timestamp="2024-01-01T00:00:00Z"><nd ref="1"/><tag k="x" v="y"/></way>` +
		`<relation id="-9" version="1" timestamp="2024-01-01T00:00:00Z"><member type="way" ref="5" role="outer"/></relation></modify></osmChange>`)
	f.Add(`<osmChange><modify><way id="x"><tag/></way>`)
	f.Fuzz(func(t *testing.T, in string) {
		var got []osmmeta.Version
		err := osmmeta.ReadChange(strings.NewReader(in), osmmeta.ReadOptions{Content: true}, func(v *osmmeta.Version) error {
			got = append(got, *v)
			return nil
		})
		if err != nil {
			return
		}
		for _, v := range got {
			if v.ID == 0 || v.Version <= 0 || v.Timestamp.IsZero() {
				t.Fatalf("version without id, version or timestamp: %+v", v)
			}
			if v.Type != 'n' && v.Type != 'w' && v.Type != 'r' {
				t.Fatalf("type %q", v.Type)
			}
			if v.Action == osmmeta.ActionDelete && (v.Tags != nil || v.HasCoord || v.Nodes != nil || v.Members != nil) {
				t.Fatalf("deletion with content: %+v", v)
			}
			if len(v.Tags) > 5000 || len(v.Nodes) > 8000 || len(v.Members) > 128000 {
				t.Fatalf("beyond the limits: %d tags, %d nodes, %d members", len(v.Tags), len(v.Nodes), len(v.Members))
			}
		}
	})
}
