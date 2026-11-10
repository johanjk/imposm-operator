package limitto

import "testing"

const westMonaco = `{"type":"FeatureCollection","features":[{"type":"Feature","properties":{},
"geometry":{"type":"Polygon","coordinates":[[[7.40,43.72],[7.42,43.72],[7.42,43.76],[7.40,43.76],[7.40,43.72]]]}}]}`

func TestValidate(t *testing.T) {
	if n, err := Validate([]byte(westMonaco)); err != nil || n != 1 {
		t.Fatalf("%d %v", n, err)
	}
	for name, bad := range map[string]string{
		"point":      `{"type":"Point","coordinates":[7,43]}`,
		"open ring":  `{"type":"Polygon","coordinates":[[[7,43],[8,43],[8,44],[7,44]]]}`,
		"mercator":   `{"type":"Polygon","coordinates":[[[825000,5420000],[826000,5420000],[826000,5421000],[825000,5420000]]]}`,
		"empty fc":   `{"type":"FeatureCollection","features":[]}`,
		"not json":   `polygon please`,
		"line in fc": `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"LineString","coordinates":[[7,43],[8,44]]}}]}`,
	} {
		if _, err := Validate([]byte(bad)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func FuzzValidate(f *testing.F) {
	f.Add([]byte(westMonaco))
	f.Add([]byte(`{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1],[0,0]]]]}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		n, err := Validate(b)
		if err == nil && n < 1 {
			t.Fatal("valid but no polygons")
		}
	})
}
