// Package limitto validates imposm limitto GeoJSON: a Polygon or
// MultiPolygon, or a Feature / FeatureCollection of them, in EPSG:4326.
package limitto

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// MaxSize bounds the GeoJSON (ConfigMaps cannot exceed 1 MiB anyway).
const MaxSize = 4 << 20

type object struct {
	Type        string            `json:"type"`
	Coordinates json.RawMessage   `json:"coordinates"`
	Geometry    *object           `json:"geometry"`
	Features    []json.RawMessage `json:"features"`
}

// Validate checks the GeoJSON and returns the number of polygons.
func Validate(data []byte) (int, error) {
	if len(data) > MaxSize {
		return 0, fmt.Errorf("limitTo: larger than %d bytes", MaxSize)
	}
	var o object
	if err := json.Unmarshal(data, &o); err != nil {
		return 0, fmt.Errorf("limitTo: %w", err)
	}
	n, err := polygons(&o, 0)
	if err != nil {
		return 0, fmt.Errorf("limitTo: %w", err)
	}
	if n == 0 {
		return 0, errors.New("limitTo: no polygons")
	}
	return n, nil
}

func polygons(o *object, depth int) (int, error) {
	if depth > 3 {
		return 0, errors.New("nesting too deep")
	}
	switch o.Type {
	case "Polygon":
		var rings [][][]float64
		if err := json.Unmarshal(o.Coordinates, &rings); err != nil {
			return 0, fmt.Errorf("polygon coordinates: %w", err)
		}
		return 1, checkPolygon(rings)
	case "MultiPolygon":
		var polys [][][][]float64
		if err := json.Unmarshal(o.Coordinates, &polys); err != nil {
			return 0, fmt.Errorf("multipolygon coordinates: %w", err)
		}
		if len(polys) == 0 {
			return 0, errors.New("empty multipolygon")
		}
		for _, p := range polys {
			if err := checkPolygon(p); err != nil {
				return 0, err
			}
		}
		return len(polys), nil
	case "Feature":
		if o.Geometry == nil {
			return 0, errors.New("feature without geometry")
		}
		return polygons(o.Geometry, depth+1)
	case "FeatureCollection":
		total := 0
		for i, raw := range o.Features {
			var f object
			if err := json.Unmarshal(raw, &f); err != nil {
				return 0, fmt.Errorf("feature %d: %w", i, err)
			}
			if f.Type != "Feature" {
				return 0, fmt.Errorf("feature %d: type %q", i, f.Type)
			}
			n, err := polygons(&f, depth+1)
			if err != nil {
				return 0, fmt.Errorf("feature %d: %w", i, err)
			}
			total += n
		}
		return total, nil
	}
	return 0, fmt.Errorf("unsupported type %q (imposm supports Polygon and MultiPolygon)", o.Type)
}

func checkPolygon(rings [][][]float64) error {
	if len(rings) == 0 {
		return errors.New("polygon without rings")
	}
	for _, ring := range rings {
		if len(ring) < 4 {
			return errors.New("ring with fewer than 4 positions")
		}
		for _, pos := range ring {
			if len(pos) < 2 || math.IsNaN(pos[0]) || math.IsNaN(pos[1]) ||
				pos[0] < -180 || pos[0] > 180 || pos[1] < -90 || pos[1] > 90 {
				return fmt.Errorf("position %v is not an EPSG:4326 longitude/latitude", pos)
			}
		}
		first, last := ring[0], ring[len(ring)-1]
		if first[0] != last[0] || first[1] != last[1] {
			return errors.New("ring is not closed")
		}
	}
	return nil
}

// ParseBuffer parses a cacheBufferDegrees value ("" is 0).
func ParseBuffer(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f < 0 || f > 10 || math.IsNaN(f) {
		return 0, fmt.Errorf("cacheBufferDegrees %q: must be a number between 0 and 10", s)
	}
	return f, nil
}

// Hash is the sha256 of the GeoJSON bytes.
func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
