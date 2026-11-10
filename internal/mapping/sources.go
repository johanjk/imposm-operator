package mapping

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	v1 "github.com/johanjk/imposm-operator/api/v1alpha1"
	"github.com/johanjk/imposm-operator/internal/limitto"
)

// KeySource reads the ConfigMaps a spec refers to. The controller backs it
// with the API (ConfigMap data), the runner with the mounted directories
// (<dir>/<configmap>/<key>), so both resolve a spec the same way.
type KeySource interface {
	// Keys lists the keys of a ConfigMap.
	Keys(configMap string) ([]string, error)
	// Get returns one key's value; an error wrapping os.ErrNotExist when the
	// key does not exist.
	Get(configMap, key string) ([]byte, error)
}

// IsPartName reports whether a ConfigMap key or file is a mapping part when
// no keys are listed: *.yaml, *.yml and *.json, except dotfiles (the "..data"
// links of mounted ConfigMaps) and kustomization files (kept next to the
// parts by configMapGenerator setups).
func IsPartName(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasPrefix(strings.ToLower(name), "kustomization.") {
		return false
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yaml", ".yml", ".json":
		return true
	}
	return false
}

// FromSources resolves a spec's mapping sources into parts, in order: inline
// sources, and the listed (or all part-named) keys of each ConfigMap.
func FromSources(srcs []v1.MappingSource, ks KeySource) ([]Part, error) {
	var parts []Part
	for i, src := range srcs {
		if src.Inline != "" {
			parts = append(parts, Part{Name: fmt.Sprintf("inline-%d", i), Data: []byte(src.Inline)})
			continue
		}
		if src.ConfigMapRef == nil {
			continue
		}
		cm := src.ConfigMapRef.Name
		keys := src.ConfigMapRef.Keys
		if len(keys) == 0 {
			all, err := ks.Keys(cm)
			if err != nil {
				return nil, fmt.Errorf("mapping ConfigMap %s: %w", cm, err)
			}
			for _, k := range all {
				if IsPartName(k) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			if len(keys) == 0 {
				return nil, fmt.Errorf("mapping ConfigMap %s has no .yaml/.yml/.json keys", cm)
			}
		}
		for _, k := range keys {
			data, err := ks.Get(cm, k)
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("mapping ConfigMap %s has no key %q", cm, k)
			}
			if err != nil {
				return nil, fmt.Errorf("mapping ConfigMap %s key %s: %w", cm, k, err)
			}
			if len(data) > MaxPartSize {
				return nil, fmt.Errorf("mapping ConfigMap %s key %s: larger than %d bytes", cm, k, MaxPartSize)
			}
			parts = append(parts, Part{Name: cm + "/" + k, Data: data})
		}
	}
	return parts, nil
}

// LimitTo resolves and validates imposm.limitTo (nil when unset).
func LimitTo(lt *v1.LimitToSpec, ks KeySource) ([]byte, error) {
	if lt == nil {
		return nil, nil
	}
	data := []byte(lt.Inline)
	if ref := lt.ConfigMapRef; ref != nil {
		var err error
		if data, err = ks.Get(ref.Name, ref.Key); errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("limitTo ConfigMap %s has no key %q", ref.Name, ref.Key)
		} else if err != nil {
			return nil, fmt.Errorf("limitTo ConfigMap %s: %w", ref.Name, err)
		}
	}
	if _, err := limitto.Validate(data); err != nil {
		return nil, err
	}
	return data, nil
}

// Dirs is a KeySource over mounted ConfigMaps: <Root>/<configmap>/<key>.
type Dirs struct{ Root string }

func (d Dirs) Keys(cm string) ([]string, error) {
	ents, err := os.ReadDir(filepath.Join(d.Root, cm))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		fi, err := os.Stat(filepath.Join(d.Root, cm, e.Name())) // follow ConfigMap symlinks
		if err == nil && fi.Mode().IsRegular() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

func (d Dirs) Get(cm, key string) ([]byte, error) {
	p := filepath.Join(d.Root, cm, key)
	fi, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	if fi.Size() > MaxPartSize {
		return nil, fmt.Errorf("%s: larger than %d bytes", p, MaxPartSize)
	}
	return os.ReadFile(p)
}
