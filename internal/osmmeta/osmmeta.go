// Package osmmeta keeps OpenStreetMap element metadata (version, timestamp,
// changeset), optionally every version of every element (an audit trail)
// and the changesets themselves, in PostgreSQL. It is self-contained: give
// it a snapshot (.osm.pbf), replication diffs (.osc.gz) and changeset
// replication files, and it maintains the tables, idempotently and
// resumably. It does not use imposm or the operator: the operator feeds it
// the same snapshot and diffs it feeds imposm. Only the join knows imposm:
// EnsureViews writes the element function (imposm's id rules) and the views
// over imposm's tables. See docs/design-osm-metadata.md.
//
// Typical use:
//
//	st, _ := osmmeta.NewPostgres(pool, osmmeta.Config{Schema: "osm_history", Owner: "planet", Elements: true, History: osmmeta.HistoryFull})
//	_ = st.Ensure(ctx)
//	_, _ = st.LoadSnapshot(ctx, "planet.osm.pbf", osmmeta.Position{Sequence: seq, Timestamp: ts})
//	_, _ = st.ApplyDiffs(ctx, []osmmeta.DiffFile{{Path: "000/004/968.osc.gz", Sequence: 4968}}, next)
package osmmeta

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// History modes.
const (
	HistoryVersions = "versions"
	HistoryFull     = "full"
)

// Action of a version.
const (
	ActionCreate   = "create"
	ActionModify   = "modify"
	ActionDelete   = "delete"
	ActionBaseline = "baseline" // the element's state when the sync started
)

// Member of a relation version.
type Member struct {
	Type string `json:"type"` // node, way, relation
	Ref  int64  `json:"ref"`
	Role string `json:"role"`
}

// Version is one version of an element, from a snapshot or a diff.
type Version struct {
	Type      byte // 'n', 'w', 'r'
	ID        int64
	Version   int32
	Timestamp time.Time
	Changeset int64
	Action    string
	Tags      map[string]string
	// Content (history mode full): node position, way nodes, relation
	// members.
	HasCoord bool
	Lon, Lat float64
	Nodes    []int64
	Members  []Member
}

// Tagged: the version has at least one tag.
func (v *Version) Tagged() bool { return len(v.Tags) > 0 }

// Position is how far a stream (element metadata, changesets) got.
type Position struct {
	Sequence  int64
	Timestamp time.Time
	// Source identifies what the position belongs to (the snapshot and
	// replication URLs, or the changeset area).
	Source string
}

// Streams in the position table.
const (
	StreamOwner      = "owner" // Source: who uses the schema (Config.Owner)
	StreamElements   = "elements"
	StreamChangesets = "changesets"
	StreamPruned     = "pruned"     // Timestamp: the last finished pruning
	StreamContent    = "content"    // Timestamp: content cleared up to here
	StreamMissing    = "missing"    // Sequence: changesets missing (daily)
	StreamBBox       = "bbox"       // Source: the snapshot header's bbox
	StreamPrunedRows = "prunedrows" // Sequence: rows the last pruning removed
)

// Config is what a store keeps, from spec.metadata.
type Config struct {
	// Schema holds the tables of one store; it is claimed by Owner.
	Schema string
	// Owner identifies the user of the schema (e.g. the sync). Ensure
	// refuses a schema another owner claimed (ErrSchemaOwned), so two stores
	// never write the same tables. Empty: no claim.
	Owner string
	// Elements: element metadata is kept (otherwise only changesets).
	Elements bool
	// AllNodes keeps untagged nodes too (elements: all).
	AllNodes bool
	// History: "" (none), HistoryVersions or HistoryFull.
	History    string
	IndexTags  bool
	Changesets bool
	IndexText  bool
	Retention  Retention
	// ChangesetRetention: changesets closed longer ago (0: kept).
	ChangesetRetention time.Duration
}

// Retention of the history (zero values: keep).
type Retention struct {
	MaxAge      time.Duration
	MaxVersions int
	Deleted     time.Duration
	Content     time.Duration
}

// Full keeps each version's content.
func (c Config) Full() bool { return c.History == HistoryFull }

// HasHistory keeps every version.
func (c Config) HasHistory() bool {
	return c.History == HistoryVersions || c.History == HistoryFull
}

var durRe = regexp.MustCompile(`^([0-9]{1,6})([dh])$`)

// ParseDuration parses "<n>d" or "<n>h" ("" is 0).
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	m := durRe.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("duration %q: want <n>d or <n>h", s)
	}
	n, _ := strconv.Atoi(m[1])
	if m[2] == "d" {
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.Duration(n) * time.Hour, nil
}

// BBox is a lon/lat bounding box.
type BBox struct{ MinLon, MinLat, MaxLon, MaxLat float64 }

// String is "minLon,minLat,maxLon,maxLat".
func (b BBox) String() string {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	return f(b.MinLon) + "," + f(b.MinLat) + "," + f(b.MaxLon) + "," + f(b.MaxLat)
}

// Intersects reports whether the boxes overlap (touching counts).
func (b BBox) Intersects(o BBox) bool {
	return b.MinLon <= o.MaxLon && o.MinLon <= b.MaxLon && b.MinLat <= o.MaxLat && o.MinLat <= b.MaxLat
}

// ParseBBox parses "minLon,minLat,maxLon,maxLat".
func ParseBBox(s string) (BBox, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return BBox{}, fmt.Errorf("bbox %q: want minLon,minLat,maxLon,maxLat", s)
	}
	var v [4]float64
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return BBox{}, fmt.Errorf("bbox %q: %w", s, err)
		}
		v[i] = f
	}
	b := BBox{v[0], v[1], v[2], v[3]}
	if b.MinLon < -180 || b.MaxLon > 180 || b.MinLat < -90 || b.MaxLat > 90 || b.MinLon > b.MaxLon || b.MinLat > b.MaxLat {
		return BBox{}, fmt.Errorf("bbox %q: out of range or min > max", s)
	}
	return b, nil
}

// ErrSchemaOwned: the schema's tables belong to another owner.
type ErrSchemaOwned struct{ Schema, Owner string }

func (e *ErrSchemaOwned) Error() string {
	return fmt.Sprintf("osmmeta: schema %s is used by %s: give this one its own schema, or, if %s no longer exists: "+
		"DELETE FROM %s.position WHERE stream = 'owner'", e.Schema, e.Owner, e.Owner, e.Schema)
}

// ErrNoMetadata: changesets are kept but the snapshot or a diff has no
// changeset ids to join them by (the public Geofabrik downloads strip them).
// Without changesets such a source is accepted: versions and timestamps are
// kept, the changeset column stays NULL.
var ErrNoMetadata = errors.New("changesets are on but the source has no changeset ids in its elements (public Geofabrik downloads strip them): use a source with full metadata, or turn changesets off to keep versions and timestamps only")

// MetadataCheck counts the versions of a snapshot or diff chunk: a source
// without changeset ids is refused while changesets are on.
type MetadataCheck struct{ versions, withChangeset int64 }

// Add counts v.
func (c *MetadataCheck) Add(v *Version) {
	c.versions++
	if v.Changeset > 0 {
		c.withChangeset++
	}
}

// Err is ErrNoMetadata when changesets are on and the versions (at least
// one) have no changeset ids.
func (c *MetadataCheck) Err(changesets bool) error {
	if changesets && c.versions > 0 && c.withChangeset == 0 {
		return ErrNoMetadata
	}
	return nil
}

// DiffFile is one replication diff.
type DiffFile struct {
	Path     string
	Sequence int64
}

// LoadStats of a snapshot load.
type LoadStats struct {
	Elements int64
	BBox     *BBox
}

// PruneStats of a retention pass.
type PruneStats struct {
	Deleted, Cleared int64
	// Done: every rule finished (otherwise the next run continues).
	Done bool
}

// Stats for the status.
type Stats struct {
	Elements, HistoryRows, Changesets, ChangesetsMissing int64
	HistoryOldest                                        time.Time
	Positions                                            map[string]Position
}

// Store keeps the metadata tables (Postgres; testutil.FakeMeta for engine
// tests).
type Store interface {
	// Ensure creates the schema, tables, indexes and the changes view the
	// config needs.
	Ensure(ctx context.Context) error
	Position(ctx context.Context, stream string) (Position, bool, error)
	SetPosition(ctx context.Context, stream string, p Position) error
	// LoadSnapshot replaces the element metadata with a snapshot's (and adds
	// baseline history rows), and sets the elements position to pos. Nothing
	// changes when it fails (ErrNoMetadata for a snapshot without changeset
	// ids while changesets are on).
	LoadSnapshot(ctx context.Context, pbf string, pos Position) (LoadStats, error)
	// ApplyDiffs applies .osc.gz files in order and sets the elements
	// position to to, in one transaction. Replays change nothing; a gap
	// after the elements position (or another source) is refused.
	ApplyDiffs(ctx context.Context, files []DiffFile, to Position) error
	// ApplyChangesets upserts changesets and sets the changesets position.
	ApplyChangesets(ctx context.Context, cs []Changeset, to Position) error
	// Prune applies the retention until done or the budget is used.
	Prune(ctx context.Context, now time.Time, budget time.Duration) (PruneStats, error)
	// CountMissing counts changesets referenced by diff metadata from the
	// window [since, until) that are not in the changeset table.
	CountMissing(ctx context.Context, since, until time.Time) (int64, error)
	Stats(ctx context.Context) (Stats, error)
	// EnsureViews makes the schema's views exactly views: each an imposm
	// table with its elements' version, timestamp and changeset, named like
	// the table. Views of other tables are dropped; up-to-date ones are left
	// alone. Without element metadata there are none. It also writes the
	// join function element by the id rules of the deployed import's
	// mapping (singleIDSpace: its use_single_id_space), which may differ
	// from the spec's until a re-import is deployed.
	EnsureViews(ctx context.Context, singleIDSpace bool, views []View) error
	// DropViews drops the views. A view follows its table when the table is
	// moved, so they are dropped before imposm moves production tables
	// (imposm drops backup tables without CASCADE) and ensured after.
	DropViews(ctx context.Context) error
}

// View is an imposm table exposed with its elements' metadata.
type View struct {
	Schema, Table string
	// ID is the table's id column; Geometry its geometry column (empty:
	// none, positive ids are then taken as ways).
	ID, Geometry string
}
