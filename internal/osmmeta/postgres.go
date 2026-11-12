package osmmeta

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/johanjk/imposm-operator/internal/strutil"
)

// Postgres keeps the metadata tables in PostgreSQL.
type Postgres struct {
	pool *pgxpool.Pool
	cfg  Config
}

// NewPostgres validates the config's names.
func NewPostgres(pool *pgxpool.Pool, cfg Config) (*Postgres, error) {
	if !strutil.IsUserSchema(cfg.Schema) {
		return nil, fmt.Errorf("osmmeta: invalid schema %q", cfg.Schema)
	}
	return &Postgres{pool: pool, cfg: cfg}, nil
}

// Table names.
const (
	tMeta       = "element_meta"
	tHistory    = "element_history"
	tSuperseded = "history_superseded"
	tBreaks     = "history_breaks"
	tRetention  = "history_retention"
	tChangesets = "changesets"
	tPosition   = "position"
	tStaging    = "snapshot_load"
	tMetaNew    = "element_meta_new"
	fElement    = "element"
	vChanges    = "element_changes"
)

func quote(s string) string { return pgx.Identifier{s}.Sanitize() }

// t is the qualified, quoted name of a table of this sync.
func (p *Postgres) t(name string) string { return pgx.Identifier{p.cfg.Schema, name}.Sanitize() }

// execer is a pool, connection or transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// execAll runs statements in order; an error names the failing one.
func execAll(ctx context.Context, q execer, stmts ...string) error {
	for _, s := range stmts {
		if _, err := q.Exec(ctx, s); err != nil {
			return fmt.Errorf("osmmeta: %w: %s", err, firstLine(s))
		}
	}
	return nil
}

// setPos writes a stream's position.
func (p *Postgres) setPos(ctx context.Context, q execer, stream string, pos Position) error {
	_, err := q.Exec(ctx, `INSERT INTO `+p.t(tPosition)+` (stream, sequence, "timestamp", source, updated_at) VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (stream) DO UPDATE SET sequence = EXCLUDED.sequence, "timestamp" = EXCLUDED."timestamp",
		source = EXCLUDED.source, updated_at = now()`, stream, pos.Sequence, tsArg(pos.Timestamp), pos.Source)
	return err
}

// Content columns: a version's tags, node position, way nodes and relation
// members (history mode full).
const (
	contentDDL   = `tags jsonb, lon double precision, lat double precision, nodes bigint[], members jsonb`
	contentClear = `tags = NULL, lon = NULL, lat = NULL, nodes = NULL, members = NULL`
)

// Ensure creates what the config needs. Optional indexes that were turned
// off are dropped; tables are never dropped here.
func (p *Postgres) Ensure(ctx context.Context) error {
	var stmts []string
	add := func(s ...string) { stmts = append(stmts, s...) }
	if p.cfg.Elements {
		add(`CREATE TABLE IF NOT EXISTS ` + p.t(tMeta) + metaColumns)
		add(p.metaIndexes(tMeta, "")...)
	}
	if p.cfg.HasHistory() {
		add(`CREATE TABLE IF NOT EXISTS `+p.t(tHistory)+` (
			osm_type "char" NOT NULL, osm_id bigint NOT NULL, version integer NOT NULL,
			"timestamp" timestamptz NOT NULL, changeset bigint, action text NOT NULL,
			sequence bigint NOT NULL, recorded_at timestamptz NOT NULL DEFAULT now(),
			`+contentDDL+`,
			CONSTRAINT `+quote("element_history_pkey")+` PRIMARY KEY (osm_type, osm_id, version))`,
			`CREATE INDEX IF NOT EXISTS `+quote("element_history_changeset")+` ON `+p.t(tHistory)+` (changeset)`,
			`CREATE INDEX IF NOT EXISTS `+quote("element_history_timestamp")+` ON `+p.t(tHistory)+` ("timestamp") WHERE action <> 'baseline'`,
			`CREATE INDEX IF NOT EXISTS `+quote("element_history_sequence")+` ON `+p.t(tHistory)+` USING brin (sequence)`,
			`CREATE TABLE IF NOT EXISTS `+p.t(tSuperseded)+` (
			osm_type "char" NOT NULL, osm_id bigint NOT NULL, version integer NOT NULL,
			superseded_at timestamptz NOT NULL,
			CONSTRAINT `+quote("history_superseded_pkey")+` PRIMARY KEY (osm_type, osm_id, version))`,
			`CREATE INDEX IF NOT EXISTS `+quote("history_superseded_at")+` ON `+p.t(tSuperseded)+` (superseded_at)`,
			`CREATE TABLE IF NOT EXISTS `+p.t(tBreaks)+` (
			id bigserial PRIMARY KEY, sequence_from bigint, sequence_to bigint, reason text NOT NULL,
			at timestamptz NOT NULL DEFAULT now())`,
			`CREATE TABLE IF NOT EXISTS `+p.t(tRetention)+` (
			id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now(), rule text NOT NULL,
			cutoff timestamptz, rows_deleted bigint NOT NULL DEFAULT 0, rows_cleared bigint NOT NULL DEFAULT 0)`)
		tagsIdx := quote("element_history_tags")
		if p.cfg.Full() && p.cfg.IndexTags {
			add(`CREATE INDEX IF NOT EXISTS ` + tagsIdx + ` ON ` + p.t(tHistory) + ` USING gin (tags jsonb_path_ops) WHERE action <> 'baseline'`)
		} else {
			add(`DROP INDEX IF EXISTS ` + quote(p.cfg.Schema) + `.` + tagsIdx)
		}
		if p.cfg.Full() {
			add(p.changesView())
		}
	}
	if p.cfg.Changesets {
		add(`CREATE TABLE IF NOT EXISTS `+p.t(tChangesets)+` (
			id bigint PRIMARY KEY, created_at timestamptz NOT NULL, closed_at timestamptz,
			open boolean NOT NULL, num_changes integer, comment text, tags jsonb NOT NULL,
			bbox geometry(Polygon, 4326))`,
			`CREATE INDEX IF NOT EXISTS `+quote("changesets_bbox")+` ON `+p.t(tChangesets)+` USING gist (bbox)`,
			`CREATE INDEX IF NOT EXISTS `+quote("changesets_closed_at")+` ON `+p.t(tChangesets)+` (closed_at)`,
			`CREATE INDEX IF NOT EXISTS `+quote("changesets_open")+` ON `+p.t(tChangesets)+` (id) WHERE open`)
		textIdx := quote("changesets_comment")
		if p.cfg.IndexText {
			add(`CREATE INDEX IF NOT EXISTS ` + textIdx + ` ON ` + p.t(tChangesets) + ` USING gin (to_tsvector('simple', coalesce(comment, '')))`)
		} else {
			add(`DROP INDEX IF EXISTS ` + quote(p.cfg.Schema) + `.` + textIdx)
		}
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if err := p.claim(ctx, tx); err != nil {
			return err
		}
		return execAll(ctx, tx, stmts...)
	})
}

// claim creates the schema and the position table, refuses them unless the
// database user owns them, and records Owner as the schema's user, or
// refuses a schema another owner holds. Another database user's schema or
// table is refused before anything is written to it: its owner could have
// put a trigger on it, run with this user's rights.
func (p *Postgres) claim(ctx context.Context, tx pgx.Tx) error {
	if err := execAll(ctx, tx, `CREATE SCHEMA IF NOT EXISTS `+quote(p.cfg.Schema)); err != nil {
		return err
	}
	if err := p.checkOwned(ctx, tx, `SELECT nspowner FROM pg_namespace WHERE nspname = $1`, p.cfg.Schema, "schema "+p.cfg.Schema); err != nil {
		return err
	}
	if err := execAll(ctx, tx, `CREATE TABLE IF NOT EXISTS `+p.t(tPosition)+` (
			stream text PRIMARY KEY, sequence bigint NOT NULL DEFAULT 0, "timestamp" timestamptz,
			source text NOT NULL DEFAULT '', updated_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	if err := p.checkOwned(ctx, tx, `SELECT relowner FROM pg_class WHERE oid = to_regclass($1)`, p.t(tPosition), "table "+p.cfg.Schema+"."+tPosition); err != nil {
		return err
	}
	if p.cfg.Owner == "" {
		return nil
	}
	// the row lock serializes two stores claiming the same new schema
	var owner string
	err := tx.QueryRow(ctx, `INSERT INTO `+p.t(tPosition)+` (stream, source) VALUES ($1, $2)
		ON CONFLICT (stream) DO UPDATE SET stream = EXCLUDED.stream RETURNING source`, StreamOwner, p.cfg.Owner).Scan(&owner)
	if err != nil {
		return fmt.Errorf("osmmeta: claiming schema %s: %w", p.cfg.Schema, err)
	}
	if owner != p.cfg.Owner {
		return &ErrSchemaOwned{Schema: p.cfg.Schema, Owner: owner}
	}
	return nil
}

// checkOwned refuses an object (its owner from query) that the database
// user neither owns nor is a member of the owner role of.
func (p *Postgres) checkOwned(ctx context.Context, tx pgx.Tx, query, arg, what string) error {
	var ok bool
	var owner string
	err := tx.QueryRow(ctx, `SELECT pg_has_role(current_user, o, 'MEMBER'), o::regrole::text FROM (`+query+`) x(o)`, arg).Scan(&ok, &owner)
	if err != nil {
		return fmt.Errorf("osmmeta: owner of %s: %w", what, err)
	}
	if !ok {
		return fmt.Errorf("osmmeta: %s is owned by %s, not by this sync's database user: give the sync a schema of its own, "+
			"or create it for the user (CREATE SCHEMA %s AUTHORIZATION <user>)", what, owner, p.cfg.Schema)
	}
	return nil
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	return s
}

const metaColumns = ` (
	osm_type "char" NOT NULL, osm_id bigint NOT NULL, version integer NOT NULL,
	"timestamp" timestamptz NOT NULL, changeset bigint, deleted boolean NOT NULL DEFAULT false,
	sequence bigint NOT NULL)`

// metaIndexDefs are the element metadata table's indexes: name and
// definition.
var metaIndexDefs = [][2]string{
	{"element_meta_changeset", `(changeset)`},
	{"element_meta_timestamp", `("timestamp")`},
	{"element_meta_deleted", `("timestamp") WHERE deleted`},
}

// metaIndexes of the element metadata table; suffix names a table being
// built (its indexes are renamed when it is swapped in).
func (p *Postgres) metaIndexes(table, suffix string) []string {
	out := []string{
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = ` + lit("element_meta_pkey"+suffix) +
			` AND connamespace = ` + lit(p.cfg.Schema) + `::regnamespace) THEN ALTER TABLE ` + p.t(table) +
			` ADD CONSTRAINT ` + quote("element_meta_pkey"+suffix) + ` PRIMARY KEY (osm_type, osm_id); END IF; END $$`}
	for _, ix := range metaIndexDefs {
		out = append(out, `CREATE INDEX IF NOT EXISTS `+quote(ix[0]+suffix)+` ON `+p.t(table)+` `+ix[1])
	}
	return out
}

func lit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// viewMarker starts the comment of every view EnsureViews creates, followed
// by the owner and a fingerprint of the view's definition and its table.
const viewMarker = "osmmeta view "

// viewMark is the comment of a view of this store.
func (p *Postgres) viewMark(fingerprint string) string {
	return viewMarker + p.cfg.Owner + " " + fingerprint
}

// viewSQL is a view's definition.
func (p *Postgres) viewSQL(v View) string {
	geom := "NULL::geometry"
	if v.Geometry != "" {
		geom = "t." + quote(v.Geometry)
	}
	return `SELECT t.*, m.version AS osm_version, m."timestamp" AS osm_timestamp, m.changeset AS osm_changeset
	FROM ` + quote(v.Schema) + "." + quote(v.Table) + ` t
	CROSS JOIN LATERAL ` + p.t(fElement) + `(t.` + quote(v.ID) + `, ` + geom + `) e
	LEFT JOIN ` + p.t(tMeta) + ` m ON m.osm_type = e.osm_type AND m.osm_id = e.osm_id`
}

// ownNames are the store's own tables, functions and views.
var ownNames = map[string]bool{tMeta: true, tHistory: true, tSuperseded: true, tBreaks: true, tRetention: true,
	tChangesets: true, tPosition: true, tStaging: true, tMetaNew: true, fElement: true, vChanges: true}

func (p *Postgres) EnsureViews(ctx context.Context, singleIDSpace bool, views []View) error {
	var conflicts []string
	err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// another sync's schema is refused before its views or function
		// are touched
		if err := p.claim(ctx, tx); err != nil {
			return err
		}
		own, stray, err := p.listViews(ctx, tx)
		if err != nil {
			return err
		}
		if err := p.dropViews(ctx, tx, stray, "left from a previous metadata schema"); err != nil {
			return err
		}
		var ready bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, p.t(tMeta)).Scan(&ready); err != nil {
			return err
		}
		ready = ready && p.cfg.Elements
		if ready {
			var postgis string
			if err := tx.QueryRow(ctx, `SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'postgis'`).Scan(&postgis); err != nil {
				return fmt.Errorf("osmmeta: postgis extension: %w", err)
			}
			if err := execAll(ctx, tx, elementFunction(p.t(fElement), postgis, singleIDSpace)); err != nil {
				return err
			}
		}
		want := map[string]bool{}
		for _, v := range views {
			if !ready || v.ID == "" {
				continue
			}
			var oid *uint32
			if err := tx.QueryRow(ctx, `SELECT to_regclass($1)::oid`, pgx.Identifier{v.Schema, v.Table}.Sanitize()).Scan(&oid); err != nil {
				return err
			}
			if oid == nil {
				continue // not deployed (yet)
			}
			name := p.t(v.Table)
			if _, ok := own[v.Table]; !ok {
				// a name taken by something this store did not create: that
				// table gets no view, the others do
				var taken bool
				if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&taken); err != nil {
					return err
				}
				if taken || ownNames[v.Table] {
					conflicts = append(conflicts, v.Table)
					continue
				}
			}
			want[v.Table] = true
			def := p.viewSQL(v)
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d", def, *oid)))
			mark := p.viewMark(hex.EncodeToString(sum[:8]))
			if own[v.Table].mark == mark {
				continue
			}
			if err := execAll(ctx, tx, `DROP VIEW IF EXISTS `+name, `CREATE VIEW `+name+` AS `+def,
				`COMMENT ON VIEW `+name+` IS `+lit(mark)); err != nil {
				return err
			}
		}
		var unwanted []string
		for name := range own {
			if !want[name] {
				unwanted = append(unwanted, p.t(name))
			}
		}
		return p.dropViews(ctx, tx, unwanted, "")
	})
	if err == nil && len(conflicts) > 0 {
		err = fmt.Errorf("osmmeta: no view for %s: the name is taken in %s by an object the operator did not create",
			strings.Join(conflicts, ", "), p.cfg.Schema)
	}
	return err
}

// DropViews drops this store's views, in its schema and (left from a
// previous metadata schema) elsewhere; other views are left alone.
func (p *Postgres) DropViews(ctx context.Context) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		own, stray, err := p.listViews(ctx, tx)
		if err != nil {
			return err
		}
		for name := range own {
			stray = append(stray, p.t(name))
		}
		return p.dropViews(ctx, tx, stray, "something depends on it?")
	})
}

// DropOwnerViews drops the views made for owner in any schema: when a sync
// turns the metadata off, its views would otherwise follow the production
// tables into the backup and fail the next deploy.
func DropOwnerViews(ctx context.Context, pool *pgxpool.Pool, owner string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		views, err := ownerViews(ctx, tx, owner)
		if err != nil {
			return err
		}
		var names []string
		for _, v := range views {
			names = append(names, pgx.Identifier{v.schema, v.name}.Sanitize())
		}
		return (&Postgres{}).dropViews(ctx, tx, names, "")
	})
}

func (p *Postgres) dropViews(ctx context.Context, tx pgx.Tx, names []string, why string) error {
	for _, n := range names {
		if _, err := tx.Exec(ctx, `DROP VIEW IF EXISTS `+n); err != nil {
			if why != "" {
				return fmt.Errorf("osmmeta: drop view %s (%s): %w", n, why, err)
			}
			return fmt.Errorf("osmmeta: drop view %s: %w", n, err)
		}
	}
	return nil
}

// ownView is a view EnsureViews made.
type ownView struct{ schema, name, def, mark string }

// ownerViews are the views made for owner, in any schema: their comment is
// the marker, the owner, a space and a fingerprint.
func ownerViews(ctx context.Context, tx pgx.Tx, owner string) ([]ownView, error) {
	prefix := viewMarker + owner + " "
	rows, err := tx.Query(ctx, `SELECT n.nspname, c.relname, pg_get_viewdef(c.oid), d.description
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_description d ON d.objoid = c.oid AND d.classoid = 'pg_class'::regclass AND d.objsubid = 0
		WHERE c.relkind = 'v' AND starts_with(d.description, $1)
		  AND strpos(substr(d.description, length($1) + 1), ' ') = 0`, prefix)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ownView, error) {
		var v ownView
		err := r.Scan(&v.schema, &v.name, &v.def, &v.mark)
		return v, err
	})
}

// listViews are this store's views: those in its schema by name, and the
// qualified names of those in other schemas (stray: left after
// spec.metadata.schema changed; only with an owner).
func (p *Postgres) listViews(ctx context.Context, tx pgx.Tx) (map[string]ownView, []string, error) {
	views, err := ownerViews(ctx, tx, p.cfg.Owner)
	if err != nil {
		return nil, nil, err
	}
	own := map[string]ownView{}
	var stray []string
	for _, v := range views {
		switch {
		case v.schema == p.cfg.Schema:
			own[v.name] = v
		case p.cfg.Owner != "":
			stray = append(stray, pgx.Identifier{v.schema, v.name}.Sanitize())
		}
	}
	return own, stray, nil
}

// elementFunction maps an imposm row's osm_id (and geometry, which tells
// nodes from ways without a single id space) to the OSM element, by the id
// rules of the deployed import's mapping. PostGIS is called by its schema:
// a function body is resolved with the caller's search_path.
func elementFunction(name, postgis string, singleIDSpace bool) string {
	body := `SELECT CASE WHEN id < 0 THEN 'r' WHEN geom IS NOT NULL AND ` + postgis + `.GeometryType(geom) = 'POINT' THEN 'n' ELSE 'w' END::"char",
		CASE WHEN id < 0 THEN -id ELSE id END`
	if singleIDSpace {
		// imposm: ways -id, relations -1e17 - id
		body = `SELECT CASE WHEN id <= -100000000000000000 THEN 'r' WHEN id < 0 THEN 'w' ELSE 'n' END::"char",
		CASE WHEN id <= -100000000000000000 THEN -100000000000000000 - id WHEN id < 0 THEN -id ELSE id END`
	}
	return `CREATE OR REPLACE FUNCTION ` + name + `(id bigint, geom ` + postgis + `.geometry)
		RETURNS TABLE (osm_type "char", osm_id bigint) LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $f$ ` + body + ` $f$`
}

// changesView: what each version changed against the previous kept one.
// The previous version is looked up per row (primary key), so filters on
// changeset, time or element use their indexes.
func (p *Postgres) changesView() string {
	return `CREATE OR REPLACE VIEW ` + p.t(vChanges) + ` AS
	SELECT h.osm_type, h.osm_id, h.version, h."timestamp", h.changeset, h.action, h.sequence,
		prev.version AS previous_version,
		CASE WHEN h.tags IS NOT NULL AND prev.tags IS NOT NULL THEN
			(SELECT coalesce(jsonb_object_agg(e.key, e.value), '{}') FROM jsonb_each(h.tags) e WHERE NOT prev.tags ? e.key) END AS tags_added,
		CASE WHEN h.tags IS NOT NULL AND prev.tags IS NOT NULL THEN
			(SELECT coalesce(jsonb_object_agg(e.key, e.value), '{}') FROM jsonb_each(prev.tags) e WHERE NOT h.tags ? e.key) END AS tags_removed,
		CASE WHEN h.tags IS NOT NULL AND prev.tags IS NOT NULL THEN
			(SELECT coalesce(jsonb_object_agg(e.key, jsonb_build_array(prev.tags -> e.key, e.value)), '{}')
			 FROM jsonb_each(h.tags) e WHERE prev.tags ? e.key AND prev.tags -> e.key <> e.value) END AS tags_changed,
		CASE WHEN h.osm_type = 'n' AND h.lon IS NOT NULL AND prev.lon IS NOT NULL THEN
			ST_DistanceSphere(ST_MakePoint(prev.lon, prev.lat), ST_MakePoint(h.lon, h.lat)) END AS moved_m,
		CASE WHEN h.osm_type = 'w' AND h.nodes IS NOT NULL AND prev.nodes IS NOT NULL THEN h.nodes IS DISTINCT FROM prev.nodes END AS nodes_changed,
		CASE WHEN h.osm_type = 'r' AND h.members IS NOT NULL AND prev.members IS NOT NULL THEN h.members IS DISTINCT FROM prev.members END AS members_changed
	FROM ` + p.t(tHistory) + ` h
	JOIN LATERAL (SELECT q.version, q.tags, q.lon, q.lat, q.nodes, q.members FROM ` + p.t(tHistory) + ` q
		WHERE q.osm_type = h.osm_type AND q.osm_id = h.osm_id AND q.version < h.version
		ORDER BY q.version DESC LIMIT 1) prev ON true
	WHERE h.action <> 'baseline'`
}

// Position reads a stream's position.
func (p *Postgres) Position(ctx context.Context, stream string) (Position, bool, error) {
	return position(ctx, p.pool, p.t(tPosition), stream)
}

// queryer is a pool, connection or transaction.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func position(ctx context.Context, q queryer, table, stream string) (Position, bool, error) {
	var pos Position
	var ts *time.Time
	err := q.QueryRow(ctx, `SELECT sequence, "timestamp", source FROM `+table+` WHERE stream = $1`, stream).
		Scan(&pos.Sequence, &ts, &pos.Source)
	if errors.Is(err, pgx.ErrNoRows) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, err
	}
	if ts != nil {
		pos.Timestamp = ts.UTC()
	}
	return pos, true, nil
}

// supersededSQL records when each replaced version of the elements in keys
// (a table with osm_type, osm_id) was replaced: the next version's
// timestamp (retention reads this). Content retention passes each time only
// once, so a version replaced before where it got (the old history's last
// version, replaced by a new snapshot's older baseline) is cleared here.
func (p *Postgres) supersededSQL(keys string) string {
	return `WITH ins AS (INSERT INTO ` + p.t(tSuperseded) + ` (osm_type, osm_id, version, superseded_at)
		SELECT osm_type, osm_id, version, next_ts FROM (
			SELECT h.osm_type, h.osm_id, h.version,
				lead(h."timestamp") OVER (PARTITION BY h.osm_type, h.osm_id ORDER BY h.version) AS next_ts
			FROM ` + p.t(tHistory) + ` h JOIN ` + keys + ` k USING (osm_type, osm_id)) x
		WHERE next_ts IS NOT NULL
		ON CONFLICT DO NOTHING
		RETURNING osm_type, osm_id, version, superseded_at)
	UPDATE ` + p.t(tHistory) + ` h SET ` + contentClear + `
	FROM ins WHERE h.osm_type = ins.osm_type AND h.osm_id = ins.osm_id AND h.version = ins.version
		AND ` + fmt.Sprint(p.cfg.Retention.Content > 0) + `
		AND ins.superseded_at < (SELECT "timestamp" FROM ` + p.t(tPosition) + ` WHERE stream = 'content')`
}

// SetPosition writes a stream's position.
func (p *Postgres) SetPosition(ctx context.Context, stream string, pos Position) error {
	return p.setPos(ctx, p.pool, stream, pos)
}

func tsArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// rowSource feeds COPY from a producer goroutine.
type rowSource struct {
	rows chan []any
	cur  []any
	err  error
	errc chan error
}

func newRowSource(ctx context.Context, produce func(emit func([]any) error) error) *rowSource {
	rs := &rowSource{rows: make(chan []any, 1024), errc: make(chan error, 1)}
	go func() {
		defer close(rs.rows)
		rs.errc <- produce(func(row []any) error {
			select {
			case rs.rows <- row:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	return rs
}

func (r *rowSource) Next() bool {
	row, ok := <-r.rows
	if !ok {
		r.err = <-r.errc
		return false
	}
	r.cur = row
	return true
}

func (r *rowSource) Values() ([]any, error) { return r.cur, nil }
func (r *rowSource) Err() error             { return r.err }

// drain stops the producer after COPY failed early.
func (r *rowSource) drain() {
	for range r.rows {
	}
}

var contentCols = []string{"tags", "lon", "lat", "nodes", "members"}

// content columns of a version (nil unless the history keeps content).
func (p *Postgres) content(v *Version) []any {
	if !p.cfg.Full() || v.Action == ActionDelete {
		return []any{nil, nil, nil, nil, nil}
	}
	var tags, members any
	if v.Tags != nil {
		b, _ := json.Marshal(v.Tags)
		tags = string(b)
	} else {
		tags = "{}"
	}
	var lon, lat, nodes any
	if v.HasCoord {
		lon, lat = v.Lon, v.Lat
	}
	if v.Type == 'w' {
		nodes = v.Nodes
		if v.Nodes == nil {
			nodes = []int64{}
		}
	}
	if v.Type == 'r' {
		ms := v.Members
		if ms == nil {
			ms = []Member{}
		}
		b, _ := json.Marshal(ms)
		members = string(b)
	}
	return []any{tags, lon, lat, nodes, members}
}

func typeStr(b byte) string { return string(rune(b)) }

// LoadSnapshot: see Store.
func (p *Postgres) LoadSnapshot(ctx context.Context, pbf string, pos Position) (LoadStats, error) {
	var st LoadStats
	if !p.cfg.Elements {
		return st, errors.New("osmmeta: element metadata is not enabled")
	}
	staging := p.t(tStaging)
	if _, err := p.pool.Exec(ctx, `DROP TABLE IF EXISTS `+staging+`; CREATE UNLOGGED TABLE `+staging+` (
		osm_type "char", osm_id bigint, version integer, "timestamp" timestamptz, changeset bigint, `+contentDDL+`)`); err != nil {
		return st, fmt.Errorf("osmmeta: staging table: %w", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
		defer cancel()
		_, _ = p.pool.Exec(c, `DROP TABLE IF EXISTS `+staging)
	}()
	var check MetadataCheck
	keepNode := func(tagged bool) bool { return p.cfg.AllNodes || tagged }
	rs := newRowSource(ctx, func(emit func([]any) error) error {
		bbox, err := ReadSnapshot(ctx, pbf, p.cfg.Full(), keepNode, func(v *Version) error {
			st.Elements++
			check.Add(v)
			row := append([]any{typeStr(v.Type), v.ID, v.Version, v.Timestamp, nullID(v.Changeset)}, p.content(v)...)
			return emit(row)
		})
		st.BBox = bbox
		return err
	})
	cols := append([]string{"osm_type", "osm_id", "version", "timestamp", "changeset"}, contentCols...)
	_, err := p.pool.CopyFrom(ctx, pgx.Identifier{p.cfg.Schema, tStaging}, cols, rs)
	rs.drain()
	if err != nil {
		return st, fmt.Errorf("osmmeta: loading %s: %w", pbf, err)
	}
	if err := check.Err(p.cfg.Changesets); err != nil {
		return st, err
	}
	err = pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// The new element metadata is built next to the old one and swapped
		// in, with its indexes built after the rows (faster than inserting
		// into an indexed table).
		newT := p.t(tMetaNew)
		stmts := []string{`DROP TABLE IF EXISTS ` + newT, `CREATE TABLE ` + newT + metaColumns,
			`INSERT INTO ` + newT + ` (osm_type, osm_id, version, "timestamp", changeset, deleted, sequence)
			 SELECT osm_type, osm_id, version, "timestamp", changeset, false, 0 FROM ` + staging}
		stmts = append(stmts, p.metaIndexes(tMetaNew, "_new")...)
		if p.cfg.HasHistory() {
			// a history kept across a source change: its versions are
			// replaced by the new baselines
			var kept bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+p.t(tHistory)+`)`).Scan(&kept); err != nil {
				return err
			}
			stmts = append(stmts,
				// a history kept across a source change records the break
				`INSERT INTO `+p.t(tBreaks)+` (sequence_from, sequence_to, reason)
				 SELECT (SELECT sequence FROM `+p.t(tPosition)+` WHERE stream = 'elements'), `+fmt.Sprint(pos.Sequence)+`,
				 'snapshot loaded: new baseline (first load, source change or rebuild)'
				 WHERE EXISTS (SELECT 1 FROM `+p.t(tHistory)+`)`,
				`INSERT INTO `+p.t(tHistory)+` (osm_type, osm_id, version, "timestamp", changeset, action, sequence, tags, lon, lat, nodes, members)
				 SELECT osm_type, osm_id, version, "timestamp", changeset, 'baseline', 0, tags, lon, lat, nodes, members FROM `+staging+`
				 ON CONFLICT DO NOTHING`)
			if kept {
				stmts = append(stmts, p.supersededSQL(staging))
			}
		}
		// the views join element_meta: they are recreated on the new table
		// in this transaction, so readers never miss them
		views, _, err := p.listViews(ctx, tx)
		if err != nil {
			return err
		}
		for name := range views {
			stmts = append(stmts, `DROP VIEW `+p.t(name))
		}
		stmts = append(stmts, `DROP TABLE IF EXISTS `+p.t(tMeta), `ALTER TABLE `+newT+` RENAME TO `+quote(tMeta))
		sch := quote(p.cfg.Schema)
		stmts = append(stmts, `ALTER TABLE `+p.t(tMeta)+` RENAME CONSTRAINT `+quote("element_meta_pkey_new")+` TO `+quote("element_meta_pkey"))
		for _, ix := range metaIndexDefs {
			stmts = append(stmts, `ALTER INDEX `+sch+`.`+quote(ix[0]+"_new")+` RENAME TO `+quote(ix[0]))
		}
		for name, v := range views {
			stmts = append(stmts, `CREATE VIEW `+p.t(name)+` AS `+v.def, `COMMENT ON VIEW `+p.t(name)+` IS `+lit(v.mark))
		}
		if err := execAll(ctx, tx, stmts...); err != nil {
			return err
		}
		return p.setPos(ctx, tx, StreamElements, pos)
	})
	if err != nil {
		return st, err
	}
	if st.BBox != nil {
		_ = p.SetPosition(ctx, StreamBBox, Position{Source: st.BBox.String()})
	}
	p.analyze(ctx, tMeta, tHistory)
	return st, nil
}

func (p *Postgres) analyze(ctx context.Context, tables ...string) {
	for _, t := range tables {
		if p.has(t) {
			_, _ = p.pool.Exec(ctx, `ANALYZE `+p.t(t))
		}
	}
}

// has reports whether the config keeps table.
func (p *Postgres) has(table string) bool {
	switch table {
	case tHistory, tSuperseded:
		return p.cfg.HasHistory()
	case tMeta:
		return p.cfg.Elements
	case tChangesets:
		return p.cfg.Changesets
	}
	return true
}

func nullID(id int64) any {
	if id <= 0 {
		return nil
	}
	return id
}

// ApplyDiffs: see Store.
func (p *Postgres) ApplyDiffs(ctx context.Context, files []DiffFile, to Position) error {
	if !p.cfg.Elements {
		return errors.New("osmmeta: element metadata is not enabled")
	}
	if len(files) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		// a replay is harmless, a gap is not
		cur, ok, err := position(ctx, tx, p.t(tPosition), StreamElements)
		if err != nil {
			return err
		}
		if !ok || cur.Source != to.Source || files[0].Sequence > cur.Sequence+1 {
			return fmt.Errorf("osmmeta: diffs from %d of %q do not follow the element metadata at %d of %q",
				files[0].Sequence, to.Source, cur.Sequence, cur.Source)
		}
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE osmmeta_chunk (
			osm_type "char", osm_id bigint, version integer, "timestamp" timestamptz, changeset bigint,
			action text, tagged boolean, sequence bigint, `+contentDDL+`) ON COMMIT DROP`); err != nil {
			return err
		}
		var check MetadataCheck
		rs := newRowSource(ctx, func(emit func([]any) error) error {
			for _, f := range files {
				err := ReadChangeFile(f.Path, ReadOptions{Content: p.cfg.Full()}, func(v *Version) error {
					check.Add(v)
					return emit(append([]any{typeStr(v.Type), v.ID, v.Version, v.Timestamp, nullID(v.Changeset),
						v.Action, v.Tagged(), f.Sequence}, p.content(v)...))
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		cols := append([]string{"osm_type", "osm_id", "version", "timestamp", "changeset", "action", "tagged", "sequence"}, contentCols...)
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"osmmeta_chunk"}, cols, rs)
		rs.drain()
		if err != nil {
			return err
		}
		if err := check.Err(p.cfg.Changesets); err != nil {
			return err
		}
		// An untagged node is kept when an earlier version was (it has rows
		// already in table, or a tagged version in this chunk).
		keep := func(table string) string {
			return `(c.osm_type <> 'n' OR ` + fmt.Sprint(p.cfg.AllNodes) + ` OR q.any_tagged
				OR EXISTS (SELECT 1 FROM ` + p.t(table) + ` x WHERE x.osm_type = 'n' AND x.osm_id = c.osm_id))`
		}
		stmts := []string{`CREATE TEMP TABLE osmmeta_q ON COMMIT DROP AS
			SELECT osm_type, osm_id, bool_or(tagged) AS any_tagged FROM osmmeta_chunk GROUP BY 1, 2`,
			`INSERT INTO ` + p.t(tMeta) + ` (osm_type, osm_id, version, "timestamp", changeset, deleted, sequence)
			 SELECT DISTINCT ON (c.osm_type, c.osm_id) c.osm_type, c.osm_id, c.version, c."timestamp", c.changeset,
				c.action = 'delete', c.sequence
			 FROM osmmeta_chunk c JOIN osmmeta_q q USING (osm_type, osm_id)
			 WHERE ` + keep(tMeta) + `
			 ORDER BY c.osm_type, c.osm_id, c.version DESC
			 ON CONFLICT (osm_type, osm_id) DO UPDATE SET version = EXCLUDED.version, "timestamp" = EXCLUDED."timestamp",
				changeset = EXCLUDED.changeset, deleted = EXCLUDED.deleted, sequence = EXCLUDED.sequence
			 WHERE ` + p.t(tMeta) + `.version <= EXCLUDED.version`}
		if p.cfg.HasHistory() {
			stmts = append(stmts,
				`INSERT INTO `+p.t(tHistory)+` (osm_type, osm_id, version, "timestamp", changeset, action, sequence, tags, lon, lat, nodes, members)
				 SELECT c.osm_type, c.osm_id, c.version, c."timestamp", c.changeset, c.action, c.sequence, c.tags, c.lon, c.lat, c.nodes, c.members
				 FROM osmmeta_chunk c JOIN osmmeta_q q USING (osm_type, osm_id)
				 WHERE `+keep(tHistory)+`
				 ON CONFLICT DO NOTHING`,
				p.supersededSQL("osmmeta_q"))
		}
		if err := execAll(ctx, tx, stmts...); err != nil {
			return err
		}
		return p.setPos(ctx, tx, StreamElements, to)
	})
}

// ApplyChangesets: see Store.
func (p *Postgres) ApplyChangesets(ctx context.Context, cs []Changeset, to Position) error {
	return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
		if len(cs) > 0 {
			if _, err := tx.Exec(ctx, `CREATE TEMP TABLE osmmeta_cs (id bigint, created_at timestamptz, closed_at timestamptz,
				open boolean, num_changes integer, comment text, tags jsonb,
				min_lon float8, min_lat float8, max_lon float8, max_lat float8) ON COMMIT DROP`); err != nil {
				return err
			}
			rows := make([][]any, 0, len(cs))
			for _, c := range cs {
				tags, _ := json.Marshal(c.Tags)
				var closed, comment any
				if c.ClosedAt != nil {
					closed = *c.ClosedAt
				}
				if v, ok := c.Tags["comment"]; ok {
					comment = v
				}
				row := []any{c.ID, c.CreatedAt, closed, c.Open, c.NumChanges, comment, string(tags), nil, nil, nil, nil}
				if b := c.BBox; b != nil {
					row[7], row[8], row[9], row[10] = b.MinLon, b.MinLat, b.MaxLon, b.MaxLat
				}
				rows = append(rows, row)
			}
			if _, err := tx.CopyFrom(ctx, pgx.Identifier{"osmmeta_cs"},
				[]string{"id", "created_at", "closed_at", "open", "num_changes", "comment", "tags", "min_lon", "min_lat", "max_lon", "max_lat"},
				pgx.CopyFromRows(rows)); err != nil {
				return err
			}
			// a changeset appears again when it closes: the newest state wins
			if _, err := tx.Exec(ctx, `INSERT INTO `+p.t(tChangesets)+` (id, created_at, closed_at, open, num_changes, comment, tags, bbox)
				SELECT DISTINCT ON (id) id, created_at, closed_at, open, num_changes, comment, tags,
					CASE WHEN min_lon IS NOT NULL THEN ST_SetSRID(ST_Expand(ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat), 0.0000001), 4326) END
				FROM osmmeta_cs ORDER BY id, open, closed_at DESC NULLS LAST
				ON CONFLICT (id) DO UPDATE SET closed_at = EXCLUDED.closed_at, open = EXCLUDED.open,
					num_changes = EXCLUDED.num_changes, comment = EXCLUDED.comment, tags = EXCLUDED.tags, bbox = EXCLUDED.bbox
				WHERE `+p.t(tChangesets)+`.open OR `+p.t(tChangesets)+`.closed_at IS NULL`); err != nil {
				return err
			}
		}
		return p.setPos(ctx, tx, StreamChangesets, to)
	})
}

// pruneBatch is the number of rows one retention statement handles.
const pruneBatch = 50000

// Prune: see Store. Each rule runs in batches until it has nothing left or
// the budget is used; every batch is logged in the retention table.
func (p *Postgres) Prune(ctx context.Context, now time.Time, budget time.Duration) (PruneStats, error) {
	var st PruneStats
	deadline := time.Now().Add(budget)
	r := p.cfg.Retention
	type rule struct {
		name   string
		on     bool
		cutoff time.Time
		clear  bool
		sql    string
	}
	var rules []rule
	if p.cfg.HasHistory() {
		rules = append(rules,
			rule{name: "maxAge", on: r.MaxAge > 0, cutoff: now.Add(-r.MaxAge), sql: `WITH s AS (
				DELETE FROM ` + p.t(tSuperseded) + ` WHERE ctid IN (SELECT ctid FROM ` + p.t(tSuperseded) + `
					WHERE superseded_at < $1 LIMIT ` + fmt.Sprint(pruneBatch) + `)
				RETURNING osm_type, osm_id, version)
			DELETE FROM ` + p.t(tHistory) + ` h USING s WHERE h.osm_type = s.osm_type AND h.osm_id = s.osm_id AND h.version = s.version`},
			rule{name: "maxVersions", on: r.MaxVersions > 0, sql: `WITH x AS (
				SELECT osm_type, osm_id, version FROM (
					SELECT h.osm_type, h.osm_id, h.version,
						row_number() OVER (PARTITION BY h.osm_type, h.osm_id ORDER BY h.version DESC) AS rn
					FROM ` + p.t(tHistory) + ` h
					WHERE (h.osm_type, h.osm_id) IN (SELECT osm_type, osm_id FROM ` + p.t(tSuperseded) + `
						GROUP BY 1, 2 HAVING count(*) >= ` + fmt.Sprint(r.MaxVersions) + ` LIMIT ` + fmt.Sprint(pruneBatch/10) + `)) y
				WHERE rn > ` + fmt.Sprint(r.MaxVersions) + `),
			s AS (DELETE FROM ` + p.t(tSuperseded) + ` u USING x WHERE u.osm_type = x.osm_type AND u.osm_id = x.osm_id AND u.version = x.version)
			DELETE FROM ` + p.t(tHistory) + ` h USING x WHERE h.osm_type = x.osm_type AND h.osm_id = x.osm_id AND h.version = x.version`},
			rule{name: "content", on: p.cfg.Full() && r.Content > 0, cutoff: now.Add(-r.Content), clear: true},
		)
	}
	if p.cfg.Elements {
		del := `WITH e AS (SELECT osm_type, osm_id FROM ` + p.t(tMeta) + ` WHERE deleted AND "timestamp" < $1 LIMIT ` + fmt.Sprint(pruneBatch/10) + `)`
		if p.cfg.HasHistory() {
			del += `, s AS (DELETE FROM ` + p.t(tSuperseded) + ` u USING e WHERE u.osm_type = e.osm_type AND u.osm_id = e.osm_id),
			h AS (DELETE FROM ` + p.t(tHistory) + ` x USING e WHERE x.osm_type = e.osm_type AND x.osm_id = e.osm_id)`
		}
		del += ` DELETE FROM ` + p.t(tMeta) + ` m USING e WHERE m.osm_type = e.osm_type AND m.osm_id = e.osm_id`
		rules = append(rules, rule{name: "deleted", on: r.Deleted > 0, cutoff: now.Add(-r.Deleted), sql: del})
	}
	if p.cfg.Changesets {
		var refs []string
		if p.cfg.Elements {
			refs = append(refs, `NOT EXISTS (SELECT 1 FROM `+p.t(tMeta)+` m WHERE m.changeset = c.id)`)
		}
		if p.cfg.HasHistory() {
			refs = append(refs, `NOT EXISTS (SELECT 1 FROM `+p.t(tHistory)+` h WHERE h.changeset = c.id)`)
		}
		where := `c.closed_at < $1`
		if len(refs) > 0 {
			where += ` AND ` + strings.Join(refs, " AND ")
		}
		rules = append(rules, rule{name: "changesets", on: p.cfg.ChangesetRetention > 0, cutoff: now.Add(-p.cfg.ChangesetRetention),
			sql: `DELETE FROM ` + p.t(tChangesets) + ` WHERE ctid IN (SELECT c.ctid FROM ` + p.t(tChangesets) + ` c WHERE ` + where +
				` LIMIT ` + fmt.Sprint(pruneBatch) + `)`})
	}
	for _, ru := range rules {
		if !ru.on {
			continue
		}
		var n int64
		var err error
		if ru.clear {
			n, err = p.clearContent(ctx, ru.cutoff, deadline)
			st.Cleared += n
		} else {
			n, err = p.pruneLoop(ctx, ru.sql, ru.cutoff, deadline)
			st.Deleted += n
		}
		if n > 0 && p.cfg.HasHistory() {
			d, c := n, int64(0)
			if ru.clear {
				d, c = 0, n
			}
			_, _ = p.pool.Exec(ctx, `INSERT INTO `+p.t(tRetention)+` (rule, cutoff, rows_deleted, rows_cleared) VALUES ($1, $2, $3, $4)`,
				ru.name, tsArg(ru.cutoff), d, c)
		}
		if err != nil {
			return st, fmt.Errorf("osmmeta: retention %s: %w", ru.name, err)
		}
		if time.Now().After(deadline) {
			return st, nil
		}
	}
	st.Done = true
	if st.Deleted+st.Cleared > 0 {
		for _, t := range []string{tHistory, tSuperseded, tMeta, tChangesets} {
			if p.has(t) {
				_, _ = p.pool.Exec(ctx, `VACUUM (ANALYZE) `+p.t(t))
			}
		}
	}
	return st, nil
}

func (p *Postgres) pruneLoop(ctx context.Context, sql string, cutoff time.Time, deadline time.Time) (int64, error) {
	var total int64
	args := []any{}
	if strings.Contains(sql, "$1") {
		args = append(args, cutoff)
	}
	for time.Now().Before(deadline) {
		tag, err := p.pool.Exec(ctx, sql, args...)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() == 0 {
			return total, nil
		}
	}
	return total, nil
}

// clearContent clears the content of versions replaced before cutoff,
// continuing from where the last pass stopped (StreamContent).
func (p *Postgres) clearContent(ctx context.Context, cutoff, deadline time.Time) (int64, error) {
	from, _, err := p.Position(ctx, StreamContent)
	if err != nil {
		return 0, err
	}
	var total int64
	for time.Now().Before(deadline) {
		var upTo *time.Time
		var n int64
		err := pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			// the next batch of replaced versions, by replacement time
			if err := tx.QueryRow(ctx, `SELECT max(superseded_at) FROM (SELECT superseded_at FROM `+p.t(tSuperseded)+`
				WHERE superseded_at >= $1 AND superseded_at < $2 ORDER BY superseded_at LIMIT `+fmt.Sprint(pruneBatch)+`) x`,
				tsOrEpoch(from.Timestamp), cutoff).Scan(&upTo); err != nil {
				return err
			}
			if upTo == nil {
				return nil
			}
			tag, err := tx.Exec(ctx, `UPDATE `+p.t(tHistory)+` h SET `+contentClear+`
				FROM `+p.t(tSuperseded)+` s
				WHERE s.superseded_at >= $1 AND s.superseded_at <= $2
					AND h.osm_type = s.osm_type AND h.osm_id = s.osm_id AND h.version = s.version
					AND (h.tags IS NOT NULL OR h.lon IS NOT NULL OR h.nodes IS NOT NULL OR h.members IS NOT NULL)`,
				tsOrEpoch(from.Timestamp), *upTo)
			if err != nil {
				return err
			}
			n = tag.RowsAffected()
			return p.setPos(ctx, tx, StreamContent, Position{Timestamp: upTo.Add(time.Microsecond)})
		})
		if err != nil {
			return total, err
		}
		if upTo == nil {
			return total, nil
		}
		total += n
		from.Timestamp = upTo.Add(time.Microsecond)
	}
	return total, nil
}

func tsOrEpoch(t time.Time) time.Time {
	if t.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return t
}

// CountMissing: see Store.
func (p *Postgres) CountMissing(ctx context.Context, since, until time.Time) (int64, error) {
	if !p.cfg.Elements || !p.cfg.Changesets {
		return 0, nil
	}
	var n int64
	err := p.pool.QueryRow(ctx, `SELECT count(DISTINCT m.changeset) FROM `+p.t(tMeta)+` m
		WHERE m.sequence > 0 AND m."timestamp" >= $1 AND m."timestamp" < $2 AND m.changeset IS NOT NULL
			AND NOT EXISTS (SELECT 1 FROM `+p.t(tChangesets)+` c WHERE c.id = m.changeset)`, since, until).Scan(&n)
	return n, err
}

// Stats: see Store. Row counts are the planner's estimates (exact after
// ANALYZE), so they stay cheap for large tables.
func (p *Postgres) Stats(ctx context.Context) (Stats, error) {
	st := Stats{Positions: map[string]Position{}}
	rows, err := p.pool.Query(ctx, `SELECT stream, sequence, "timestamp", source FROM `+p.t(tPosition))
	if err != nil {
		return st, err
	}
	for rows.Next() {
		var s string
		var pos Position
		var ts *time.Time
		if err := rows.Scan(&s, &pos.Sequence, &ts, &pos.Source); err != nil {
			rows.Close()
			return st, err
		}
		if ts != nil {
			pos.Timestamp = ts.UTC()
		}
		st.Positions[s] = pos
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return st, err
	}
	est := func(table string) (int64, error) {
		var n *float64
		if err := p.pool.QueryRow(ctx, `SELECT reltuples::float8 FROM pg_class WHERE oid = to_regclass($1)`,
			p.cfg.Schema+"."+quote(table)).Scan(&n); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, nil
			}
			return 0, err
		}
		if n == nil || *n < 0 { // never analyzed
			var c int64
			err := p.pool.QueryRow(ctx, `SELECT count(*) FROM `+p.t(table)).Scan(&c)
			return c, err
		}
		return int64(*n), nil
	}
	if p.cfg.Elements {
		if st.Elements, err = est(tMeta); err != nil {
			return st, err
		}
	}
	if p.cfg.HasHistory() {
		if st.HistoryRows, err = est(tHistory); err != nil {
			return st, err
		}
		var oldest *time.Time
		if err := p.pool.QueryRow(ctx, `SELECT min("timestamp") FROM `+p.t(tHistory)+` WHERE action <> 'baseline'`).Scan(&oldest); err != nil {
			return st, err
		}
		if oldest != nil {
			st.HistoryOldest = oldest.UTC()
		}
	}
	if p.cfg.Changesets {
		if st.Changesets, err = est(tChangesets); err != nil {
			return st, err
		}
	}
	if m, ok := st.Positions[StreamMissing]; ok {
		st.ChangesetsMissing = m.Sequence
	}
	return st, nil
}
