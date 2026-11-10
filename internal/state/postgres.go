package state

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"k8s.io/apimachinery/pkg/util/wait"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Postgres is the production Store.
type Postgres struct {
	pool    *pgxpool.Pool
	connStr string
	schema  string
}

// OpenPostgres connects; schema is the bookkeeping schema (validated).
func OpenPostgres(ctx context.Context, connString, schema string) (*Postgres, error) {
	if !identRe.MatchString(schema) {
		return nil, fmt.Errorf("state: invalid schema name %q", schema)
	}
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.ConnectTimeout = 15 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("state: %w", err)
	}
	if err := retry(ctx, func() error { return pool.Ping(ctx) }); err != nil {
		pool.Close()
		return nil, fmt.Errorf("state: connect: %w", err)
	}
	return &Postgres{pool: pool, connStr: connString, schema: schema}, nil
}

// Close closes the pool.
func (p *Postgres) Close() { p.pool.Close() }

// Pool exposes the pool (for tests and checks).
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

func (p *Postgres) q(sql string) string { return strings.ReplaceAll(sql, "{{schema}}", p.schema) }

// retry retries transient connection errors a few times. Statement errors
// (constraint violations etc.) are returned immediately.
func retry(ctx context.Context, fn func() error) error {
	var err error
	b := wait.Backoff{Duration: 200 * time.Millisecond, Factor: 2, Jitter: 0.2, Steps: 6}
	for b.Steps > 0 {
		if err = fn(); err == nil || !transient(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(b.Step()):
		}
	}
	return err
}

func transient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 08: connection exception, 57P01-03: admin shutdown / crash / cannot connect now,
		// 40001/40P01: serialization failure / deadlock.
		return strings.HasPrefix(pgErr.Code, "08") || strings.HasPrefix(pgErr.Code, "57P") ||
			pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return pgconn.SafeToRetry(err) || isConnErr(err)
}

func isConnErr(err error) bool {
	s := err.Error()
	for _, m := range []string{"connection refused", "connection reset", "broken pipe", "unexpected EOF", "conn closed", "failed to connect", "terminating connection"} {
		if strings.Contains(s, m) {
			return true
		}
	}
	return false
}

// Migrate creates the schema and applies pending migrations under a
// transaction-scoped advisory lock, so concurrent runners do not race.
func (p *Postgres) Migrate(ctx context.Context) error {
	names, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	return retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey("migrate:"+p.schema)); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, p.q(`CREATE SCHEMA IF NOT EXISTS {{schema}};
				CREATE TABLE IF NOT EXISTS {{schema}}.schema_migrations (
					version int PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`)); err != nil {
				return err
			}
			for _, name := range names {
				v, err := strconv.Atoi(strings.SplitN(strings.TrimPrefix(name, "migrations/"), "_", 2)[0])
				if err != nil {
					return fmt.Errorf("migration %s: bad name", name)
				}
				var done bool
				if err := tx.QueryRow(ctx, p.q("SELECT EXISTS (SELECT 1 FROM {{schema}}.schema_migrations WHERE version=$1)"), v).Scan(&done); err != nil {
					return err
				}
				if done {
					continue
				}
				body, _ := migrationFS.ReadFile(name)
				if _, err := tx.Exec(ctx, p.q(string(body))); err != nil {
					return fmt.Errorf("migration %s: %w", name, err)
				}
				if _, err := tx.Exec(ctx, p.q("INSERT INTO {{schema}}.schema_migrations(version) VALUES ($1)"), v); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func lockKey(s string) int64 {
	h := fnv.New64a()
	h.Write([]byte("imposm-operator:" + s))
	return int64(h.Sum64())
}

type pgLock struct {
	conn *pgx.Conn
	key  int64
}

// TryLock takes a session advisory lock on a dedicated connection. If that
// connection dies the lock is gone; Check reports it so the engine stops
// before its next step.
func (p *Postgres) TryLock(ctx context.Context, name string) (Lock, error) {
	var conn *pgx.Conn
	err := retry(ctx, func() error {
		var err error
		conn, err = pgx.Connect(ctx, p.connStr)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("state: lock connection: %w", err)
	}
	key := lockKey("target:" + p.schema + ":" + name)
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&ok); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("state: lock: %w", err)
	}
	if !ok {
		conn.Close(ctx)
		return nil, nil
	}
	return &pgLock{conn: conn, key: key}, nil
}

func (l *pgLock) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// Verify we still hold it, not merely that a connection exists.
	var held bool
	err := l.conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory'
		AND pid=pg_backend_pid() AND granted AND ((classid::bigint<<32)|objid::bigint)=$1)`, l.key).Scan(&held)
	if err != nil {
		return fmt.Errorf("state: lock connection lost: %w", err)
	}
	if !held {
		return errors.New("state: advisory lock no longer held")
	}
	return nil
}

func (l *pgLock) Release() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", l.key)
	l.conn.Close(ctx)
}

// EnsurePostGIS creates the postgis extension if it is missing. Only when it
// is missing do we need the privilege to create it.
func (p *Postgres) EnsurePostGIS(ctx context.Context) error {
	return retry(ctx, func() error {
		var have bool
		if err := p.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='postgis')").Scan(&have); err != nil {
			return err
		}
		if have {
			return nil
		}
		if _, err := p.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS postgis"); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42501" {
				return fmt.Errorf("the postgis extension is missing and this user may not create it; ask a database admin to run CREATE EXTENSION postgis: %w", err)
			}
			return err
		}
		return nil
	})
}

func (p *Postgres) EnsureTarget(ctx context.Context, name string) (int64, error) {
	var id int64
	err := retry(ctx, func() error {
		return p.pool.QueryRow(ctx, p.q(`INSERT INTO {{schema}}.target(name) VALUES ($1)
			ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id`), name).Scan(&id)
	})
	return id, err
}

const importCols = `id, target_id, pbf_url, replication_url, config_hash, sequence, timestamp, pbf_md5,
	phase, last_error, created_at, updated_at, deployed_at, backup_kept, reason`

func scanImport(row pgx.Row) (*Import, error) {
	var (
		imp Import
		ts  *time.Time
		seq *int64
	)
	err := row.Scan(&imp.ID, &imp.TargetID, &imp.PBFURL, &imp.ReplicationURL, &imp.ConfigHash, &seq, &ts,
		&imp.PBFMD5, &imp.Phase, &imp.LastError, &imp.CreatedAt, &imp.UpdatedAt, &imp.DeployedAt, &imp.BackupKept, &imp.Reason)
	if err != nil {
		return nil, err
	}
	if seq != nil {
		imp.Sequence = *seq
	}
	if ts != nil {
		imp.Timestamp = ts.UTC()
	}
	return &imp, nil
}

func (p *Postgres) CurrentImport(ctx context.Context, targetID int64) (*Import, error) {
	var imp *Import
	err := retry(ctx, func() error {
		var err error
		imp, err = scanImport(p.pool.QueryRow(ctx, p.q(`SELECT `+importCols+` FROM {{schema}}.import
			WHERE target_id=$1 AND phase NOT IN ('superseded') ORDER BY id DESC LIMIT 1`), targetID))
		if errors.Is(err, pgx.ErrNoRows) {
			imp, err = nil, nil
		}
		return err
	})
	return imp, err
}

func (p *Postgres) DeployedImport(ctx context.Context, targetID int64) (*Import, error) {
	var imp *Import
	err := retry(ctx, func() error {
		var err error
		imp, err = scanImport(p.pool.QueryRow(ctx, p.q(`SELECT `+importCols+` FROM {{schema}}.import
			WHERE target_id=$1 AND phase='deployed'`), targetID))
		if errors.Is(err, pgx.ErrNoRows) {
			imp, err = nil, nil
		}
		return err
	})
	return imp, err
}

// CreateImport inserts imp and marks older imports of the target that were
// never deployed (or are no longer usable) as superseded. The deployed import
// stays "deployed" until the new one is deployed.
func (p *Postgres) CreateImport(ctx context.Context, imp *Import) error {
	return retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, p.q(`UPDATE {{schema}}.import SET phase='superseded', updated_at=now()
				WHERE target_id=$1 AND phase NOT IN ('deployed','reverted','superseded')`), imp.TargetID); err != nil {
				return err
			}
			if imp.Phase == "" {
				imp.Phase = PhasePending
			}
			return tx.QueryRow(ctx, p.q(`INSERT INTO {{schema}}.import
				(target_id, pbf_url, replication_url, config_hash, phase, reason)
				VALUES ($1,$2,$3,$4,$5,$6) RETURNING id, created_at, updated_at`),
				imp.TargetID, imp.PBFURL, imp.ReplicationURL, imp.ConfigHash, imp.Phase, imp.Reason).
				Scan(&imp.ID, &imp.CreatedAt, &imp.UpdatedAt)
		})
	})
}

// UpdateImport stores phase, snapshot info and error. Deploying an import
// supersedes the previously deployed one in the same transaction.
func (p *Postgres) UpdateImport(ctx context.Context, imp *Import) error {
	var seq *int64
	var ts *time.Time
	if imp.Sequence > 0 {
		seq = &imp.Sequence
	}
	if !imp.Timestamp.IsZero() {
		ts = &imp.Timestamp
	}
	return retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			if imp.Phase == PhaseDeployed {
				if _, err := tx.Exec(ctx, p.q(`UPDATE {{schema}}.import SET phase='superseded', updated_at=now()
					WHERE target_id=$1 AND id<>$2 AND phase IN ('deployed','reverted')`), imp.TargetID, imp.ID); err != nil {
					return err
				}
			}
			return tx.QueryRow(ctx, p.q(`UPDATE {{schema}}.import SET sequence=$2, timestamp=$3, pbf_md5=$4, phase=$5,
				last_error=$6, backup_kept=$7, config_hash=$8, updated_at=now(),
				deployed_at=CASE WHEN $5='deployed' AND deployed_at IS NULL THEN now() ELSE deployed_at END
				WHERE id=$1 RETURNING updated_at, deployed_at`),
				imp.ID, seq, ts, imp.PBFMD5, imp.Phase, imp.LastError, imp.BackupKept, imp.ConfigHash).Scan(&imp.UpdatedAt, &imp.DeployedAt)
		})
	})
}

func (p *Postgres) Applied(ctx context.Context, imp *Import) (Position, error) {
	pos := Position{Sequence: imp.Sequence, Timestamp: imp.Timestamp}
	err := retry(ctx, func() error {
		var (
			to *int64
			ts *time.Time
		)
		err := p.pool.QueryRow(ctx, p.q(`SELECT to_seq, to_timestamp FROM {{schema}}.diff_batch
			WHERE import_id=$1 AND status='applied' ORDER BY to_seq DESC LIMIT 1`), imp.ID).Scan(&to, &ts)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		pos.Sequence = *to
		if ts != nil {
			pos.Timestamp = ts.UTC()
		}
		return nil
	})
	return pos, err
}

func (p *Postgres) BeginBatch(ctx context.Context, b *Batch) error {
	if b.Status == "" {
		b.Status = BatchRunning
	}
	return p.writeBatch(ctx, b, true)
}

func (p *Postgres) FinishBatch(ctx context.Context, b *Batch) error {
	return p.writeBatch(ctx, b, false)
}

func (p *Postgres) writeBatch(ctx context.Context, b *Batch, insert bool) error {
	var ts *time.Time
	if !b.ToTimestamp.IsZero() {
		ts = &b.ToTimestamp
	}
	err := retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			if b.Status == BatchApplied {
				// Refuse overlaps with applied batches (the partial unique
				// index only catches identical starts).
				var overlap bool
				if err := tx.QueryRow(ctx, p.q(`SELECT EXISTS (SELECT 1 FROM {{schema}}.diff_batch
					WHERE import_id=$1 AND status='applied' AND id<>$2 AND from_seq<=$4 AND to_seq>=$3)`),
					b.ImportID, b.ID, b.From, b.To).Scan(&overlap); err != nil {
					return err
				}
				if overlap {
					return ErrConflict
				}
			}
			if insert {
				return tx.QueryRow(ctx, p.q(`INSERT INTO {{schema}}.diff_batch
					(import_id, from_seq, to_seq, to_timestamp, target_schema, status, error, note, finished_at)
					VALUES ($1,$2,$3,$4,$5,$6,$7,$8, CASE WHEN $6='running' THEN NULL ELSE now() END)
					RETURNING id, started_at, finished_at`),
					b.ImportID, b.From, b.To, ts, b.TargetSchema, b.Status, b.Error, b.Note).
					Scan(&b.ID, &b.StartedAt, &b.FinishedAt)
			}
			return tx.QueryRow(ctx, p.q(`UPDATE {{schema}}.diff_batch SET to_seq=$2, to_timestamp=$3, status=$4,
				error=$5, note=$6, finished_at=now() WHERE id=$1 RETURNING finished_at`),
				b.ID, b.To, ts, b.Status, b.Error, b.Note).Scan(&b.FinishedAt)
		})
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrConflict
	}
	return err
}

func (p *Postgres) Batches(ctx context.Context, importID int64) ([]Batch, error) {
	var out []Batch
	err := retry(ctx, func() error {
		out = nil
		rows, err := p.pool.Query(ctx, p.q(`SELECT id, import_id, from_seq, to_seq, to_timestamp, target_schema,
			status, error, note, started_at, finished_at FROM {{schema}}.diff_batch
			WHERE import_id=$1 ORDER BY from_seq, id`), importID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var (
				b  Batch
				ts *time.Time
			)
			if err := rows.Scan(&b.ID, &b.ImportID, &b.From, &b.To, &ts, &b.TargetSchema, &b.Status,
				&b.Error, &b.Note, &b.StartedAt, &b.FinishedAt); err != nil {
				return err
			}
			if ts != nil {
				b.ToTimestamp = ts.UTC()
			}
			out = append(out, b)
		}
		return rows.Err()
	})
	return out, err
}

// Imports lists all imports of a target (newest first).
func (p *Postgres) Imports(ctx context.Context, targetID int64) ([]Import, error) {
	rows, err := p.pool.Query(ctx, p.q(`SELECT `+importCols+` FROM {{schema}}.import WHERE target_id=$1 ORDER BY id DESC`), targetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Import
	for rows.Next() {
		imp, err := scanImport(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *imp)
	}
	return out, rows.Err()
}

// AnyOSMID reports whether any of ids appears in the osm_id column of
// schema.table (used to tell whether an interrupted diff chunk committed).
func (p *Postgres) AnyOSMID(ctx context.Context, schema, table string, ids []int64) (bool, error) {
	if len(ids) == 0 {
		return false, nil
	}
	var found bool
	err := retry(ctx, func() error {
		var exists bool
		if err := p.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name='osm_id')`, schema, table).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			found = false
			return nil
		}
		return p.pool.QueryRow(ctx, fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s.%s WHERE osm_id = ANY($1))`,
			pgx.Identifier{schema}.Sanitize(), pgx.Identifier{table}.Sanitize()), ids).Scan(&found)
	})
	return found, err
}

// ClaimSchema holds the table claim registry. It is fixed (independent of
// the configured state schema) so every sync in the database sees it.
const ClaimSchema = "imposm_operator"

func (p *Postgres) Claim(ctx context.Context, owner string, tables []string, keep bool) error {
	sort.Strings(tables)
	return retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey("claims")); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+ClaimSchema+`;
				CREATE TABLE IF NOT EXISTS `+ClaimSchema+`.table_claim (
					owner      text PRIMARY KEY,
					tables     text[] NOT NULL,
					updated_at timestamptz NOT NULL DEFAULT now())`); err != nil {
				return err
			}
			var (
				other    string
				overlap  []string
				lastSeen time.Time
			)
			err := tx.QueryRow(ctx, `SELECT owner, ARRAY(SELECT unnest(tables) INTERSECT SELECT unnest($2::text[]) ORDER BY 1 LIMIT 10), updated_at
				FROM `+ClaimSchema+`.table_claim WHERE owner <> $1 AND tables && $2::text[] ORDER BY owner LIMIT 1`,
				owner, tables).Scan(&other, &overlap, &lastSeen)
			switch {
			case err == nil:
				return &ClaimConflict{Owner: other, Tables: overlap, LastSeen: lastSeen}
			case !errors.Is(err, pgx.ErrNoRows):
				return err
			}
			merge := "$2::text[]"
			if keep {
				merge = "ARRAY(SELECT DISTINCT unnest(" + ClaimSchema + ".table_claim.tables || $2::text[]) ORDER BY 1)"
			}
			_, err = tx.Exec(ctx, `INSERT INTO `+ClaimSchema+`.table_claim (owner, tables) VALUES ($1, $2)
				ON CONFLICT (owner) DO UPDATE SET tables = `+merge+`, updated_at = now()`, owner, tables)
			return err
		})
	})
}

func (p *Postgres) Claimed(ctx context.Context, owner string) ([]string, error) {
	var out []string
	err := retry(ctx, func() error {
		var exists bool
		if err := p.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, ClaimSchema+".table_claim").Scan(&exists); err != nil || !exists {
			out = nil
			return err
		}
		err := p.pool.QueryRow(ctx, `SELECT tables FROM `+ClaimSchema+`.table_claim WHERE owner = $1`, owner).Scan(&out)
		if errors.Is(err, pgx.ErrNoRows) {
			out, err = nil, nil
		}
		return err
	})
	return out, err
}

func qualifiedIdent(qualified string) string {
	schema, table := SplitTable(qualified)
	return pgx.Identifier{schema, table}.Sanitize()
}

func (p *Postgres) DropTables(ctx context.Context, tables []string) (map[string]string, error) {
	kept := map[string]string{}
	for _, t := range tables {
		err := retry(ctx, func() error {
			_, err := p.pool.Exec(ctx, "DROP TABLE IF EXISTS "+qualifiedIdent(t))
			return err
		})
		var pe *pgconn.PgError
		switch {
		case errors.As(err, &pe) && pe.Code == "2BP01": // dependent_objects_still_exist
			kept[t] = pe.Message
		case err != nil:
			return kept, fmt.Errorf("drop %s: %w", t, err)
		}
	}
	return kept, nil
}

func (p *Postgres) MoveTables(ctx context.Context, moves []TableMove) error {
	return retry(ctx, func() error {
		return pgx.BeginFunc(ctx, p.pool, func(tx pgx.Tx) error {
			exists := func(t string) (bool, error) {
				var ok bool
				err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, qualifiedIdent(t)).Scan(&ok)
				return ok, err
			}
			// Only tables that exist move. A target may be another moved
			// table's old name (a chain or a swap): every table is first
			// renamed to a temporary name, then to its target.
			var todo []TableMove
			moving := map[string]bool{}
			for _, m := range moves {
				ok, err := exists(m.From)
				if err != nil {
					return err
				}
				if ok && m.From != m.To {
					todo = append(todo, m)
					moving[m.From] = true
				}
			}
			for _, m := range todo {
				ok, err := exists(m.To)
				if err != nil {
					return err
				}
				if ok && !moving[m.To] {
					return &TableExists{Table: m.To}
				}
			}
			for i, m := range todo {
				schema, table := SplitTable(m.From)
				if err := renameTable(ctx, tx, schema, table, tempTableName(i)); err != nil {
					return err
				}
			}
			created := map[string]bool{}
			for i, m := range todo {
				fromSchema, _ := SplitTable(m.From)
				schema, table := SplitTable(m.To)
				tmp := tempTableName(i)
				if schema != fromSchema {
					if !created[schema] {
						if _, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
							return fmt.Errorf("create schema %s: %w", schema, err)
						}
						created[schema] = true
					}
					// indexes, constraints and owned sequences move along
					if _, err := tx.Exec(ctx, "ALTER TABLE "+pgx.Identifier{fromSchema, tmp}.Sanitize()+" SET SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
						return fmt.Errorf("move %s to %s: %w", m.From, schema, err)
					}
				}
				if err := renameTable(ctx, tx, schema, tmp, table); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// tempTableName is the i-th table's name between the two steps of
// MoveTables (short, so derived index names stay below 63 bytes).
func tempTableName(i int) string { return fmt.Sprintf("imposm_operator_mv%d", i) }

// renameTable renames schema.from to schema.to, with the indexes (and so
// the constraints they back) and the owned sequences named after it, so a
// later table of the old name can be created or deployed next to it.
func renameTable(ctx context.Context, tx pgx.Tx, schema, from, to string) error {
	if _, err := tx.Exec(ctx, "ALTER TABLE "+pgx.Identifier{schema, from}.Sanitize()+" RENAME TO "+pgx.Identifier{to}.Sanitize()); err != nil {
		return fmt.Errorf("rename %s.%s to %s: %w", schema, from, to, err)
	}
	rows, err := tx.Query(ctx, `
		SELECT c.relname, CASE c.relkind WHEN 'S' THEN 'SEQUENCE' ELSE 'INDEX' END
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND left(c.relname, length($2)) = $2 AND (
			c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid = to_regclass($3)) OR
			c.oid IN (SELECT objid FROM pg_depend WHERE refobjid = to_regclass($3) AND deptype IN ('a', 'i') AND classid = 'pg_class'::regclass))`,
		schema, from, pgx.Identifier{schema, to}.Sanitize())
	if err != nil {
		return err
	}
	type rel struct{ name, kind string }
	var rels []rel
	for rows.Next() {
		var r rel
		if err := rows.Scan(&r.name, &r.kind); err != nil {
			rows.Close()
			return err
		}
		rels = append(rels, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, r := range rels {
		name := to + strings.TrimPrefix(r.name, from)
		if len(name) > 63 {
			continue // Postgres would truncate; keep the old name
		}
		if _, err := tx.Exec(ctx, "ALTER "+r.kind+" "+pgx.Identifier{schema, r.name}.Sanitize()+" RENAME TO "+pgx.Identifier{name}.Sanitize()); err != nil {
			return fmt.Errorf("rename %s %s.%s: %w", strings.ToLower(r.kind), schema, r.name, err)
		}
	}
	return nil
}
