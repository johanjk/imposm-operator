// Command tablesums checks the osm_* tables of an imposm schema:
//
//	tablesums -conn C                        per-table checksums as JSON
//	tablesums -conn C -dump rows.json.gz     write a row dump (reference)
//	tablesums -conn C -compare-rows rows.json.gz   compare with a row dump
//	tablesums -conn C -tolerant-with C2      compare two databases
//
// Row comparisons require equal row counts and attributes; geometries may
// differ by -tolerance (imposm rounds coordinates differently depending on
// how diffs were grouped) and float columns by 0.1%. Exit 1 on difference.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/johanjk/imposm-operator/internal/mapping"
	"github.com/johanjk/imposm-operator/internal/testutil"
)

func main() {
	conn := flag.String("conn", "", "libpq connection string (PG* env and PGPASSFILE are honoured)")
	schema := flag.String("schema", "public", "schema with the imposm tables")
	prefix := flag.String("prefix", "osm_", "table prefix")
	compare := flag.String("compare", "", "reference.json to compare against")
	head := flag.Int64("head", 0, "head sequence to record in the output")
	tolerantWith := flag.String("tolerant-with", "", "second connection string: compare both databases row by row with a geometry tolerance")
	tol := flag.Float64("tolerance", 0.05, "geometry tolerance, in SRID units (metres for 3857)")
	dump := flag.String("dump", "", "write a gzipped row dump to this file")
	compareRows := flag.String("compare-rows", "", "compare with a row dump written by -dump")
	compareSchema := flag.String("compare-schema", "", "compare -schema with this schema of the same database, server-side")
	mappingDir := flag.String("mapping-dir", "", "imposm mapping parts (-compare-schema, -compare-rows): generalized tables get their simplification tolerance added")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()
	c, err := pgx.Connect(ctx, *conn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer c.Close(ctx)
	extra := map[string]float64{}
	if *mappingDir != "" {
		parts, err := mapping.LoadDir(*mappingDir)
		if err == nil {
			var m *mapping.Result
			if m, err = mapping.Merge(parts); err == nil {
				extra = m.GeneralizedTolerances
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if *compareSchema != "" {
		d, err := testutil.CompareSchemasTol(ctx, c, *schema, *compareSchema, *prefix, *tol, extra)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if d != "" {
			fmt.Print(d)
			os.Exit(1)
		}
		fmt.Printf("schemas %s and %s are equivalent within tolerance\n", *schema, *compareSchema)
		return
	}
	if *dump != "" {
		rows, err := testutil.TableRows(ctx, c, *schema, *prefix)
		if err == nil {
			err = testutil.WriteRowDump(*dump, testutil.RowDump{Head: *head, Tables: rows})
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Printf("wrote %d tables to %s\n", len(rows), *dump)
		return
	}
	if *compareRows != "" {
		ref, err := testutil.ReadRowDump(*compareRows)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		got, err := testutil.TableRows(ctx, c, *schema, *prefix)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		d, err := testutil.CompareRows(ctx, ref.Tables, got, c, *tol, extra)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if d != "" {
			fmt.Print(d)
			os.Exit(1)
		}
		fmt.Printf("%d tables match %s within tolerance\n", len(got), *compareRows)
		return
	}
	if *tolerantWith != "" {
		c2, err := pgx.Connect(ctx, *tolerantWith)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer c2.Close(ctx)
		d, err := testutil.CompareTolerant(ctx, c, c2, *schema, *tol)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if d != "" {
			fmt.Print(d)
			os.Exit(1)
		}
		fmt.Println("databases are equivalent within tolerance")
		return
	}
	sums, err := testutil.TableChecksumsPrefix(ctx, c, *schema, *prefix)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *compare == "" {
		b, _ := json.MarshalIndent(testutil.Reference{Head: *head, Tables: sums}, "", "  ")
		fmt.Println(string(b))
		return
	}
	ref, err := testutil.LoadReference(*compare)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if d := testutil.DiffSums(ref.Tables, sums); d != "" {
		fmt.Print(d)
		os.Exit(1)
	}
	fmt.Printf("%d tables match %s\n", len(sums), *compare)
}
