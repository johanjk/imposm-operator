# Database

What a sync needs from its PostgreSQL database, and how to give readers
access to the data. Schema names below are the defaults
(`database.schemas`: `import`, `public`, `backup`, `imposm_operator`); use
the sync's own.

## What a sync does

The runner connects as the sync's user (`database.user`, or `user` in
`database.secretRef`) and:

| Does | Needs |
| --- | --- |
| Creates its schemas when missing (`CREATE SCHEMA IF NOT EXISTS`): import, production, backup and state | `CREATE` on the database, or the schemas created beforehand and owned by the user |
| Records claims on table names in `imposm_operator.table_claim`, shared by every sync in the database (fixed name, whatever `schemas.state` is) | Creating it, or `USAGE` on `imposm_operator` and `SELECT, INSERT, UPDATE, DELETE` on the table |
| Imports into the import schema with imposm, and deploys by moving the tables into production (and the previous ones into backup) with `ALTER TABLE … SET SCHEMA` and `RENAME` | Owning the tables (it creates them), `CREATE` on the three schemas |
| Drops tables that left the mapping, the backup and stale imports | Owning those tables |
| Takes advisory locks (one runner per sync, claims across syncs) | nothing extra |
| Checks for the `postgis` extension and runs `CREATE EXTENSION postgis` only when it is missing | nothing, once an admin has created the extension |

It needs no superuser or role-management rights. `jsonb_tags` columns are
`jsonb`, a built-in type; `hstore_tags` columns need the `hstore` extension,
which neither imposm nor the operator creates.

## Setup

As a database admin:

```sql
CREATE ROLE osm LOGIN PASSWORD '...';
CREATE DATABASE osm OWNER osm;            -- or, in an existing database: GRANT CREATE ON DATABASE osm TO osm;
\c osm
CREATE EXTENSION postgis;
```

Without `CREATE` on the database, create the schemas for the user instead:

```sql
CREATE SCHEMA import AUTHORIZATION osm;
CREATE SCHEMA backup AUTHORIZATION osm;
CREATE SCHEMA imposm_operator AUTHORIZATION osm;
-- production: the database owner's public schema is not the sync user's;
-- prefer a schema of its own, e.g. schemas.production: osm
CREATE SCHEMA osm AUTHORIZATION osm;
```

The default production schema is `public`. Since PostgreSQL 15 only the
database owner may create in `public`, so a sync user that does not own the
database needs `GRANT CREATE ON SCHEMA public TO osm`, or (better) a
production schema of its own.

Several syncs with different users in one database share
`imposm_operator.table_claim`: create the schema as an admin and grant each
sync user `USAGE, CREATE` on it (the first sync creates the table and owns
it; grant the others access to it, or create it in advance).

CloudNativePG: the cluster's application user owns its database, so the
example in [`examples/cnpg-cluster.yaml`](../examples/cnpg-cluster.yaml)
needs nothing more (its `postgis` extension is created at bootstrap).

Azure Database for PostgreSQL: allow `POSTGIS` (and `HSTORE` for
`hstore_tags`) in the server parameter `azure.extensions` before
`CREATE EXTENSION`.

## Readers

Give consumers their own read-only role, not the sync user:

```sql
CREATE ROLE osm_reader LOGIN PASSWORD '...';
GRANT CONNECT ON DATABASE osm TO osm_reader;
GRANT USAGE ON SCHEMA osm TO osm_reader;   -- the production schema
-- every table the sync user creates from now on, in any schema
ALTER DEFAULT PRIVILEGES FOR ROLE osm GRANT SELECT ON TABLES TO osm_reader;
-- tables that exist already
GRANT SELECT ON ALL TABLES IN SCHEMA osm TO osm_reader;
```

Set the default privileges without `IN SCHEMA`: a sync creates its tables in
the import schema and moves them into production on deploy, and a table
keeps the privileges it was created with when it moves. Default privileges
`IN SCHEMA <production>` would never apply, as no table is created there.
With `USAGE` only on the production schema, readers see the deployed data,
not an import in progress or the backup. The schema must exist for the
`GRANT USAGE`: create it for the sync user first (`CREATE SCHEMA osm
AUTHORIZATION osm`) or grant after the first deploy.

## Metadata tables

With `spec.metadata` the sync user also creates `metadata.schema`
(by default the production schema with `_history`, e.g. `osm_history`; one
per sync) and owns the tables in it (see
[design-osm-metadata.md](design-osm-metadata.md)): `CREATE` on the database
covers it, or create the schema for the user beforehand. The schema must
belong to the sync user (or a role it is a member of): one owned by another
user is refused, as its owner could plant a trigger that runs with the sync
user's rights.

```sql
CREATE SCHEMA osm_history AUTHORIZATION osm;
```

Readers:

```sql
GRANT USAGE ON SCHEMA osm_history TO osm_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE osm IN SCHEMA osm_history GRANT SELECT ON TABLES TO osm_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA osm_history TO osm_reader;
```

Unlike imposm's tables, these are created in their own schema and stay
there, so `IN SCHEMA` default privileges work (`ON TABLES` covers views). A
snapshot load replaces `element_meta` with a new table, and a deploy
recreates the views of the production tables; both get the default
privileges. The views run with the sync user's rights, so a reader with
`SELECT` on them reads the production rows without access to the
production schema. The join function `element` is executable by everyone
(PostgreSQL's default for functions).

