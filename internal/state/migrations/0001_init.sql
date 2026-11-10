-- {{schema}} is replaced with the configured (validated) state schema.
CREATE TABLE {{schema}}.target (
    id         bigserial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE {{schema}}.import (
    id              bigserial PRIMARY KEY,
    target_id       bigint NOT NULL REFERENCES {{schema}}.target(id),
    pbf_url         text NOT NULL,
    replication_url text NOT NULL,
    -- the import's configuration (mapping, SRID, table prefix, limitTo, schemas)
    config_hash     text NOT NULL,
    -- why it was started (Initial, MappingChanged, SRIDChanged, LimitToChanged,
    -- SourceChanged, ConfigChanged, ReimportAction, CacheLost, Recovery)
    reason          text NOT NULL DEFAULT '',
    sequence        bigint CHECK (sequence >= 0),
    timestamp       timestamptz,
    pbf_md5         text NOT NULL DEFAULT '',
    phase           text NOT NULL CHECK (phase IN ('pending', 'downloaded', 'read', 'written', 'optimized',
                                                   'ready', 'deployed', 'reverted', 'superseded', 'needs_reimport')),
    last_error      text NOT NULL DEFAULT '',
    -- the backup schema holds the production data this import replaced
    -- (deploy.keepBackup at deploy time, cleared by remove-backup):
    -- revert-deploy needs it
    backup_kept     boolean NOT NULL DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deployed_at     timestamptz
);
CREATE INDEX import_target_idx ON {{schema}}.import (target_id, id DESC);
-- at most one deployed import per target
CREATE UNIQUE INDEX import_one_deployed ON {{schema}}.import (target_id) WHERE phase = 'deployed';

CREATE TABLE {{schema}}.diff_batch (
    id            bigserial PRIMARY KEY,
    import_id     bigint NOT NULL REFERENCES {{schema}}.import(id),
    from_seq      bigint NOT NULL,
    to_seq        bigint NOT NULL,
    to_timestamp  timestamptz,
    target_schema text NOT NULL,
    status        text NOT NULL CHECK (status IN ('running', 'applied', 'failed')),
    error         text NOT NULL DEFAULT '',
    note          text NOT NULL DEFAULT '',
    started_at    timestamptz NOT NULL DEFAULT now(),
    finished_at   timestamptz,
    CHECK (to_seq >= from_seq)
);
CREATE UNIQUE INDEX diff_batch_applied_from ON {{schema}}.diff_batch (import_id, from_seq) WHERE status = 'applied';
CREATE INDEX diff_batch_import_idx ON {{schema}}.diff_batch (import_id, to_seq DESC) WHERE status = 'applied';
