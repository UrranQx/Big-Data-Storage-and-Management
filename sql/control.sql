CREATE TABLE IF NOT EXISTS receipts (
    snapshot_id text NOT NULL, store text NOT NULL,
    package_digest text NOT NULL, verified_at timestamptz NOT NULL DEFAULT now(),
    summary jsonb NOT NULL, PRIMARY KEY (snapshot_id, store)
);
CREATE TABLE IF NOT EXISTS snapshot_status (
    snapshot_id text PRIMARY KEY, state text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(), details jsonb NOT NULL
);
