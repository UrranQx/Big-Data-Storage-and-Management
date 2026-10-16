CREATE TABLE IF NOT EXISTS payloads (
    key text PRIMARY KEY, document jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS responses (
    key text PRIMARY KEY, snapshot_id text NOT NULL,
    requested_at timestamptz NOT NULL, fetched_at timestamptz NOT NULL,
    http_status integer, document jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS responses_snapshot ON responses(snapshot_id);
CREATE TABLE IF NOT EXISTS observations (
    key text PRIMARY KEY, source_system text NOT NULL,
    entity_type text NOT NULL, source_id text NOT NULL,
    snapshot_id text NOT NULL, response_id text NOT NULL REFERENCES responses(key),
    payload text NOT NULL REFERENCES payloads(key), observed_at timestamptz NOT NULL,
    game_id text, category_id text, status text, document jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS observations_history ON observations(source_system, entity_type, source_id, observed_at);
CREATE INDEX IF NOT EXISTS observations_selection ON observations(snapshot_id, game_id, category_id, status);
CREATE TABLE IF NOT EXISTS manifests (
    key text PRIMARY KEY, document jsonb NOT NULL
);
