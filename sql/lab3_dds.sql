CREATE TABLE IF NOT EXISTS speedrun_3nf.analytics_batch (
    batch_key text PRIMARY KEY,
    snapshots jsonb NOT NULL,
    projection_version text NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS speedrun_3nf.game_observation (
    observation_key text PRIMARY KEY,
    snapshot_id text NOT NULL,
    game_version_id bigint NOT NULL REFERENCES speedrun_3nf.dim_game_version,
    source_version_key text NOT NULL,
    observed_at timestamptz NOT NULL,
    projection text NOT NULL
);

CREATE TABLE IF NOT EXISTS speedrun_3nf.run_observation (
    observation_key text PRIMARY KEY,
    snapshot_id text NOT NULL,
    run_id text NOT NULL REFERENCES speedrun_3nf.fact_run,
    version_id bigint NOT NULL REFERENCES speedrun_3nf.fact_run_version,
    source_version_key text NOT NULL,
    source_check_key text NOT NULL,
    observed_at timestamptz NOT NULL,
    is_orphaned boolean,
    participant_ids text[] NOT NULL
);
CREATE INDEX IF NOT EXISTS run_observation_snapshot ON speedrun_3nf.run_observation (snapshot_id, run_id);

CREATE TABLE IF NOT EXISTS speedrun_3nf.leaderboard_observation (
    observation_key text PRIMARY KEY,
    snapshot_id text NOT NULL,
    run_id text NOT NULL REFERENCES speedrun_3nf.fact_run,
    source_check_key text NOT NULL,
    observed_at timestamptz NOT NULL,
    response_id text,
    source_path text,
    filters jsonb NOT NULL,
    source_place bigint,
    source_obsolete boolean
);
CREATE INDEX IF NOT EXISTS leaderboard_observation_snapshot ON speedrun_3nf.leaderboard_observation (snapshot_id, run_id, observed_at);

CREATE TABLE IF NOT EXISTS speedrun_3nf.player_area_observation (
    observation_key text PRIMARY KEY,
    snapshot_id text NOT NULL,
    player_id text NOT NULL,
    observed_at timestamptz NOT NULL,
    source_version_key text NOT NULL,
    display_name text,
    area_id text,
    has_area boolean NOT NULL,
    is_guest boolean
);
CREATE INDEX IF NOT EXISTS player_area_observation_player ON speedrun_3nf.player_area_observation (player_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS speedrun_3nf.wr_source_observation (
    observation_key text PRIMARY KEY,
    snapshot_id text NOT NULL,
    source_check_key text NOT NULL,
    source_version_key text NOT NULL,
    scope_key text NOT NULL REFERENCES speedrun_3nf.dim_wr_scope,
    observed_at timestamptz NOT NULL,
    performed_at timestamptz,
    run_ids text[] NOT NULL
);

CREATE TABLE IF NOT EXISTS speedrun_3nf.snapshot_coverage (
    snapshot_id text PRIMARY KEY,
    stage text NOT NULL,
    observed_until timestamptz,
    source_coverage jsonb NOT NULL
);

ALTER TABLE speedrun_3nf.dim_run_time ADD COLUMN IF NOT EXISTS duration_ns bigint;
ALTER TABLE speedrun_3nf.dim_run_time ADD COLUMN IF NOT EXISTS parse_status text;

COMMENT ON TABLE speedrun_3nf.run_observation IS 'Run observation linked to the committed ODS check and normalized SCD version';
COMMENT ON TABLE speedrun_3nf.leaderboard_observation IS 'Observed source place and obsolete flag in the original query context';
COMMENT ON TABLE speedrun_3nf.player_area_observation IS 'Presence of areaId is distinct from a missing profile field';
COMMENT ON TABLE speedrun_3nf.wr_source_observation IS 'Source WR event observations retained independently of reconstructed records';
