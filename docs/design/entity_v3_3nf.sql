-- Проект схемы. Проверять в отдельной тестовой БД.
BEGIN;
CREATE SCHEMA speedrun_3nf;
SET LOCAL search_path TO speedrun_3nf;

CREATE TABLE dim_game (
    game_id text PRIMARY KEY,
    release_year integer,
    release_month smallint CHECK (release_month BETWEEN 1 AND 12),
    release_day smallint CHECK (release_day BETWEEN 1 AND 31)
);

-- Наблюдаемая версия метаданных игры.
CREATE TABLE dim_game_version (
    game_version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    game_id text NOT NULL REFERENCES dim_game,
    name text,
    slug text,
    website_url text,
    game_type text,
    run_count bigint,
    total_player_count bigint,
    active_player_count bigint,
    default_timer_code text,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_platform (
    platform_id text PRIMARY KEY
);

CREATE TABLE dim_platform_definition (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    platform_id text NOT NULL REFERENCES dim_platform,
    name text,
    source_url text,
    release_year integer,
    PRIMARY KEY (game_version_id, platform_id)
);

CREATE TABLE game_platform (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    position integer NOT NULL CHECK (position >= 0),
    platform_id text NOT NULL REFERENCES dim_platform,
    PRIMARY KEY (game_version_id, position)
);

CREATE TABLE dim_game_timer (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    position integer NOT NULL CHECK (position >= 0),
    timer_code text NOT NULL,
    PRIMARY KEY (game_version_id, position)
);

CREATE TABLE dim_game_region (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    position integer NOT NULL CHECK (position >= 0),
    region_id text NOT NULL,
    PRIMARY KEY (game_version_id, position)
);

CREATE TABLE dim_category (
    category_id text PRIMARY KEY,
    game_id text REFERENCES dim_game
);

CREATE TABLE dim_category_definition (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    category_id text NOT NULL REFERENCES dim_category,
    name text,
    source_url text,
    rules_text text,
    archived boolean,
    is_per_level boolean,
    is_misc boolean,
    enforce_ms boolean,
    exact_players boolean,
    num_players integer,
    player_match_mode text,
    time_direction text,
    source_position integer,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    PRIMARY KEY (game_version_id, category_id),
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_game_level (
    level_id text PRIMARY KEY,
    game_id text REFERENCES dim_game
);

-- Определение уровня в составе версии метаданных игры.
CREATE TABLE dim_level_definition (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    level_id text NOT NULL REFERENCES dim_game_level,
    name text,
    source_url text,
    archived boolean,
    source_position integer,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    PRIMARY KEY (game_version_id, level_id),
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_variable (
    variable_id text PRIMARY KEY,
    game_id text REFERENCES dim_game
);

CREATE TABLE dim_variable_value (
    value_id text PRIMARY KEY,
    variable_id text REFERENCES dim_variable
);

CREATE TABLE dim_variable_definition (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    variable_id text NOT NULL REFERENCES dim_variable,
    name text,
    source_url text,
    default_value_id text REFERENCES dim_variable_value,
    archived boolean,
    is_mandatory boolean,
    is_subcategory boolean,
    is_obsoleting boolean,
    is_user_defined boolean,
    display_mode text,
    source_position integer,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    PRIMARY KEY (game_version_id, variable_id),
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_value_definition (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    value_id text NOT NULL REFERENCES dim_variable_value,
    name text,
    source_url text,
    rules_text text,
    archived boolean,
    source_position integer,
    PRIMARY KEY (game_version_id, value_id)
);

CREATE TABLE dim_variable_category (
    game_version_id bigint NOT NULL,
    variable_id text NOT NULL,
    category_id text NOT NULL REFERENCES dim_category,
    PRIMARY KEY (game_version_id, variable_id, category_id),
    FOREIGN KEY (game_version_id, variable_id) REFERENCES dim_variable_definition
);

CREATE TABLE dim_variable_level (
    game_version_id bigint NOT NULL,
    variable_id text NOT NULL,
    level_id text NOT NULL REFERENCES dim_game_level,
    PRIMARY KEY (game_version_id, variable_id, level_id),
    FOREIGN KEY (game_version_id, variable_id) REFERENCES dim_variable_definition
);

CREATE TABLE dim_player (
    player_id text PRIMARY KEY,
    signed_up_at timestamptz
);

CREATE TABLE dim_player_version (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id text NOT NULL REFERENCES dim_player,
    display_name text,
    profile_url text,
    area_id text,
    power_level integer,
    color1_id text,
    color2_id text,
    color_animate text,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_game_moderator (
    game_version_id bigint NOT NULL REFERENCES dim_game_version(game_version_id),
    position integer NOT NULL CHECK (position >= 0),
    player_id text NOT NULL REFERENCES dim_player,
    forum_id text,
    moderator_level text,
    PRIMARY KEY (game_version_id, position)
);

CREATE TABLE fact_run (
    run_id text PRIMARY KEY
);

CREATE TABLE fact_run_version (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text NOT NULL REFERENCES fact_run,
    category_id text REFERENCES dim_category,
    game_without_category_id text REFERENCES dim_game,
    platform_id text REFERENCES dim_platform,
    level_id text REFERENCES dim_game_level,
    region_id text,
    status_code integer,
    is_emulated boolean,
    enforce_ms boolean,
    performed_at timestamptz,
    submitted_at timestamptz,
    verified_at timestamptz,
    submitted_by_id text REFERENCES dim_player,
    verified_by_id text REFERENCES dim_player,
    comment_text text,
    video_state text,
    valid_from timestamptz,
    valid_to timestamptz NULL,
    CHECK (category_id IS NULL OR game_without_category_id IS NULL),
    CHECK (valid_to IS NULL or valid_to >= valid_from)
);

CREATE TABLE dim_run_participant (
    version_id bigint NOT NULL REFERENCES fact_run_version,
    position integer NOT NULL CHECK (position >= 0),
    player_id text REFERENCES dim_player,
    guest_name text,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE dim_run_time (
    version_id bigint NOT NULL REFERENCES fact_run_version,
    source_field text NOT NULL,
    source_duration text NOT NULL,
    PRIMARY KEY (version_id, source_field)
);

CREATE TABLE dim_run_value (
    version_id bigint NOT NULL REFERENCES fact_run_version,
    position integer NOT NULL CHECK (position >= 0),
    value_id text NOT NULL REFERENCES dim_variable_value,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE dim_run_video (
    version_id bigint NOT NULL REFERENCES fact_run_version,
    position integer NOT NULL CHECK (position >= 0),
    source_video text NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE dim_wr_scope (
    scope_key text PRIMARY KEY,
    category_id text REFERENCES dim_category,
    game_without_category_id text REFERENCES dim_game,
    verified_filter integer,
    obsolete_filter integer,
    emulator_filter integer,
    video_filter integer,
    CHECK (category_id IS NULL OR game_without_category_id IS NULL)
);

CREATE TABLE dim_wr_scope_value (
    scope_key text NOT NULL REFERENCES dim_wr_scope,
    position integer NOT NULL CHECK (position >= 0),
    value_id text NOT NULL REFERENCES dim_variable_value,
    PRIMARY KEY (scope_key, position)
);

CREATE TABLE fact_wr_event (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope_key text NOT NULL REFERENCES dim_wr_scope,
    performed_at timestamptz
);

CREATE TABLE fact_wr_event_run (
    event_id bigint NOT NULL REFERENCES fact_wr_event,
    position integer NOT NULL CHECK (position >= 0),
    run_id text REFERENCES fact_run,
    PRIMARY KEY (event_id, position)
);

CREATE INDEX game_version_timeline ON dim_game_version (game_id, valid_from);
CREATE INDEX player_version_timeline ON dim_player_version (player_id, valid_from);
CREATE INDEX run_version_timeline ON fact_run_version (run_id, valid_from);

COMMIT;
