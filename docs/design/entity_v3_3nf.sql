-- Проект схемы. Проверять в отдельной тестовой БД.
BEGIN;
CREATE SCHEMA speedrun_3nf;
SET LOCAL search_path TO speedrun_3nf;

CREATE TABLE game (
    game_id text PRIMARY KEY
);

CREATE TABLE game_version (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    game_id text NOT NULL REFERENCES game,
    name text,
    slug text,
    website_url text,
    game_type text,
    run_count bigint,
    total_player_count bigint,
    active_player_count bigint,
    added_at timestamptz,
    updated_at timestamptz,
    release_year integer,
    release_month smallint CHECK (release_month BETWEEN 1 AND 12),
    release_day smallint CHECK (release_day BETWEEN 1 AND 31),
    default_timer_code text
);

CREATE TABLE platform (
    platform_id text PRIMARY KEY
);

CREATE TABLE platform_definition (
    version_id bigint NOT NULL REFERENCES game_version,
    platform_id text NOT NULL REFERENCES platform,
    name text,
    source_url text,
    release_year integer,
    PRIMARY KEY (version_id, platform_id)
);

CREATE TABLE game_platform (
    version_id bigint NOT NULL REFERENCES game_version,
    position integer NOT NULL CHECK (position >= 0),
    platform_id text NOT NULL REFERENCES platform,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE game_timer (
    version_id bigint NOT NULL REFERENCES game_version,
    position integer NOT NULL CHECK (position >= 0),
    timer_code text NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE game_region (
    version_id bigint NOT NULL REFERENCES game_version,
    position integer NOT NULL CHECK (position >= 0),
    region_id text NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE category (
    category_id text PRIMARY KEY,
    game_id text REFERENCES game
);

CREATE TABLE category_definition (
    version_id bigint NOT NULL REFERENCES game_version,
    category_id text NOT NULL REFERENCES category,
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
    PRIMARY KEY (version_id, category_id)
);

CREATE TABLE game_level (
    level_id text PRIMARY KEY,
    game_id text REFERENCES game
);

CREATE TABLE level_definition (
    version_id bigint NOT NULL REFERENCES game_version,
    level_id text NOT NULL REFERENCES game_level,
    name text,
    source_url text,
    archived boolean,
    source_position integer,
    PRIMARY KEY (version_id, level_id)
);

CREATE TABLE variable (
    variable_id text PRIMARY KEY,
    game_id text REFERENCES game
);

CREATE TABLE variable_value (
    value_id text PRIMARY KEY,
    variable_id text REFERENCES variable
);

CREATE TABLE variable_definition (
    version_id bigint NOT NULL REFERENCES game_version,
    variable_id text NOT NULL REFERENCES variable,
    name text,
    source_url text,
    default_value_id text REFERENCES variable_value,
    archived boolean,
    is_mandatory boolean,
    is_subcategory boolean,
    is_obsoleting boolean,
    is_user_defined boolean,
    display_mode text,
    source_position integer,
    PRIMARY KEY (version_id, variable_id)
);

CREATE TABLE value_definition (
    version_id bigint NOT NULL REFERENCES game_version,
    value_id text NOT NULL REFERENCES variable_value,
    name text,
    source_url text,
    rules_text text,
    archived boolean,
    source_position integer,
    PRIMARY KEY (version_id, value_id)
);

CREATE TABLE variable_category (
    version_id bigint NOT NULL,
    variable_id text NOT NULL,
    category_id text NOT NULL REFERENCES category,
    PRIMARY KEY (version_id, variable_id, category_id),
    FOREIGN KEY (version_id, variable_id) REFERENCES variable_definition
);

CREATE TABLE variable_level (
    version_id bigint NOT NULL,
    variable_id text NOT NULL,
    level_id text NOT NULL REFERENCES game_level,
    PRIMARY KEY (version_id, variable_id, level_id),
    FOREIGN KEY (version_id, variable_id) REFERENCES variable_definition
);

CREATE TABLE player (
    player_id text PRIMARY KEY
);

CREATE TABLE player_version (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    player_id text NOT NULL REFERENCES player,
    display_name text,
    profile_url text,
    area_id text,
    power_level integer,
    color1_id text,
    color2_id text,
    color_animate text,
    signed_up_at timestamptz,
    updated_at timestamptz
);

CREATE TABLE game_moderator (
    version_id bigint NOT NULL REFERENCES game_version,
    position integer NOT NULL CHECK (position >= 0),
    player_id text NOT NULL REFERENCES player,
    forum_id text,
    moderator_level text,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE run (
    run_id text PRIMARY KEY
);

CREATE TABLE run_version (
    version_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id text NOT NULL REFERENCES run,
    category_id text REFERENCES category,
    game_without_category_id text REFERENCES game,
    platform_id text REFERENCES platform,
    level_id text REFERENCES game_level,
    region_id text,
    status_code integer,
    is_emulated boolean,
    enforce_ms boolean,
    performed_at timestamptz,
    submitted_at timestamptz,
    verified_at timestamptz,
    submitted_by_id text REFERENCES player,
    verified_by_id text REFERENCES player,
    comment_text text,
    video_state text,
    CHECK (category_id IS NULL OR game_without_category_id IS NULL)
);

CREATE TABLE run_participant (
    version_id bigint NOT NULL REFERENCES run_version,
    position integer NOT NULL CHECK (position >= 0),
    player_id text REFERENCES player,
    guest_name text,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE run_time (
    version_id bigint NOT NULL REFERENCES run_version,
    source_field text NOT NULL,
    source_duration text NOT NULL,
    PRIMARY KEY (version_id, source_field)
);

CREATE TABLE run_value (
    version_id bigint NOT NULL REFERENCES run_version,
    position integer NOT NULL CHECK (position >= 0),
    value_id text NOT NULL REFERENCES variable_value,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE run_video (
    version_id bigint NOT NULL REFERENCES run_version,
    position integer NOT NULL CHECK (position >= 0),
    source_video text NOT NULL,
    PRIMARY KEY (version_id, position)
);

CREATE TABLE wr_scope (
    scope_key text PRIMARY KEY,
    category_id text REFERENCES category,
    game_without_category_id text REFERENCES game,
    verified_filter integer,
    obsolete_filter integer,
    emulator_filter integer,
    video_filter integer,
    CHECK (category_id IS NULL OR game_without_category_id IS NULL)
);

CREATE TABLE wr_scope_value (
    scope_key text NOT NULL REFERENCES wr_scope,
    position integer NOT NULL CHECK (position >= 0),
    value_id text NOT NULL REFERENCES variable_value,
    PRIMARY KEY (scope_key, position)
);

CREATE TABLE wr_event (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scope_key text NOT NULL REFERENCES wr_scope,
    performed_at timestamptz
);

CREATE TABLE wr_event_run (
    event_id bigint NOT NULL REFERENCES wr_event,
    position integer NOT NULL CHECK (position >= 0),
    run_id text REFERENCES run,
    PRIMARY KEY (event_id, position)
);

COMMIT;
