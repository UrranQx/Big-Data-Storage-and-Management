CREATE TABLE game_portfolio AS
SELECT f.holder_id,max(f.holder_name) AS holder_name,s.game_id,max(s.game_name) AS game_name,
       min(f.first_at) AS first_at,max(p.strength) FILTER (WHERE p.field_size>=10) AS strength
FROM first_publication f JOIN scope s USING (scope_id)
JOIN player person ON person.player_id=f.holder_id AND person.is_guest IS DISTINCT FROM true
LEFT JOIN pb p ON p.scope_id=f.scope_id AND p.holder_id=f.holder_id
WHERE s.field_platform='all'
GROUP BY f.holder_id,s.game_id;
CREATE UNIQUE INDEX ON game_portfolio (holder_id,game_id);
CREATE INDEX ON game_portfolio (game_id,first_at);

CREATE TEMP TABLE _first_game ON COMMIT DROP AS
SELECT holder_id,min(first_at) AS first_at FROM game_portfolio GROUP BY holder_id;
CREATE UNIQUE INDEX ON _first_game (holder_id);
CREATE TEMP TABLE _game_names ON COMMIT DROP AS
SELECT game_id,max(game_name) AS game_name FROM game_portfolio GROUP BY game_id;

CREATE TEMP TABLE _baseline ON COMMIT DROP AS
SELECT g.game_id,
       count(*) FILTER (WHERE b.first_at IS NULL OR b.first_at>a.first_at) AS potential_people,
       count(*) FILTER (WHERE b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days') AS transitioned_people
FROM _first_game a CROSS JOIN _game_names g
LEFT JOIN game_portfolio b ON b.holder_id=a.holder_id AND b.game_id=g.game_id
WHERE a.first_at<=(SELECT as_of FROM context)-interval '365 days'
GROUP BY g.game_id;

CREATE TABLE game_transition AS
WITH counts AS (
    SELECT a.game_id AS source_game,g.game_id AS destination_game,
           count(*) FILTER (WHERE b.first_at IS NULL OR b.first_at>a.first_at) AS potential_people,
           count(*) FILTER (WHERE b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days') AS transitioned_people,
           count(*) FILTER (WHERE b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days' AND b.strength>=75) AS strong_people
    FROM game_portfolio a CROSS JOIN _game_names g
    LEFT JOIN game_portfolio b ON b.holder_id=a.holder_id AND b.game_id=g.game_id
    WHERE a.game_id<>g.game_id AND a.first_at<=(SELECT as_of FROM context)-interval '365 days'
    GROUP BY a.game_id,g.game_id
)
SELECT c.*,sn.game_name AS source_name,dn.game_name AS destination_name,
       (c.transitioned_people+1.0)/(c.potential_people+2.0) AS transition_rate,
       (b.transitioned_people+1.0)/(b.potential_people+2.0) AS baseline_rate,
       ((c.transitioned_people+1.0)/(c.potential_people+2.0))/((b.transitioned_people+1.0)/(b.potential_people+2.0)) AS lift,
       c.transitioned_people>=5 AND c.potential_people>=30 AS supported
FROM counts c JOIN _baseline b ON b.game_id=c.destination_game
JOIN _game_names sn ON sn.game_id=c.source_game JOIN _game_names dn ON dn.game_id=c.destination_game;
CREATE UNIQUE INDEX ON game_transition (source_game,destination_game);

CREATE TABLE destination_category AS
SELECT a.game_id AS source_game,s.game_id AS destination_game,s.scope_id,s.category_name,s.values_name,
       count(DISTINCT a.holder_id) AS transitioned_people,
       count(DISTINCT a.holder_id) FILTER (WHERE p.strength>=75 AND p.field_size>=10) AS strong_people
FROM game_portfolio a JOIN game_portfolio b ON b.holder_id=a.holder_id AND b.game_id<>a.game_id
JOIN scope s ON s.game_id=b.game_id AND s.field_platform='all'
JOIN pb p ON p.scope_id=s.scope_id AND p.holder_id=a.holder_id
WHERE a.first_at<=(SELECT as_of FROM context)-interval '365 days'
  AND b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days'
GROUP BY a.game_id,s.game_id,s.scope_id,s.category_name,s.values_name;
CREATE INDEX ON destination_category (source_game,destination_game,strong_people DESC);

COMMENT ON TABLE game_transition IS 'Observed transitions within 365 days, Laplace-smoothed rates and lift over first-game destination frequency. Support requires 5 transitions and 30 eligible people. Strong results use the current archive field.';
