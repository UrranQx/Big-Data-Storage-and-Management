-- The caller pins the DDS snapshot and the destination schema in one transaction.
CREATE TEMP TABLE _games ON COMMIT DROP AS
SELECT DISTINCT ON (g.game_id) g.*
FROM speedrun_3nf.dim_game_version g
JOIN speedrun_3nf.game_observation o USING (game_version_id)
JOIN _snapshots s USING (snapshot_id)
WHERE g.valid_from <= (SELECT as_of FROM context)
  AND o.projection='game_detail'
  AND EXISTS (SELECT 1 FROM speedrun_3nf.dim_category_definition c WHERE c.game_version_id=g.game_version_id)
ORDER BY g.game_id,g.valid_from DESC,g.game_version_id DESC;

CREATE TEMP TABLE _observed_runs ON COMMIT DROP AS
SELECT DISTINCT ON (o.run_id) o.*
FROM speedrun_3nf.run_observation o JOIN _snapshots s USING (snapshot_id)
WHERE o.observed_at <= (SELECT as_of FROM context)
ORDER BY o.run_id,o.observed_at DESC,o.observation_key DESC;
CREATE UNIQUE INDEX ON _observed_runs (run_id);

CREATE TEMP TABLE _places ON COMMIT DROP AS
SELECT DISTINCT ON (o.run_id) o.*
FROM speedrun_3nf.leaderboard_observation o JOIN _snapshots s USING (snapshot_id)
WHERE o.observed_at <= (SELECT as_of FROM context)
ORDER BY o.run_id,o.observed_at DESC,o.observation_key DESC;
CREATE UNIQUE INDEX ON _places (run_id);

CREATE TABLE player AS
WITH names AS (
    SELECT DISTINCT ON (p.player_id) p.player_id,p.display_name,p.is_guest
    FROM speedrun_3nf.player_area_observation p JOIN _snapshots s USING (snapshot_id)
    WHERE p.observed_at <= (SELECT as_of FROM context)
    ORDER BY p.player_id,p.observed_at DESC,p.observation_key DESC
), areas AS (
    SELECT DISTINCT ON (p.player_id) p.player_id,p.area_id
    FROM speedrun_3nf.player_area_observation p JOIN _snapshots s USING (snapshot_id)
    WHERE p.has_area AND p.is_guest IS DISTINCT FROM true
      AND p.observed_at <= (SELECT as_of FROM context)
    ORDER BY p.player_id,p.observed_at DESC,p.observation_key DESC
)
SELECT n.player_id,coalesce(n.display_name,n.player_id) AS player_name,n.is_guest,
       nullif(upper(split_part(a.area_id,'/',1)),'') AS country_code
FROM names n LEFT JOIN areas a USING (player_id);
CREATE UNIQUE INDEX ON player (player_id);
ANALYZE player;

CREATE TEMP TABLE _input ON COMMIT DROP AS
SELECT o.run_id,o.version_id,o.snapshot_id,o.source_version_key,o.observed_at,
       o.is_orphaned,o.participant_ids,r.category_id,c.game_id,g.name AS game_name,
       d.name AS category_name,coalesce(d.is_misc,false) AS is_misc,d.rules_text,
       d.time_direction,CASE d.time_direction WHEN '0' THEN 1 WHEN '1' THEN -1 END AS direction,
       r.status_code,r.platform_id,r.is_emulated,r.performed_at,r.submitted_at,r.verified_at,
       r.level_id,r.comment_text,coalesce(pn.name,r.platform_id,'Неизвестно') AS platform_name,
       CASE g.default_timer_code WHEN '0' THEN 'time' WHEN '1' THEN 'timeWithLoads' WHEN '2' THEN 'igt' END AS timer,
       t.duration_ns,t.parse_status,p.source_place,p.source_obsolete,
       array_to_string(o.participant_ids,'+') AS holder_id,
       coalesce(h.holder_name,array_to_string(o.participant_ids,' + ')) AS holder_name,
       v.values_key,coalesce(v.values_name,'') AS values_name,
       v.missing_required,
       coalesce(video.source_video,'https://www.speedrun.com/runs/' || o.run_id) AS video_url
FROM _observed_runs o
JOIN speedrun_3nf.fact_run_version r USING (version_id)
JOIN speedrun_3nf.dim_category c USING (category_id)
JOIN _games g ON g.game_id=c.game_id
JOIN speedrun_3nf.dim_category_definition d ON d.game_version_id=g.game_version_id AND d.category_id=r.category_id
LEFT JOIN _places p ON p.run_id=o.run_id
LEFT JOIN speedrun_3nf.dim_platform_definition pn ON pn.game_version_id=g.game_version_id AND pn.platform_id=r.platform_id
LEFT JOIN speedrun_3nf.dim_run_time t ON t.version_id=r.version_id AND t.source_field=
    CASE g.default_timer_code WHEN '0' THEN 'time' WHEN '1' THEN 'timeWithLoads' WHEN '2' THEN 'igt' END
LEFT JOIN LATERAL (
    SELECT string_agg(coalesce(p.player_name,u.id),' + ' ORDER BY u.id) AS holder_name
    FROM unnest(o.participant_ids) u(id) LEFT JOIN player p ON p.player_id=u.id
) h ON true
LEFT JOIN LATERAL (
    SELECT string_agg(vd.variable_id || '=' || coalesce(rv.value_id,vd.default_value_id,'?'),',' ORDER BY vd.variable_id) AS values_key,
           string_agg(vd.name || '=' || coalesce(val.name,'?'),', ' ORDER BY vd.variable_id) AS values_name,
           bool_or(coalesce(rv.value_id,vd.default_value_id) IS NULL) AS missing_required
    FROM speedrun_3nf.dim_variable_definition vd
    LEFT JOIN LATERAL (
        SELECT dr.value_id FROM speedrun_3nf.dim_run_value dr
        JOIN speedrun_3nf.dim_variable_value vv USING (value_id)
        WHERE dr.version_id=r.version_id AND vv.variable_id=vd.variable_id
        ORDER BY dr.position LIMIT 1
    ) rv ON true
    LEFT JOIN speedrun_3nf.dim_value_definition val
      ON val.game_version_id=g.game_version_id AND val.value_id=coalesce(rv.value_id,vd.default_value_id)
    WHERE vd.game_version_id=g.game_version_id AND vd.is_subcategory
      -- These marts contain full-game runs; level-only variables are unrelated
      -- even when they have no category binding and a default value.
      AND NOT EXISTS (SELECT 1 FROM speedrun_3nf.dim_variable_level vl
                      WHERE vl.game_version_id=g.game_version_id AND vl.variable_id=vd.variable_id)
      AND (NOT EXISTS (SELECT 1 FROM speedrun_3nf.dim_variable_category vc WHERE vc.game_version_id=g.game_version_id AND vc.variable_id=vd.variable_id)
           OR EXISTS (SELECT 1 FROM speedrun_3nf.dim_variable_category vc WHERE vc.game_version_id=g.game_version_id AND vc.variable_id=vd.variable_id AND vc.category_id=r.category_id))
) v ON true
LEFT JOIN LATERAL (
    SELECT source_video FROM speedrun_3nf.dim_run_video WHERE version_id=r.version_id ORDER BY position LIMIT 1
) video ON true
WHERE d.is_per_level IS DISTINCT FROM true;

CREATE TABLE quality_summary AS
SELECT (SELECT count(*) FROM _observed_runs) AS source_runs,
       (SELECT count(*) FROM _observed_runs)-count(*) AS unresolved_scope_runs,
       count(*) AS observed_runs,
       count(*) FILTER (WHERE duration_ns IS NULL OR duration_ns<=0) AS invalid_timer,
       count(*) FILTER (WHERE is_orphaned) AS orphaned_runs,
       count(*) FILTER (WHERE cardinality(participant_ids)=0) AS missing_participants,
       count(*) FILTER (WHERE missing_required) AS missing_scope_value,
       count(*) FILTER (WHERE direction IS NULL) AS unknown_direction,
       count(*) FILTER (WHERE submitted_at IS NULL) AS missing_submission_date,
       count(*) FILTER (WHERE performed_at IS NULL) AS missing_performed_date,
       count(*) FILTER (WHERE source_place IS NULL) AS missing_source_place
       ,count(*) FILTER (WHERE verified_at<submitted_at) AS quarantined_date_anomalies
FROM _input;

CREATE TEMP TABLE _eligible ON COMMIT DROP AS
SELECT i.*,f.field_platform,
       md5(concat_ws('|',i.game_id,i.category_id,coalesce(i.values_key,''),i.timer,i.time_direction,
                     md5(coalesce(i.rules_text,'')),f.field_platform)) AS scope_id
FROM _input i
CROSS JOIN LATERAL (SELECT 'all'::text AS field_platform UNION ALL SELECT i.platform_id WHERE i.platform_id IS NOT NULL) f
WHERE i.status_code=1 AND i.is_orphaned IS DISTINCT FROM true AND i.level_id IS NULL
  AND i.duration_ns>0 AND i.direction IS NOT NULL AND cardinality(i.participant_ids)>0
  AND i.missing_required IS DISTINCT FROM true
  AND (i.verified_at<i.submitted_at) IS NOT TRUE;

CREATE TABLE scope AS
SELECT DISTINCT scope_id,game_id,game_name,category_id,category_name,timer,direction,is_misc,field_platform,
       CASE WHEN field_platform='all' THEN 'Все платформы' ELSE platform_name END AS field_name,
       coalesce(values_key,'') AS values_key,values_name,md5(coalesce(rules_text,'')) AS rules_version,
       game_name || ' · ' || category_name || CASE WHEN values_name='' THEN '' ELSE ' · ' || values_name END
           || ' · ' || CASE WHEN field_platform='all' THEN 'Все платформы' ELSE platform_name END || ' · ' || timer AS scope_name
FROM _eligible;
CREATE UNIQUE INDEX ON scope (scope_id);
ANALYZE scope;

CREATE TABLE run AS
SELECT scope_id,run_id,version_id,snapshot_id,source_version_key,observed_at,
       holder_id,holder_name,participant_ids,duration_ns,duration_ns * direction AS score,
       duration_ns / 1e9::numeric AS seconds,source_place,source_obsolete,
       performed_at,submitted_at,verified_at,platform_id,is_emulated,video_url
FROM _eligible;
CREATE UNIQUE INDEX ON run (scope_id,run_id);
CREATE INDEX ON run (scope_id,holder_id,submitted_at);
CREATE INDEX ON run (scope_id,performed_at);
ANALYZE run;

CREATE TABLE pb AS
WITH best AS (
    SELECT DISTINCT ON (scope_id,holder_id) *
    FROM run WHERE source_obsolete=false
    ORDER BY scope_id,holder_id,score,run_id
), ranked AS (
    SELECT *,rank() OVER (PARTITION BY scope_id ORDER BY score) AS place,
           count(*) OVER (PARTITION BY scope_id) AS field_size,
           count(*) OVER (PARTITION BY scope_id,score) AS tie_size,
           first_value(seconds) OVER (PARTITION BY scope_id ORDER BY score) AS wr_seconds
    FROM best
)
SELECT *,100.0 * (field_size-place-(tie_size-1)/2.0) / nullif(field_size-1,0) AS strength,
       abs(seconds-wr_seconds) AS gap_seconds,
       100.0*abs(seconds-wr_seconds)/nullif(wr_seconds,0) AS gap_pct
FROM ranked;
CREATE UNIQUE INDEX ON pb (scope_id,holder_id);
CREATE INDEX ON pb (holder_id,scope_id);
ANALYZE pb;

CREATE TABLE field AS
SELECT s.*,count(p.holder_id) AS field_size,min(p.wr_seconds) AS wr_seconds,
       max(p.gap_pct) FILTER (WHERE p.place<=10) AS top10_gap_pct,
       count(*) FILTER (WHERE p.gap_pct<=1) AS within_one_pct,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY p.seconds) AS median_seconds
FROM scope s LEFT JOIN pb p USING (scope_id) GROUP BY s.scope_id,s.game_id,s.game_name,s.category_id,s.category_name,
    s.timer,s.direction,s.is_misc,s.field_platform,s.field_name,s.values_key,s.values_name,s.rules_version,s.scope_name;
CREATE UNIQUE INDEX ON field (scope_id);
ANALYZE field;

CREATE TABLE target AS
SELECT f.scope_id,t.target_code,t.target_n,max(p.score) AS threshold_score,
       max(p.score)*f.direction/1e9::numeric AS threshold_seconds,f.field_size,
       f.field_size>=t.target_n AS sufficient_field
FROM field f
CROSS JOIN LATERAL (
    VALUES ('top3',3::bigint),('top5',5),('top10',10),('top50',50),('top100',100),
           ('top1pct',greatest(1,ceil(f.field_size*0.01)::bigint)),
           ('top25pct',greatest(1,ceil(f.field_size*0.25)::bigint))
) t(target_code,target_n)
JOIN pb p ON p.scope_id=f.scope_id AND p.place<=t.target_n
GROUP BY f.scope_id,t.target_code,t.target_n,f.field_size,f.direction;
CREATE UNIQUE INDEX ON target (scope_id,target_code);

CREATE TABLE goal AS
SELECT p.scope_id,p.holder_id,p.holder_name,p.run_id,p.place,p.field_size,p.seconds AS pb_seconds,p.strength,
       t.target_code,t.target_n,t.threshold_seconds,t.sufficient_field,
       greatest(0,p.score-t.threshold_score)/1e9::numeric AS improvement_seconds,
       100.0*greatest(0,p.score-t.threshold_score)/nullif(abs(t.threshold_score),0) AS improvement_pct
FROM pb p JOIN target t USING (scope_id) WHERE t.target_code<>'top25pct';
CREATE INDEX ON goal (holder_id,scope_id,target_code);

CREATE TABLE country_contribution AS
SELECT DISTINCT i.run_id,i.game_id,i.game_name,i.category_id,i.category_name,i.is_misc,
       i.source_place,coalesce(c.country_code,'UNKNOWN') AS country_code
FROM _input i
LEFT JOIN LATERAL unnest(i.participant_ids) u(player_id) ON true
LEFT JOIN player p USING (player_id)
LEFT JOIN country c USING (country_code)
WHERE i.status_code=1 AND i.source_obsolete=false AND i.source_place BETWEEN 1 AND 3
  AND i.is_orphaned IS DISTINCT FROM true AND i.level_id IS NULL
  AND (i.verified_at<i.submitted_at) IS NOT TRUE;
CREATE INDEX ON country_contribution (game_id,country_code);

CREATE TABLE country_medals AS
WITH counts AS (
    SELECT country_code,count(*) FILTER (WHERE source_place=1) AS gold,
           count(*) FILTER (WHERE source_place=2) AS silver,count(*) FILTER (WHERE source_place=3) AS bronze
    FROM country_contribution WHERE country_code<>'UNKNOWN' GROUP BY country_code
), weighted AS (
    SELECT *,1+greatest(max(silver) OVER (),max(bronze) OVER ())::numeric AS base FROM counts
)
SELECT w.*,c.country_name,gold*base*base+silver*base+bronze AS medal_score,
       rank() OVER (ORDER BY gold DESC,silver DESC,bronze DESC) AS overall_rank
FROM weighted w JOIN country c USING (country_code);

COMMENT ON TABLE pb IS 'One current PB per holder and comparable field. RANK ties use competition ranking.';
COMMENT ON TABLE country_contribution IS 'One source podium run credited once to each represented country. UNKNOWN is reported separately.';
