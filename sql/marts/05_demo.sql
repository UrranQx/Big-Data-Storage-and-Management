-- A deterministic initial selection. Dashboard controls can select any observed holder.
CREATE TABLE demo_selection AS
WITH candidates AS (
    SELECT r.holder_id,max(r.holder_name) || ' [' || r.holder_id || ']' AS runner,
           r.scope_id,s.scope_name,s.game_name,count(*) AS publications
    FROM run r JOIN scope s USING(scope_id) JOIN field f USING(scope_id)
    WHERE s.field_platform='all' AND f.field_size>=100 AND cardinality(r.participant_ids)=1
    GROUP BY r.holder_id,r.scope_id,s.scope_name,s.game_name
), profiles AS (SELECT holder_id,count(*) AS axes FROM runner_profile GROUP BY holder_id)
SELECT c.* FROM candidates c LEFT JOIN profiles p USING(holder_id)
LEFT JOIN runner_window w ON w.holder_id=c.holder_id AND w.scope_id=c.scope_id AND w.window_days=30
ORDER BY (coalesce(p.axes,0)>=2) DESC,(coalesce(w.sample_size,0)>=5) DESC,c.publications DESC,c.holder_id,c.scope_id LIMIT 1;

CREATE TABLE coverage AS
SELECT c.snapshot_id,c.stage,c.observed_until,c.source_coverage
FROM speedrun_3nf.snapshot_coverage c JOIN _snapshots s USING(snapshot_id);

CREATE TABLE runner_selector AS
SELECT DISTINCT holder_id,holder_name || ' [' || holder_id || ']' AS runner FROM pb;
CREATE UNIQUE INDEX ON runner_selector(holder_id);

CREATE TABLE source_wr AS
SELECT DISTINCT ON (o.scope_key,o.performed_at,o.run_ids)
       o.scope_key,o.performed_at,o.run_ids,o.observed_at,o.snapshot_id,o.source_check_key,
       coalesce(c.game_id,w.game_without_category_id) AS game_id,g.name AS game_name,
       w.category_id,d.name AS category_name
FROM speedrun_3nf.wr_source_observation o JOIN _snapshots sn USING(snapshot_id)
JOIN speedrun_3nf.dim_wr_scope w USING(scope_key)
LEFT JOIN speedrun_3nf.dim_category c USING(category_id)
LEFT JOIN _games g ON g.game_id=coalesce(c.game_id,w.game_without_category_id)
LEFT JOIN speedrun_3nf.dim_category_definition d ON d.game_version_id=g.game_version_id AND d.category_id=w.category_id
WHERE o.observed_at<=(SELECT as_of FROM context)
ORDER BY o.scope_key,o.performed_at,o.run_ids,o.observed_at DESC,o.observation_key;
