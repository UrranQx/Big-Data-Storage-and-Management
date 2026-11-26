CREATE TABLE runner_form AS
WITH windows AS (
    SELECT r.scope_id,r.holder_id,r.holder_name,r.run_id,r.submitted_at,r.seconds,r.score,
           row_number() OVER series AS publication_number,
           min(r.score) OVER series AS pb_score,
           array_agg(r.seconds) OVER recent AS recent_seconds,
           array_agg(r.submitted_at) OVER recent AS recent_dates
    FROM run r WHERE r.submitted_at IS NOT NULL AND r.submitted_at<=(SELECT as_of FROM context)
    WINDOW series AS (PARTITION BY r.scope_id,r.holder_id ORDER BY r.submitted_at,r.score,r.run_id ROWS UNBOUNDED PRECEDING),
           recent AS (PARTITION BY r.scope_id,r.holder_id ORDER BY r.submitted_at,r.score,r.run_id ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
)
SELECT w.*,w.pb_score*s.direction/1e9::numeric AS pb_seconds,
       cardinality(recent_seconds) AS window_count,
       q.median_seconds,q.q1_seconds,q.q3_seconds,recent_dates[1] AS window_start
FROM windows w JOIN scope s USING (scope_id)
CROSS JOIN LATERAL (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS median_seconds,
           percentile_cont(0.25) WITHIN GROUP (ORDER BY x) AS q1_seconds,
           percentile_cont(0.75) WITHIN GROUP (ORDER BY x) AS q3_seconds
    FROM unnest(w.recent_seconds) u(x)
) q;
CREATE INDEX ON runner_form (holder_id,scope_id,submitted_at);

CREATE TABLE runner_window AS
WITH numbered AS (
    SELECT r.scope_id,r.holder_id,r.holder_name,r.seconds,r.submitted_at,w.window_days,
           row_number() OVER (PARTITION BY r.scope_id,r.holder_id,w.window_days ORDER BY r.submitted_at DESC,r.run_id) AS ordinal
    FROM run r CROSS JOIN (VALUES (7),(30),(90)) w(window_days)
    WHERE r.submitted_at > (SELECT as_of FROM context)-w.window_days*interval '1 day'
      AND r.submitted_at<=(SELECT as_of FROM context)
)
SELECT scope_id,holder_id,holder_name,window_days,count(*) AS sample_size,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY seconds) AS median_seconds,
       percentile_cont(0.25) WITHIN GROUP (ORDER BY seconds) AS q1_seconds,
       percentile_cont(0.75) WITHIN GROUP (ORDER BY seconds) AS q3_seconds,
       min(submitted_at) AS first_publication,max(submitted_at) AS last_publication
FROM numbered WHERE ordinal<=10 GROUP BY scope_id,holder_id,holder_name,window_days;
CREATE INDEX ON runner_window (scope_id,window_days,median_seconds);

CREATE TABLE scope_tag AS
SELECT s.scope_id,t.tag,'explicit_source_label'::text AS classification_method,
       s.category_name || ' ' || s.values_name AS source_label
FROM scope s
CROSS JOIN LATERAL (VALUES
    ('Any%', '(\many\s*%)'), ('100%', '(100\s*%)'), ('Low%', '(\mlow\s*%)'),
    ('All Bosses', '(all\s+bosses)'), ('Glitchless', '(glitchless)'),
    ('No Major Glitches', '(no\s+major\s+glitches|\mNMG\M)')
) t(tag,pattern)
WHERE s.category_name || ' ' || s.values_name ~* t.pattern;
CREATE UNIQUE INDEX ON scope_tag (scope_id,tag);

CREATE TABLE runner_profile AS
WITH games AS (
    SELECT p.holder_id,p.holder_name,s.game_id,t.tag,max(p.strength) AS strength,
           count(*) AS scope_count
    FROM pb p JOIN scope s USING (scope_id) JOIN scope_tag t USING (scope_id)
    WHERE p.field_size>=10 AND s.field_platform='all'
    GROUP BY p.holder_id,p.holder_name,s.game_id,t.tag
)
SELECT holder_id,holder_name,tag,avg(strength) AS strength,count(*) AS game_count,sum(scope_count) AS scope_count
FROM games GROUP BY holder_id,holder_name,tag;
CREATE INDEX ON runner_profile (holder_id,tag);

CREATE TABLE first_publication AS
SELECT DISTINCT ON (r.scope_id,r.holder_id)
       r.scope_id,r.holder_id,r.holder_name,r.submitted_at AS first_at,r.score AS first_score,r.seconds AS first_seconds
FROM run r WHERE r.submitted_at IS NOT NULL AND r.submitted_at<=(SELECT as_of FROM context)
ORDER BY r.scope_id,r.holder_id,r.submitted_at,r.score,r.run_id;
CREATE UNIQUE INDEX ON first_publication (scope_id,holder_id);

CREATE TABLE cohort_member AS
SELECT f.*,t.target_code,t.target_n,t.threshold_score,t.threshold_seconds,
       extract(year FROM f.first_at)::integer AS entry_year,
       floor(100.0*(f.first_score-t.threshold_score)/abs(t.threshold_score)/10)::integer AS start_gap_bin,
       f.first_score<=t.threshold_score AS already_at_target,
       hit.reached_at,extract(epoch FROM hit.reached_at-f.first_at)/86400.0 AS days_to_target,
       extract(epoch FROM (SELECT as_of FROM context)-f.first_at)/86400.0 AS followup_days
FROM first_publication f JOIN target t USING (scope_id)
LEFT JOIN LATERAL (
    SELECT min(r.submitted_at) AS reached_at FROM run r
    WHERE r.scope_id=f.scope_id AND r.holder_id=f.holder_id AND r.score<=t.threshold_score
      AND r.submitted_at>=f.first_at AND r.submitted_at<=(SELECT as_of FROM context)
) hit ON true
WHERE t.sufficient_field AND t.threshold_score<>0;
CREATE INDEX ON cohort_member (scope_id,target_code,entry_year,start_gap_bin);
CREATE INDEX ON cohort_member (holder_id,scope_id,target_code);

CREATE TABLE cohort_summary AS
WITH counts AS (
    SELECT scope_id,target_code,entry_year,start_gap_bin,h.horizon_days,
           count(*) FILTER (WHERE followup_days>=h.horizon_days) AS observed_people,
           count(*) FILTER (WHERE followup_days>=h.horizon_days AND days_to_target<=h.horizon_days) AS reached_people
    FROM cohort_member CROSS JOIN (VALUES (30),(90),(365)) h(horizon_days)
    WHERE NOT already_at_target AND target_code<>'top25pct'
    GROUP BY scope_id,target_code,entry_year,start_gap_bin,h.horizon_days
)
SELECT *,CASE WHEN observed_people>=30 THEN 100.0*reached_people/observed_people END AS reached_pct
FROM counts;

COMMENT ON TABLE cohort_member IS 'Calendar time from the first visible verified submission to the fixed target in the archive snapshot. Current verification status is retrospective.';
COMMENT ON TABLE runner_profile IS 'Mean of the best per-game midrank percentiles, over fields with at least ten holders. Tags can overlap.';
