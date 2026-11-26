CREATE TABLE activity AS
SELECT f.scope_id,w.window_days,
       coalesce(e.wr_improvements,0) AS wr_improvements,
       coalesce(e.wr_authors,0) AS wr_authors,coalesce(e.wr_days,0) AS wr_days,
       coalesce(e.top10_authors,0) AS top10_authors,coalesce(e.top10_pbs,0) AS top10_pbs,
       coalesce(e.top10_entries,0) AS top10_entries,e.last_wr_at,
       coalesce(a.active_rivals,0) AS active_rivals,f.field_size,f.top10_gap_pct,
       rank() OVER (PARTITION BY w.window_days,(f.field_platform='all') ORDER BY coalesce(e.wr_authors,0) DESC,coalesce(e.wr_improvements,0) DESC) AS wr_race_rank,
       rank() OVER (PARTITION BY w.window_days,(f.field_platform='all') ORDER BY coalesce(e.top10_authors,0) DESC,f.top10_gap_pct ASC NULLS LAST) AS top10_race_rank
FROM field f CROSS JOIN (VALUES (7),(30),(90)) w(window_days)
LEFT JOIN LATERAL (
    SELECT count(DISTINCT event_id) FILTER (WHERE event_kind='wr' AND previous_seconds IS NOT NULL) AS wr_improvements,
           count(DISTINCT h.id) FILTER (WHERE event_kind='wr' AND previous_seconds IS NOT NULL) AS wr_authors,
           count(DISTINCT event_at::date) FILTER (WHERE event_kind='wr' AND previous_seconds IS NOT NULL) AS wr_days,
           count(DISTINCT h.id) FILTER (WHERE event_kind='top10_pb') AS top10_authors,
           count(DISTINCT event_id) FILTER (WHERE event_kind='top10_pb') AS top10_pbs,
           count(DISTINCT event_id) FILTER (WHERE event_kind='place_change' AND place<=10 AND previous_place IS NULL) AS top10_entries,
           max(event_at) FILTER (WHERE event_kind='wr') AS last_wr_at
    FROM record_event e CROSS JOIN LATERAL unnest(e.holder_ids) h(id)
    WHERE e.scope_id=f.scope_id AND e.time_basis='performed_at'
      AND e.event_at > (SELECT as_of FROM context)-w.window_days*interval '1 day'
) e ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS active_rivals FROM pb p WHERE p.scope_id=f.scope_id AND p.gap_pct<=1
      AND EXISTS (SELECT 1 FROM run r WHERE r.scope_id=p.scope_id AND r.holder_id=p.holder_id
                  AND r.submitted_at > (SELECT as_of FROM context)-w.window_days*interval '1 day')
) a ON true;
CREATE UNIQUE INDEX ON activity (scope_id,window_days);

CREATE TABLE wr_opportunity AS
SELECT p.scope_id,p.holder_id,p.holder_name,p.seconds AS pb_seconds,p.wr_seconds,p.gap_seconds,p.gap_pct,
       p.place,p.field_size,a.active_rivals,
       rank() OVER (PARTITION BY p.holder_id ORDER BY p.gap_pct,a.active_rivals,p.seconds) AS opportunity_rank
FROM pb p JOIN activity a USING (scope_id) JOIN scope s USING(scope_id)
WHERE a.window_days=30 AND s.field_platform='all';
CREATE INDEX ON wr_opportunity (holder_id,opportunity_rank);

CREATE TABLE revival AS
SELECT e.event_id,e.scope_id,e.event_at,e.previous_record_at,e.previous_seconds,e.seconds,
       e.improvement_pct,extract(epoch FROM e.event_at-e.previous_record_at)/86400.0 AS plateau_days,
       coalesce(a.before_authors,0) AS before_authors,coalesce(a.same_day_authors,0) AS same_day_authors,
       coalesce(a.after_authors,0) AS after_authors,
       e.event_at+interval '30 days'<=(SELECT as_of FROM context) AS complete_after_window,
       e.run_ids,e.previous_run_ids,
       CASE WHEN coalesce(a.before_authors,0)>=3 THEN 'Движение до WR'
            WHEN coalesce(a.after_authors,0)>=3 THEN 'Волна после WR' ELSE 'Отдельное улучшение' END AS episode_kind
FROM record_event e
LEFT JOIN LATERAL (
    SELECT count(DISTINCT holder_ids[1]) FILTER (WHERE event_at<e.event_at::date) AS before_authors,
           count(DISTINCT holder_ids[1]) FILTER (WHERE event_at::date=e.event_at::date) AS same_day_authors,
           count(DISTINCT holder_ids[1]) FILTER (WHERE event_at::date>e.event_at::date) AS after_authors
    FROM record_event p WHERE p.scope_id=e.scope_id AND p.time_basis=e.time_basis AND p.event_kind='top10_pb'
      AND p.event_at>=e.event_at::date-interval '30 days' AND p.event_at<e.event_at::date+interval '31 days'
) a ON true
WHERE e.event_kind='wr' AND e.time_basis='performed_at' AND e.previous_record_at IS NOT NULL;
CREATE UNIQUE INDEX ON revival (event_id);

CREATE TABLE calendar_activity AS
SELECT scope_id,event_at::date AS event_date,
       count(*) FILTER (WHERE event_kind='wr' AND previous_seconds IS NOT NULL) AS wr_improvements,
       count(DISTINCT holder_ids[1]) FILTER (WHERE event_kind='top10_pb') AS top10_authors,
       count(*) FILTER (WHERE event_kind='place_change') AS position_changes
FROM record_event WHERE time_basis='performed_at' GROUP BY scope_id,event_at::date;

COMMENT ON TABLE record_event IS 'Retrospective reconstruction from runs verified in the selected archive. Simultaneous results are applied together.';
COMMENT ON TABLE revival IS 'Candidate episodes around a WR improvement. Thresholds describe evidence for inspection; strategy attribution requires a dated source.';
