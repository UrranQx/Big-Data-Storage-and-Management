-- Current solo PBs. Platform copies and teams cannot inflate the score.
CREATE TEMP TABLE _ranking_pb ON COMMIT DROP AS
SELECT p.scope_id,p.holder_id,p.run_id,p.score,p.seconds,
       rank() OVER (PARTITION BY p.scope_id ORDER BY p.score) AS solo_place,
       row_number() OVER (PARTITION BY p.scope_id ORDER BY p.score,p.holder_id) AS position,
       count(*) OVER (PARTITION BY p.scope_id) AS solo_field_size,
       count(*) OVER (PARTITION BY p.scope_id,p.score) AS solo_tie_size,
       first_value(p.seconds) OVER (PARTITION BY p.scope_id ORDER BY p.score) AS wr_value
FROM pb p JOIN scope s USING(scope_id)
WHERE s.field_platform='all' AND cardinality(p.participant_ids)<=1;
CREATE UNIQUE INDEX ON _ranking_pb(scope_id,holder_id);
ANALYZE _ranking_pb;

CREATE TABLE runner_field AS
WITH metrics AS (
 SELECT scope_id,max(solo_field_size) AS solo_field_size,min(wr_value) AS wr_value,
   count(*) FILTER(WHERE abs(seconds-wr_value)/wr_value<=0.01) AS near_wr_people,
   percentile_cont(.5) WITHIN GROUP(ORDER BY seconds) AS field_median_value,
   CASE WHEN count(*)>=10 THEN max(abs(ln(seconds/wr_value))) FILTER(WHERE position<=10) END AS spread10,
   CASE WHEN count(*)>=25 THEN max(abs(ln(seconds/wr_value))) FILTER(WHERE position<=25) END AS spread25,
   CASE WHEN count(*)>=10 THEN (count(DISTINCT score) FILTER(WHERE position<=10)-1)/9.0 END AS distinct10,
   CASE WHEN count(*)>=25 THEN (count(DISTINCT score) FILTER(WHERE position<=25)-1)/24.0 END AS distinct25
 FROM _ranking_pb GROUP BY scope_id
)
SELECT *,100*(exp(spread10)-1) AS leader_top10_gap_pct,
         100*(exp(spread25)-1) AS leader_top25_gap_pct,
         (coalesce(distinct10*ln(1.01)/(ln(1.01)+spread10),0)
          +coalesce(distinct25*ln(1.01)/(ln(1.01)+spread25),0))/2 AS density,
         CASE WHEN solo_field_size<10 THEN 'small_field' ELSE 'pending' END::text AS history_status,
         0::bigint AS missing_dates,0::bigint AS future_dates,
         0::bigint AS mismatched_players,0::bigint AS extra_players
FROM metrics;
ALTER TABLE runner_field ADD PRIMARY KEY(scope_id);

CREATE TABLE runner_tenure (
 scope_id text NOT NULL,holder_id text NOT NULL,
 quality_days numeric NOT NULL CHECK(quality_days>=0),
 days_first numeric NOT NULL,days_second numeric NOT NULL,days_third numeric NOT NULL,
 episodes integer NOT NULL,self_improvements integer NOT NULL,
 PRIMARY KEY(scope_id,holder_id)
);
