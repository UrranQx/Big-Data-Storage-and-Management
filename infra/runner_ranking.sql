-- Read-only ranking over the currently published archive. Change the single
-- minimum_field_size value to inspect sensitivity to the minimum field size.
-- The shrinkage constants 5/20/50 are policy choices, not fitted probabilities.
WITH settings(minimum_field_size) AS (VALUES (10)),
solo AS MATERIALIZED (
    SELECT p.holder_id,p.scope_id,p.run_id,p.seconds,p.score,
           s.game_id,s.game_name,s.scope_name,s.direction,s.timer
    FROM speedrun_marts.pb p JOIN speedrun_marts.scope s USING(scope_id)
    -- pb already guarantees nonempty participants. <=1 preserves solo semantics
    -- without PostgreSQL's severe equality-selectivity underestimate for arrays.
    WHERE s.field_platform='all' AND cardinality(p.participant_ids)<=1
),
places AS (
    SELECT *,rank() OVER (PARTITION BY scope_id ORDER BY score) AS solo_place,
           count(*) OVER (PARTITION BY scope_id) AS solo_field_size,
           count(*) OVER (PARTITION BY scope_id,score) AS solo_tie_size,
           first_value(seconds) OVER (PARTITION BY scope_id ORDER BY score) AS wr_value
    FROM solo
),
relative AS MATERIALIZED (
    SELECT *,solo_field_size-solo_place-(solo_tie_size-1)/2.0 AS opponents_credit,
           100.0*abs(seconds-wr_value)/wr_value AS wr_gap_pct
    FROM places
),
fields AS (
    SELECT scope_id,count(*) FILTER(WHERE wr_gap_pct<=1) AS near_wr_people,
           CASE WHEN count(*)>=10 THEN max(wr_gap_pct) FILTER(WHERE solo_place<=10) END AS leader_top10_gap_pct,
           percentile_cont(0.5) WITHIN GROUP(ORDER BY seconds) AS field_median_value
    FROM relative GROUP BY scope_id
),
scored AS MATERIALIZED (
    SELECT r.*,100.0*opponents_credit/(solo_field_size-1) AS percentile,
           100.0*opponents_credit/(solo_field_size-1+20) AS result_points,
           100.0*opponents_credit/(solo_field_size-1+5) AS points_k5,
           100.0*opponents_credit/(solo_field_size-1+50) AS points_k50
    FROM relative r CROSS JOIN settings
    WHERE solo_field_size>=greatest(2,minimum_field_size)
),
ordered_games AS (
    SELECT DISTINCT ON (holder_id,game_id) *
    FROM scored
    ORDER BY holder_id,game_id,result_points DESC,scope_id
),
best_games AS (
    SELECT *,row_number() OVER(PARTITION BY holder_id ORDER BY result_points DESC,game_id) AS in_portfolio
    FROM ordered_games
),
portfolios AS (
    SELECT holder_id,count(*) AS eligible_games,
           sum(result_points) FILTER(WHERE in_portfolio<=10) AS portfolio_points,
           string_agg(game_name || ' [' || round(result_points,2)::text || ']', '; ' ORDER BY in_portfolio)
               FILTER(WHERE in_portfolio<=10) AS portfolio_games
    FROM best_games GROUP BY holder_id
),
medals AS (
    SELECT holder_id,count(*) AS eligible_fields,
           count(*) FILTER(WHERE solo_place=1) AS calculated_gold,
           count(*) FILTER(WHERE solo_place=2) AS calculated_silver,
           count(*) FILTER(WHERE solo_place=3) AS calculated_bronze,
           max(points_k5) AS best_k5,max(points_k50) AS best_k50
    FROM scored GROUP BY holder_id
),
ranked AS (
    SELECT b.*,p.eligible_games,p.portfolio_points,p.portfolio_games,
           m.eligible_fields,m.calculated_gold,m.calculated_silver,m.calculated_bronze,
           rank() OVER(ORDER BY b.result_points DESC) AS best_rank,
           rank() OVER(ORDER BY p.portfolio_points DESC) AS portfolio_rank,
           rank() OVER(ORDER BY m.best_k5 DESC) AS rank_k5,
           rank() OVER(ORDER BY m.best_k50 DESC) AS rank_k50,
           rank() OVER(ORDER BY m.calculated_gold DESC,m.calculated_silver DESC,m.calculated_bronze DESC) AS calculated_medal_rank
    FROM best_games b JOIN portfolios p USING(holder_id) JOIN medals m USING(holder_id)
    WHERE b.in_portfolio=1
),
participation AS (
    SELECT holder_id,count(*) AS solo_fields FROM solo GROUP BY holder_id
)
SELECT c.build_id,c.as_of,p.player_id AS holder_id,p.player_name,
       p.player_name || ' [' || p.player_id || ']' AS runner,
       CASE WHEN r.holder_id IS NOT NULL THEN 'Ранжирован'
            WHEN a.holder_id IS NOT NULL THEN 'Поле меньше порога'
            ELSE 'Нет текущего сольного PB в витринах' END AS ranking_status,
       greatest(2,settings.minimum_field_size) AS minimum_field_size,
       r.best_rank,r.result_points,r.portfolio_rank,r.portfolio_points,r.portfolio_games,
       r.rank_k5,r.rank_k50,r.calculated_medal_rank,
       coalesce(r.calculated_gold,0) AS calculated_gold,
       coalesce(r.calculated_silver,0) AS calculated_silver,
       coalesce(r.calculated_bronze,0) AS calculated_bronze,
       coalesce(a.solo_fields,0) AS solo_fields,coalesce(r.eligible_fields,0) AS eligible_fields,
       coalesce(r.eligible_games,0) AS eligible_games,
       r.game_name,r.scope_name,r.solo_place,r.solo_field_size,r.solo_tie_size,r.percentile,
       r.seconds AS result_value,r.wr_value,r.wr_gap_pct,r.timer,
       CASE r.direction WHEN 1 THEN 'Меньше лучше' WHEN -1 THEN 'Больше лучше (время или очки)' END AS comparison_direction,
       f.near_wr_people,f.leader_top10_gap_pct,
       CASE WHEN r.direction=1 THEN f.field_median_value END AS completion_median_seconds,
       'https://www.speedrun.com/runs/' || r.run_id AS run_url
FROM speedrun_marts.player p
LEFT JOIN ranked r ON r.holder_id=p.player_id
LEFT JOIN participation a ON a.holder_id=p.player_id
LEFT JOIN fields f ON f.scope_id=r.scope_id
CROSS JOIN settings CROSS JOIN speedrun_marts.context c
