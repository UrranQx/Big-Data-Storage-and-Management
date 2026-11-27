-- Current-build scores; the runner filter is applied AFTER global ranking.
WITH settings(minimum_field_size) AS (VALUES (10)),
scored AS MATERIALIZED (
 SELECT r.*,s.game_id,s.game_name,s.scope_name,s.direction,s.timer
 FROM speedrun_marts.runner_result r JOIN speedrun_marts.scope s USING(scope_id)
 CROSS JOIN settings WHERE solo_field_size>=greatest(2,minimum_field_size)
),
ordered_games AS (
 SELECT DISTINCT ON(holder_id,game_id) * FROM scored
 ORDER BY holder_id,game_id,result_points DESC,scope_id
),
best_games AS (
 SELECT *,row_number() OVER(PARTITION BY holder_id ORDER BY result_points DESC,game_id) AS game_order
 FROM ordered_games
),
portfolios AS (
 SELECT holder_id,count(*) AS eligible_games,
   round(sum(result_points*power(.9::numeric,game_order-1)),9) AS portfolio_points,
   round(sum(quality_points*power(.9::numeric,game_order-1)),9) AS portfolio_quality,
   round(sum(density_points*power(.9::numeric,game_order-1)),9) AS portfolio_density,
   round(sum(coalesce(tenure_points,0)*power(.9::numeric,game_order-1)),9) AS portfolio_tenure,
   count(*) FILTER(WHERE history_status<>'known') AS unknown_history_games,
   string_agg(game_name || ' [' || round(result_points*power(.9::numeric,game_order-1),2)::text || ']',
              '; ' ORDER BY game_order) AS portfolio_games
 FROM best_games GROUP BY holder_id
),
medals AS (
 SELECT holder_id,count(*) AS eligible_fields,
   count(*) FILTER(WHERE solo_place=1) AS calculated_gold,
   count(*) FILTER(WHERE solo_place=2) AS calculated_silver,
   count(*) FILTER(WHERE solo_place=3) AS calculated_bronze
 FROM scored GROUP BY holder_id
),
ranked AS (
 SELECT b.*,p.eligible_games,p.portfolio_points,p.portfolio_games,
   p.portfolio_quality,p.portfolio_density,p.portfolio_tenure,p.unknown_history_games,
   m.eligible_fields,m.calculated_gold,m.calculated_silver,m.calculated_bronze,
   rank() OVER(ORDER BY b.result_points DESC) AS best_rank,
   rank() OVER(ORDER BY p.portfolio_points DESC) AS portfolio_rank,
   rank() OVER(ORDER BY m.calculated_gold DESC,m.calculated_silver DESC,m.calculated_bronze DESC) AS calculated_medal_rank
 FROM best_games b JOIN portfolios p USING(holder_id) JOIN medals m USING(holder_id)
 WHERE b.game_order=1
),
participation AS (
 SELECT holder_id,count(*) AS solo_fields FROM speedrun_marts.runner_result GROUP BY holder_id
)
SELECT c.build_id,c.as_of,p.player_id AS holder_id,p.player_name,
 p.player_name || ' [' || p.player_id || ']' AS runner,
 CASE WHEN r.holder_id IS NOT NULL THEN 'Ранжирован'
      WHEN a.holder_id IS NOT NULL THEN 'Поле меньше порога'
      ELSE 'Нет текущего сольного PB в витринах' END AS ranking_status,
 greatest(2,settings.minimum_field_size) AS minimum_field_size,
 r.best_rank,r.result_points,r.portfolio_rank,r.portfolio_points,r.portfolio_games,
 r.portfolio_quality,r.portfolio_density,r.portfolio_tenure,r.unknown_history_games,
 r.quality_points,r.density_points,r.tenure_points,
 CASE WHEN r.holder_id IS NULL THEN NULL WHEN r.history_status='known' THEN 'История согласована со срезом'
      ELSE 'История неизвестна — бонус не начислен' END AS history_note,
 r.quality_days,r.days_first,r.days_second,r.days_third,
 r.calculated_medal_rank,
 coalesce(r.calculated_gold,0) AS calculated_gold,coalesce(r.calculated_silver,0) AS calculated_silver,
 coalesce(r.calculated_bronze,0) AS calculated_bronze,
 coalesce(a.solo_fields,0) AS solo_fields,coalesce(r.eligible_fields,0) AS eligible_fields,
 coalesce(r.eligible_games,0) AS eligible_games,
 r.game_name,r.scope_name,r.solo_place,r.solo_field_size,r.solo_tie_size,r.percentile,
 r.seconds AS result_value,r.wr_value,r.wr_gap_pct,r.timer,
 CASE r.direction WHEN 1 THEN 'Меньше лучше' WHEN -1 THEN 'Больше лучше (время или очки)' END AS comparison_direction,
 f.near_wr_people,f.leader_top10_gap_pct,f.leader_top25_gap_pct,
 CASE r.direction WHEN 1 THEN f.field_median_value END AS completion_median_seconds,
 'https://www.speedrun.com/runs/' || r.run_id AS run_url
FROM speedrun_marts.player p LEFT JOIN ranked r ON r.holder_id=p.player_id
LEFT JOIN participation a ON a.holder_id=p.player_id
LEFT JOIN speedrun_marts.runner_field f ON f.scope_id=r.scope_id
CROSS JOIN settings CROSS JOIN speedrun_marts.context c
