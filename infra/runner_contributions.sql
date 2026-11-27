-- Same best-per-game and eligibility policy as runner_ranking.sql.
WITH settings(minimum_field_size) AS (VALUES (10)),
games AS (
 SELECT DISTINCT ON(r.holder_id,s.game_id) r.*,s.game_id,s.game_name,s.scope_name
 FROM speedrun_marts.runner_result r JOIN speedrun_marts.scope s USING(scope_id)
 CROSS JOIN settings WHERE solo_field_size>=greatest(2,minimum_field_size)
 ORDER BY r.holder_id,s.game_id,r.result_points DESC,r.scope_id
), ordered AS (
 SELECT *,row_number() OVER(PARTITION BY holder_id ORDER BY result_points DESC,game_id) AS game_order FROM games
)
SELECT c.build_id,c.as_of,p.player_name || ' [' || p.player_id || ']' AS runner,
 o.holder_id,o.game_id,o.game_order,o.game_name,o.scope_name,o.solo_place,o.solo_field_size,
 o.quality_points,o.density_points,o.tenure_points,o.result_points,
 power(.9::numeric,o.game_order-1) AS game_weight,
 round(o.result_points*power(.9::numeric,o.game_order-1),9) AS weighted_points,
 o.days_first,o.days_second,o.days_third,o.quality_days,
 CASE WHEN o.history_status='known' THEN 'История согласована со срезом'
      ELSE 'История неизвестна — бонус не начислен' END AS history_note
FROM ordered o JOIN speedrun_marts.player p ON p.player_id=o.holder_id
CROSS JOIN speedrun_marts.context c
