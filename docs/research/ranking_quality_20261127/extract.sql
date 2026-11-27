-- Research only. Run inside a REPEATABLE READ, READ ONLY transaction.
-- Keep the same eligible solo fields as the released ranking.
WITH solo AS MATERIALIZED (
    SELECT p.holder_id,p.scope_id,p.run_id,p.seconds,p.score,
           s.game_id,s.game_name,s.scope_name,s.direction
    FROM speedrun_marts.pb p JOIN speedrun_marts.scope s USING(scope_id)
    WHERE s.field_platform='all' AND cardinality(p.participant_ids)<=1
), places AS (
    SELECT *,rank() OVER (PARTITION BY scope_id ORDER BY score) AS place,
        count(*) OVER (PARTITION BY scope_id) AS n,
        count(*) OVER (PARTITION BY scope_id,score) AS ties,
        first_value(seconds) OVER (PARTITION BY scope_id ORDER BY score) AS wr
    FROM solo
), gaps AS MATERIALIZED (
    SELECT *,abs(ln(seconds/wr))::double precision AS log_gap
    FROM places WHERE n>=10
), scales AS (
    SELECT scope_id,percentile_cont(0.1) WITHIN GROUP(ORDER BY log_gap) AS top_decile_gap
    FROM gaps GROUP BY scope_id
)
SELECT g.*,s.top_decile_gap,p.player_name
FROM gaps g JOIN scales s USING(scope_id)
JOIN speedrun_marts.player p ON p.player_id=g.holder_id
ORDER BY g.scope_id,g.score,g.holder_id
