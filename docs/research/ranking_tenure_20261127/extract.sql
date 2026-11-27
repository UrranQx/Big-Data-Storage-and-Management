-- Read-only historical input, including obsolete runs. Same solo field policy.
WITH fields AS (
 SELECT p.scope_id FROM speedrun_marts.pb p
 JOIN speedrun_marts.scope s USING(scope_id)
 WHERE s.field_platform='all' AND cardinality(p.participant_ids)<=1
 GROUP BY p.scope_id HAVING count(*)>=10
)
SELECT r.scope_id,r.holder_id,r.run_id,r.score,
       r.performed_at,r.submitted_at,r.verified_at,r.source_obsolete
FROM speedrun_marts.run r JOIN fields f USING(scope_id)
WHERE cardinality(r.participant_ids)<=1
ORDER BY r.scope_id,r.run_id
