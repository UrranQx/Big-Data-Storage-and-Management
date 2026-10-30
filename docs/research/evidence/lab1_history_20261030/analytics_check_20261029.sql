-- Read-only ODS analysis; temporary views live only in the current connection.
-- Countries use the public profile areaId prefix; no nationality is inferred.
CREATE TEMP VIEW audit_seen AS
SELECT document->>'dataset' dataset, document->>'source_id' source_id,
       document->>'projection' projection,
       document->'last_seen'->>'version_key' version_key,
       document->'last_seen'->>'observed_at' observed_at
FROM entity_v2.checks
WHERE document->>'snapshot_id' LIKE 'lab1-full-20261029-%';

CREATE TEMP VIEW audit_profiles AS
SELECT DISTINCT ON (p.source_id) p.source_id,
       (p.document->'raw'->>'text')::jsonb raw,
       lower(nullif(split_part((p.document->'raw'->>'text')::jsonb->>'areaId','/',1),'')) country
FROM entity_v2.players p JOIN audit_seen s ON s.dataset='players' AND s.version_key=p.key
ORDER BY p.source_id,s.observed_at DESC,p.key;

CREATE TEMP VIEW audit_runs AS
SELECT DISTINCT ON (r.source_id) r.source_id,r.game_id,r.category_id,
       (r.document->'raw'->>'text')::jsonb raw
FROM entity_v2.runs r JOIN audit_seen s ON s.dataset='runs' AND s.version_key=r.key
WHERE r.projection='leaderboard_run'
ORDER BY r.source_id,s.observed_at DESC,r.key;

CREATE TEMP VIEW audit_firsts AS
SELECT * FROM audit_runs
WHERE raw->>'place'='1' AND raw->>'verified'='1'
  AND COALESCE(raw->>'obsolete','false')!='true'
  AND COALESCE(raw->>'orphaned','false')!='true';

CREATE TEMP VIEW audit_first_players AS
SELECT DISTINCT r.source_id run_id,r.game_id,r.category_id,p.value player_id
FROM audit_firsts r CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(r.raw->'playerIds','[]')) p;

CREATE TEMP VIEW audit_historical_wr AS
SELECT DISTINCT r.source_id run_id,p.value player_id
FROM entity_v2.runs r JOIN audit_seen s ON s.dataset='runs' AND s.version_key=r.key
CROSS JOIN LATERAL
  jsonb_array_elements_text(COALESCE((r.document->'raw'->>'text')::jsonb->'playerIds','[]')) p
WHERE r.projection='source_wr_run';

-- Profiles by country, one registered source ID once.
SELECT country,count(*) profiles FROM audit_profiles GROUP BY country ORDER BY profiles DESC;
-- Distinct first-place runs credited once per participant country.
SELECT p.country,count(DISTINCT f.run_id) first_place_runs
FROM audit_first_players f LEFT JOIN audit_profiles p ON p.source_id=f.player_id
GROUP BY p.country ORDER BY first_place_runs DESC;
-- Observed first-place runs by player, not a claim about all site WRs.
SELECT f.player_id,p.raw->>'name' name,p.country,count(DISTINCT f.run_id) first_place_runs
FROM audit_first_players f LEFT JOIN audit_profiles p ON p.source_id=f.player_id
GROUP BY f.player_id,p.raw,p.country ORDER BY first_place_runs DESC,f.player_id LIMIT 20;
-- Historical WR run IDs; different from records currently held.
SELECT h.player_id,p.raw->>'name' name,p.country,count(DISTINCT h.run_id) historical_wr_runs
FROM audit_historical_wr h LEFT JOIN audit_profiles p ON p.source_id=h.player_id
GROUP BY h.player_id,p.raw,p.country ORDER BY historical_wr_runs DESC,h.player_id LIMIT 20;
