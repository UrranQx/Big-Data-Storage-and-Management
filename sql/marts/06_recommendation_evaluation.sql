-- Temporal evaluation uses only game-entry dates visible before train_until.
-- It is a retrospective archive test, not a prospective skill prediction.
CREATE TEMP TABLE _train_cutoff ON COMMIT DROP AS
SELECT as_of-interval '365 days' AS train_until,as_of AS test_until FROM context;
CREATE TEMP TABLE _train_portfolio ON COMMIT DROP AS
SELECT holder_id,game_id,first_at FROM game_portfolio
WHERE first_at<=(SELECT train_until FROM _train_cutoff);
CREATE UNIQUE INDEX ON _train_portfolio(holder_id,game_id);
CREATE INDEX ON _train_portfolio(game_id,first_at);
CREATE TEMP TABLE _train_games ON COMMIT DROP AS
SELECT game_id,count(*) AS people FROM _train_portfolio GROUP BY game_id;
CREATE TEMP TABLE _train_first ON COMMIT DROP AS
SELECT holder_id,min(first_at) AS first_at FROM _train_portfolio GROUP BY holder_id;

CREATE TEMP TABLE _train_baseline ON COMMIT DROP AS
SELECT g.game_id,
  count(*) FILTER(WHERE b.first_at IS NULL OR b.first_at>a.first_at) AS potential,
  count(*) FILTER(WHERE b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days') AS transitioned
FROM _train_first a CROSS JOIN _train_games g
LEFT JOIN _train_portfolio b ON b.holder_id=a.holder_id AND b.game_id=g.game_id
WHERE a.first_at<=(SELECT train_until FROM _train_cutoff)-interval '365 days'
GROUP BY g.game_id;

CREATE TEMP TABLE _train_lift ON COMMIT DROP AS
WITH counts AS (
  SELECT a.game_id AS source_game,g.game_id AS destination_game,
    count(*) FILTER(WHERE b.first_at IS NULL OR b.first_at>a.first_at) AS potential,
    count(*) FILTER(WHERE b.first_at>a.first_at AND b.first_at<=a.first_at+interval '365 days') AS transitioned
  FROM _train_portfolio a CROSS JOIN _train_games g
  LEFT JOIN _train_portfolio b ON b.holder_id=a.holder_id AND b.game_id=g.game_id
  WHERE a.game_id<>g.game_id AND a.first_at<=(SELECT train_until FROM _train_cutoff)-interval '365 days'
  GROUP BY a.game_id,g.game_id
)
SELECT c.source_game,c.destination_game,
  ((c.transitioned+1.0)/(c.potential+2.0))/((b.transitioned+1.0)/(b.potential+2.0)) AS lift
FROM counts c JOIN _train_baseline b ON b.game_id=c.destination_game
WHERE c.transitioned>=5 AND c.potential>=30;
CREATE INDEX ON _train_lift(source_game);

CREATE TEMP TABLE _test_users ON COMMIT DROP AS
SELECT holder_id FROM _train_first ORDER BY md5(holder_id),holder_id LIMIT 2000;
CREATE UNIQUE INDEX ON _test_users(holder_id);
CREATE TEMP TABLE _test_truth ON COMMIT DROP AS
SELECT p.holder_id,p.game_id FROM game_portfolio p JOIN _test_users u USING(holder_id)
JOIN _train_games g USING(game_id)
WHERE p.first_at>(SELECT train_until FROM _train_cutoff) AND p.first_at<=(SELECT test_until FROM _train_cutoff);

CREATE TEMP TABLE _test_recommendations ON COMMIT DROP AS
WITH scores AS (
  SELECT 'smoothed_lift'::text AS method,p.holder_id,t.destination_game AS game_id,avg(ln(t.lift)) AS score
  FROM _train_portfolio p JOIN _test_users u USING(holder_id) JOIN _train_lift t ON t.source_game=p.game_id
  WHERE NOT EXISTS(SELECT 1 FROM _train_portfolio k WHERE k.holder_id=p.holder_id AND k.game_id=t.destination_game)
  GROUP BY p.holder_id,t.destination_game
  UNION ALL
  SELECT 'popularity',u.holder_id,g.game_id,g.people::numeric FROM _test_users u CROSS JOIN _train_games g
  WHERE NOT EXISTS(SELECT 1 FROM _train_portfolio k WHERE k.holder_id=u.holder_id AND k.game_id=g.game_id)
), numbered AS (
  SELECT *,row_number() OVER(PARTITION BY method,holder_id ORDER BY score DESC,game_id) AS position FROM scores
)
SELECT * FROM numbered WHERE position<=5;

CREATE TABLE recommendation_evaluation AS
WITH people AS (
  SELECT m.method,u.holder_id,
    EXISTS(SELECT 1 FROM _test_truth t WHERE t.holder_id=u.holder_id) AS has_new_game,
    EXISTS(SELECT 1 FROM _test_recommendations r WHERE r.holder_id=u.holder_id AND r.method=m.method) AS has_recommendations,
    EXISTS(SELECT 1 FROM _test_recommendations r JOIN _test_truth t USING(holder_id,game_id)
           WHERE r.holder_id=u.holder_id AND r.method=m.method) AS hit
  FROM _test_users u CROSS JOIN (VALUES ('smoothed_lift'),('popularity')) m(method)
)
SELECT method,(SELECT train_until FROM _train_cutoff) AS train_until,
  (SELECT test_until FROM _train_cutoff) AS test_until,(SELECT count(*) FROM _train_first) AS eligible_people,
  count(*) AS test_people,count(*) FILTER(WHERE has_new_game) AS people_with_new_game,
  count(*) FILTER(WHERE has_recommendations) AS people_with_recommendations,count(*) FILTER(WHERE hit) AS hits,
  100.0*count(*) FILTER(WHERE hit)/nullif(count(*) FILTER(WHERE has_new_game),0) AS conditional_hit_at_5_pct,
  100.0*count(*) FILTER(WHERE hit)/nullif(count(*),0) AS overall_hit_at_5_pct
FROM people GROUP BY method;
COMMENT ON TABLE recommendation_evaluation IS 'Deterministic sample of up to 2000 eligible runners chosen before inspecting outcomes. Top-5 new-game hit rate over the following 365 days, versus training-period popularity. Archive verification is retrospective.';
