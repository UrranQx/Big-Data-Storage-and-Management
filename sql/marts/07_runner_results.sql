-- History has been reconstructed and audited before this step.
CREATE TABLE runner_result AS
WITH quality AS (
 SELECT p.*,f.density,f.history_status,
    CASE WHEN p.solo_field_size>1 THEN
      (p.solo_field_size-1)::double precision/(p.solo_field_size-1+20)
      * greatest(0.0,1-ln((p.solo_place+(p.solo_tie_size-1)/2.0)::double precision)
                          /ln(p.solo_field_size::double precision)) END AS q,
    CASE WHEN f.history_status='known' THEN coalesce(t.quality_days,0) END AS quality_days,
    CASE WHEN f.history_status='known' THEN coalesce(t.days_first,0) END AS days_first,
    CASE WHEN f.history_status='known' THEN coalesce(t.days_second,0) END AS days_second,
    CASE WHEN f.history_status='known' THEN coalesce(t.days_third,0) END AS days_third
 FROM _ranking_pb p JOIN runner_field f USING(scope_id)
 LEFT JOIN runner_tenure t USING(scope_id,holder_id)
), components AS (
 SELECT *, (100*q*q)::numeric(20,9) AS quality_points,
    (100*q*q*.25*density::double precision
       *exp(-greatest(0.0,(solo_place+(solo_tie_size-1)/2.0-25)::double precision)/25))::numeric(20,9) AS density_points,
    (25*quality_days/(quality_days+365))::numeric(20,9) AS tenure_points
 FROM quality
)
SELECT scope_id,holder_id,run_id,score,seconds,solo_place,solo_field_size,solo_tie_size,wr_value,
       100.0*(solo_field_size-solo_place-(solo_tie_size-1)/2.0)/nullif(solo_field_size-1,0) AS percentile,
       100.0*abs(seconds-wr_value)/wr_value AS wr_gap_pct,
       quality_points,density_points,tenure_points,
       (quality_points+density_points+coalesce(tenure_points,0))::numeric(20,9) AS result_points,
       history_status,quality_days,days_first,days_second,days_third
FROM components;
ALTER TABLE runner_result ADD PRIMARY KEY(scope_id,holder_id);
CREATE INDEX ON runner_result(holder_id);
COMMENT ON TABLE runner_tenure IS 'Retrospective verified_at solo podium tenure. Lifetime quality-weighted days survive loss of position; complete dates and final PB equality required.';
COMMENT ON TABLE runner_field IS 'Top-10/25 snapshot density, not current activity. Unknown history is explicit, never treated as measured zero.';
COMMENT ON TABLE runner_result IS 'Quality squared + bounded density bonus + retained lifetime podium points. Policy k=20, alpha=.25, tenure cap=25, half=365. Scores quantized to 9 decimal places.';
