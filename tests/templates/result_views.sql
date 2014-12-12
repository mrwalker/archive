SELECT
  app_id,
  event,
  day,
  hour,
  metric,
  event_id,
  domain_userid,
  domain_sessionidx,
  collector_tstamp,
  dvce_tstamp,
  -- Optional keys (nullable)
  result_id,
  category
FROM (
  SELECT
    app_id,
    se_category AS category,
    se_label AS result_id,
    'result_view' AS event,
    to_date(collector_tstamp) AS day,
    lpad(hour(collector_tstamp), 2, '0') AS hour,
    CAST(1 AS BIGINT) AS metric,
    event_id,
    domain_userid,
    domain_sessionidx,
    collector_tstamp,
    dvce_tstamp
  FROM
    {{inputs.partitioned_events}}
  WHERE
    event = 'result_view'
    AND se_label IS NOT NULL
    AND se_label != 'null'
    AND se_category IS NOT NULL
    AND se_category != 'null'
) views
