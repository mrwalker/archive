SELECT
  app_id,
  'impression' AS event,
  day,
  hour,
  CAST(1 AS BIGINT) AS metric,
  event_id,
  domain_userid,
  domain_sessionidx,
  collector_tstamp,
  dvce_tstamp,
  -- Optional keys (nullable)
  result_id,
  category AS search_category,
  page_number AS search_page_number
FROM
  {{inputs.searches}} searches
LATERAL VIEW
  explode(SPLIT(s.result_ids, ',')) results AS result_id
