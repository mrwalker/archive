SELECT
  app_id,
  event,
  to_date(collector_tstamp) AS day,
  lpad(hour(collector_tstamp), 2, '0') AS hour,
  event_id,
  domain_userid,
  domain_sessionidx,
  collector_tstamp,
  dvce_tstamp,
  se_label AS result_ids,
  se_category AS category,
  se_property AS result_count,
  se_value AS page_number
FROM
  {{inputs.partitioned_events}}
WHERE
  event = 'search'
  AND se_label IS NOT NULL
  AND se_label != 'null'
