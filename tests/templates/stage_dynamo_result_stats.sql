SELECT
  concat(app_id, '#', result_id) AS app_result,
  event,
  collect_set(concat(day, '#', hour, '#', agg_metric)) AS data
FROM (
  SELECT
    app_id,
    result_id,
    event,
    day,
    hour,
    SUM(metric) AS agg_metric
  FROM
    {{inputs.impressions}}
  WHERE
    app_id IS NOT NULL
    AND result_id IS NOT NULL
    AND event IS NOT NULL
    AND day IS NOT NULL
    AND hour IS NOT NULL
    AND metric IS NOT NULL
  GROUP BY
    app_id,
    result_id,
    event,
    day,
    hour
UNION ALL
  SELECT
    app_id,
    result_id,
    event,
    day,
    hour,
    SUM(metric) AS agg_metric
  FROM
    {{inputs.result_views}}
  WHERE
    app_id IS NOT NULL
    AND result_id IS NOT NULL
    AND event IS NOT NULL
    AND day IS NOT NULL
    AND hour IS NOT NULL
    AND metric IS NOT NULL
  GROUP BY
    app_id,
    result_id,
    event,
    day,
    hour
) unioned
GROUP BY
  app_id,
  result_id,
  event
