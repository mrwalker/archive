SELECT
  *
FROM
  {{inputs.stage_dynamo_result_stats}}
DISTRIBUTE BY
  CAST(rand() * 2 AS INT)
