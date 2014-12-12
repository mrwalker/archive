(
  app_result string,
  event string,
  by_hour array<string>
)
STORED BY 'com.willetinc.hive.mapreduce.dynamodb.HiveDynamoDBStorageHandler'
TBLPROPERTIES (
  "dynamodb.table.name" = "result_stats",
  "dynamodb.column.mapping" = "app_result:app_result,event:event,by_hour:by_hour"
)
