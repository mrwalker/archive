-- A subset of Snowplow's event model
-- https://github.com/snowplow/snowplow/blob/master/4-storage/hive-storage/hiveql/table-def.q
(
  app_id string,
  collector_tstamp timestamp,
  dvce_tstamp timestamp,
  event string,
  event_id string,
  domain_userid string,
  domain_sessionidx smallint,
  page_title string,
  page_urlscheme string,
  page_urlhost string,
  page_urlport int, 
  page_urlpath string,
  page_urlquery string,
  page_urlfragment string
)
PARTITIONED BY (run string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${EVENTS_TABLE}'
TBLPROPERTIES ('serialization.null.format'='')
