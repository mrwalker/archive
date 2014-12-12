-- A subset of Snowplow's event model
-- https://github.com/snowplow/snowplow/blob/master/4-storage/hive-storage/hiveql/table-def.q
(
  app_id string,
  collector_tstamp timestamp,
  dvce_tstamp timestamp,
  event_id string,
  domain_userid string,
  domain_sessionidx smallint,
  page_url string, -- Added in 0.2.0
  page_title string,
  page_referrer string, -- Added in 0.2.0
  page_urlscheme string,
  page_urlhost string,
  page_urlport int,
  page_urlpath string,
  page_urlquery string,
  page_urlfragment string,
  refr_urlscheme string,
  refr_urlhost string,
  refr_urlport int,
  refr_urlpath string,
  refr_urlquery string,
  refr_urlfragment string,
  se_category string,
  se_action string,
  se_label string,
  se_property string,
  se_value double
)
PARTITIONED BY (run string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${EVENTS_TABLE}'
TBLPROPERTIES ('serialization.null.format'='')
