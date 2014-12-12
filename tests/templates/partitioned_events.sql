-- Snowplow events partitioned by event type
-- Clustered into 32 buckets by domain_userid
-- Provides much faster per-event access for event ETLs
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
PARTITIONED BY (event string)
CLUSTERED BY (domain_userid) INTO 32 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${PARTITIONED_EVENTS_TABLE}'
TBLPROPERTIES ('serialization.null.format'='')
