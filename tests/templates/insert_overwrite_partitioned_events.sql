PARTITION (event)
SELECT
  app_id,
  collector_tstamp,
  dvce_tstamp,
  event_id,
  domain_userid,
  domain_sessionidx,
  page_url,
  page_title,
  page_referrer,
  page_urlscheme,
  page_urlhost,
  page_urlport,
  page_urlpath,
  page_urlquery,
  page_urlfragment,
  refr_urlscheme,
  refr_urlhost,
  refr_urlport,
  refr_urlpath,
  refr_urlquery,
  refr_urlfragment,
  se_category string,
  se_action string,
  se_label string,
  se_property string,
  se_value double,
  CASE
    WHEN event = 'struct' THEN se_action
    ELSE event
  END AS event
FROM
  {{inputs.events}}
