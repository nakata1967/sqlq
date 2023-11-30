SELECT
  DATE(TIMESTAMP_MICROS(event_timestamp), "Asia/Tokyo") AS date,
  MAX(
  IF
    (event_params.key = 'page_title', event_params.value.string_value, NULL)) AS page_title,
  MAX(
  IF
    (event_params.key = 'page_location', event_params.value.string_value, NULL)) AS page_location,
  COUNT(event_name) AS pageviews,
  COUNT(DISTINCT user_pseudo_id) AS users
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(event_params) AS event_params
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
GROUP BY
  date
HAVING
  page_location LIKE '%google%'
ORDER BY
  date,
  pageviews DESC,
  users DESC;