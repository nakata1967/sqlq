SELECT
  event_params.value.string_value AS page_location,
  COUNT(*) AS page_views
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(event_params) AS event_params
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
  AND event_name = 'page_view'
  AND event_params.key = 'page_location'
GROUP BY
  page_location
ORDER BY
  page_views DESC
LIMIT
  5;