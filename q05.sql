SELECT
  *
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'session_start'
  AND _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110';