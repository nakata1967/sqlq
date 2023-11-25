SELECT
  device.category AS device_category,
  COUNT(*) AS purchase_events
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  event_name = 'purchase'
  AND _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
GROUP BY
  device_category;