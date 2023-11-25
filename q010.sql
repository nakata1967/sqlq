SELECT
  items.item_id,
  SUM(items.quantity) AS total_sales
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS items
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
  AND event_name = 'purchase'
  AND items.item_id IS NOT NULL
GROUP BY
  items.item_id
ORDER BY
  total_sales DESC;