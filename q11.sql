SELECT
  COUNT(DISTINCT transaction_id) AS unique_purchase_count
FROM (
  SELECT
    event_params.value.string_value AS transaction_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_params,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20201110'
    AND event_name = 'purchase'
    AND items.item_id = '9195841'
    AND event_params.key = 'transaction_id'
);
