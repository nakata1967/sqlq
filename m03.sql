SELECT
  category,
  SUM(total_sales) AS total_sales
FROM (
  SELECT
    items.item_category AS category,
    SUM(items.quantity) AS total_sales
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND items.item_category IS NOT NULL
  GROUP BY
    category
  UNION ALL
  SELECT
    items.item_category2 AS category,
    SUM(items.quantity) AS total_sales
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND items.item_category2 IS NOT NULL
  GROUP BY
    category
  UNION ALL
  SELECT
    items.item_category3 AS category,
    SUM(items.quantity) AS total_sales
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND items.item_category3 IS NOT NULL
  GROUP BY
    category
  UNION ALL
  SELECT
    items.item_category4 AS category,
    SUM(items.quantity) AS total_sales
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND items.item_category4 IS NOT NULL
  GROUP BY
    category
  UNION ALL
  SELECT
    items.item_category5 AS category,
    SUM(items.quantity) AS total_sales
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(items) AS items
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND items.item_category5 IS NOT NULL
  GROUP BY
    category )
GROUP BY
  category
ORDER BY
  total_sales DESC;