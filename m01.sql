SELECT
  AVG(page_views_per_session) AS average_page_views_per_session
FROM (
  SELECT
    user_pseudo_id,
    session_id,
    COUNT(*) AS page_views_per_session
  FROM (
    SELECT
      event_name,
      (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'user_pseudo_id') AS user_pseudo_id,
      (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_id') AS session_id
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20201110'
      AND event_name = 'page_view' )
  GROUP BY
    user_pseudo_id,
    session_id );