SELECT
  COUNT(DISTINCT user_session) AS total_sessions
FROM (
  SELECT
    /* CAST関数を使って、整数値（int_value）を文字列（STRING）に型変換。異なるデータ型を結合する前に同じ型に揃える必要がある。 */ CONCAT(user_pseudo_id, CAST(event_params.value.int_value AS STRING)) AS user_session
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_params.key = 'ga_session_id' );