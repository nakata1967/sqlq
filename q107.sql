  /* クエリは以下のステップで構成されている： 1. SessionBoundaries CTE（Common TABLE Expression）は、各ユーザーのセッション開始イベントのタイムスタンプと、その次のイベントのタイムスタンプを取得する。これには、LEAD関数を使用している。 2. SessionDurations CTEは、セッションの終了タイムスタンプを計算します。もし次のイベントが30分以上後、または存在しない場合は、30分をセッションの長さとみなしています。 3. 最後の
SELECT
  文では、各セッションの長さ（開始タイムスタンプと終了タイムスタンプの差）の平均を計算し、その結果を時分秒のフォーマットで出力します。 */
WITH
  SessionBoundaries AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    LEAD(event_timestamp, 1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS next_event_timestamp /* ここでの LEAD 関数は以下のように機能する： event_timestamp: LEAD 関数が参照する列。 1: 現在の行の次の行を参照するためのオフセット値。ここでは、次の行（1行先）の値を取得する。 OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp): LEAD 関数が動作するウィンドウ（またはデータのサブセット）を定義する。この部分は2つの主要な機能を持つ：
  PARTITION BY
    user_pseudo_id: 結果セットを user_pseudo_id ごとに分割する。つまり、各ユーザーのデータは個別に考慮される。
  ORDER BY
    event_timestamp: 各パーティション内の行を event_timestamp の値に基づいて順序付けする。この順序に基づいて LEAD は次の行を決定する。 AS next_event_timestamp: これはエイリアスで、LEAD 関数によって取得された次の行の event_timestamp 値を next_event_timestamp という新しい列に格納する。 結果として、このクエリの部分では、各ユーザーごとに、次のセッション開始イベントのタイムスタンプを現在のセッション開始イベントの行に追加します。これにより、次のイベントと現在のイベントとの時間差を計算し、セッションの期間を推定するための基礎を作ることができる。 */
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'session_start' ),
  SessionDurations AS (
  SELECT
    user_pseudo_id,
    event_timestamp AS session_start_timestamp,
  IF
    ( next_event_timestamp IS NULL
      OR next_event_timestamp - event_timestamp > 30 * 60 * 1e6, event_timestamp + 30 * 60 * 1e6, next_event_timestamp ) AS session_end_timestamp
  FROM
    SessionBoundaries )
SELECT
  FORMAT_TIMESTAMP( "%T", TIMESTAMP_SECONDS(CAST(AVG(session_end_timestamp - session_start_timestamp) / 1e6 AS INT64)) ) AS average_session_duration_hms
FROM
  SessionDurations;