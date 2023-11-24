SELECT
  COUNT(DISTINCT user_session) AS total_sessions
FROM (
  SELECT
    /* CAST�֐����g���āA�����l�iint_value�j�𕶎���iSTRING�j�Ɍ^�ϊ��B�قȂ�f�[�^�^����������O�ɓ����^�ɑ�����K�v������B */ CONCAT(user_pseudo_id, CAST(event_params.value.int_value AS STRING)) AS user_session
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_params.key = 'ga_session_id'
    AND event_name = 'first_visit');