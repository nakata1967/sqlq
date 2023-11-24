  /* �N�G���͈ȉ��̃X�e�b�v�ō\������Ă���F 1. SessionBoundaries CTE�iCommon TABLE Expression�j�́A�e���[�U�[�̃Z�b�V�����J�n�C�x���g�̃^�C���X�^���v�ƁA���̎��̃C�x���g�̃^�C���X�^���v���擾����B����ɂ́ALEAD�֐����g�p���Ă���B 2. SessionDurations CTE�́A�Z�b�V�����̏I���^�C���X�^���v���v�Z���܂��B�������̃C�x���g��30���ȏ��A�܂��͑��݂��Ȃ��ꍇ�́A30�����Z�b�V�����̒����Ƃ݂Ȃ��Ă��܂��B 3. �Ō��
SELECT
  ���ł́A�e�Z�b�V�����̒����i�J�n�^�C���X�^���v�ƏI���^�C���X�^���v�̍��j�̕��ς��v�Z���A���̌��ʂ������b�̃t�H�[�}�b�g�ŏo�͂��܂��B */
WITH
  SessionBoundaries AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    LEAD(event_timestamp, 1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS next_event_timestamp /* �����ł� LEAD �֐��͈ȉ��̂悤�ɋ@�\����F event_timestamp: LEAD �֐����Q�Ƃ����B 1: ���݂̍s�̎��̍s���Q�Ƃ��邽�߂̃I�t�Z�b�g�l�B�����ł́A���̍s�i1�s��j�̒l���擾����B OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp): LEAD �֐������삷��E�B���h�E�i�܂��̓f�[�^�̃T�u�Z�b�g�j���`����B���̕�����2�̎�v�ȋ@�\�����F
  PARTITION BY
    user_pseudo_id: ���ʃZ�b�g�� user_pseudo_id ���Ƃɕ�������B�܂�A�e���[�U�[�̃f�[�^�͌ʂɍl�������B
  ORDER BY
    event_timestamp: �e�p�[�e�B�V�������̍s�� event_timestamp �̒l�Ɋ�Â��ď����t������B���̏����Ɋ�Â��� LEAD �͎��̍s�����肷��B AS next_event_timestamp: ����̓G�C���A�X�ŁALEAD �֐��ɂ���Ď擾���ꂽ���̍s�� event_timestamp �l�� next_event_timestamp �Ƃ����V������Ɋi�[����B ���ʂƂ��āA���̃N�G���̕����ł́A�e���[�U�[���ƂɁA���̃Z�b�V�����J�n�C�x���g�̃^�C���X�^���v�����݂̃Z�b�V�����J�n�C�x���g�̍s�ɒǉ����܂��B����ɂ��A���̃C�x���g�ƌ��݂̃C�x���g�Ƃ̎��ԍ����v�Z���A�Z�b�V�����̊��Ԃ𐄒肷�邽�߂̊�b����邱�Ƃ��ł���B */
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