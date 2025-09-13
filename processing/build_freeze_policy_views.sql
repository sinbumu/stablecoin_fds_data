-- Extend freeze policy labels and build timelines/snapshots

-- Policy mapping (extend as needed)
CREATE OR REPLACE TABLE `stablecoin_fds.dim_freeze_policy_topics` AS
SELECT * FROM UNNEST([
  STRUCT('blacklist_on' AS action,  '0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855' AS topic0),
  STRUCT('blacklist_off',          '0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e'),
  STRUCT('blacklist_on',           '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc'),
  STRUCT('blacklist_off',          '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c'),
  -- Add pause/freeze topics here when confirmed
  STRUCT('other',                  '0x0000000000000000000000000000000000000000000000000000000000000000')
]);

CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_events_policy` AS
SELECT
  LOWER(token_contract) AS token,
  LOWER(subject_address) AS subject,
  event_sig AS topic0,
  m.action,
  block_number,
  block_timestamp_utc,
  tx_hash,
  log_index
FROM `stablecoin_fds.fact_freeze_events` e
LEFT JOIN `stablecoin_fds.dim_freeze_policy_topics` m
  ON m.topic0 = e.event_sig;

-- Multi-policy timeline (currently blacklist only)
CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_policy_timeline` AS
WITH ordered AS (
  SELECT
    token, subject, action, block_number, block_timestamp_utc,
    ROW_NUMBER() OVER (PARTITION BY token, subject, action ORDER BY block_number) AS rn,
    LEAD(block_number) OVER (PARTITION BY token, subject, action ORDER BY block_number) AS to_block_excl,
    LEAD(block_timestamp_utc) OVER (PARTITION BY token, subject, action ORDER BY block_number) AS to_ts_excl
  FROM `stablecoin_fds.view_freeze_events_policy`
  WHERE action IN ('blacklist_on','blacklist_off')
), states AS (
  SELECT
    token, subject,
    CASE WHEN action = 'blacklist_on' THEN TRUE ELSE FALSE END AS is_on,
    block_number AS from_block,
    to_block_excl,
    block_timestamp_utc AS from_ts,
    to_ts_excl
  FROM ordered
)
SELECT * FROM states;

CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_policy_snapshot_latest` AS
WITH last_action AS (
  SELECT AS VALUE t FROM (
    SELECT token, subject, action, block_number,
           ROW_NUMBER() OVER (PARTITION BY token, subject ORDER BY block_number DESC) AS rn
    FROM `stablecoin_fds.view_freeze_events_policy`
    WHERE action IN ('blacklist_on','blacklist_off')
  ) t WHERE t.rn = 1
)
SELECT 
  token,
  subject,
  CASE WHEN action = 'blacklist_on' THEN TRUE ELSE FALSE END AS is_blacklisted,
  block_number AS last_update_block
FROM last_action;


