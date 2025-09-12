-- Build freeze/blacklist timeline and snapshot views

CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_events_enriched` AS
SELECT
  LOWER(token_contract) AS token,
  event_sig,
  LOWER(subject_address) AS subject,
  block_number,
  block_timestamp_utc,
  tx_hash,
  log_index,
  CASE
    WHEN event_sig IN (
      '0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855', -- Blacklisted
      '0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc'  -- AddedBlackList
    ) THEN 'blacklist_on'
    WHEN event_sig IN (
      '0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e', -- UnBlacklisted
      '0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c'  -- RemovedBlackList
    ) THEN 'blacklist_off'
    ELSE 'other'
  END AS action
FROM `stablecoin_fds.fact_freeze_events`;

CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_timeline` AS
WITH ordered AS (
  SELECT
    token,
    subject,
    block_number,
    block_timestamp_utc,
    action,
    ROW_NUMBER() OVER (PARTITION BY token, subject ORDER BY block_number) AS rn
  FROM `stablecoin_fds.view_freeze_events_enriched`
  WHERE action IN ('blacklist_on','blacklist_off')
), states AS (
  SELECT
    token,
    subject,
    block_number AS from_block,
    LEAD(block_number) OVER (PARTITION BY token, subject ORDER BY block_number) AS to_block_excl,
    block_timestamp_utc AS from_ts,
    LEAD(block_timestamp_utc) OVER (PARTITION BY token, subject ORDER BY block_number) AS to_ts_excl,
    action
  FROM ordered
)
SELECT
  token,
  subject,
  from_block,
  to_block_excl,
  from_ts,
  to_ts_excl,
  CASE WHEN action = 'blacklist_on' THEN TRUE ELSE FALSE END AS is_blacklisted
FROM states;

CREATE OR REPLACE VIEW `stablecoin_fds.view_freeze_snapshot_latest` AS
WITH last_action AS (
  SELECT AS VALUE t FROM (
    SELECT token, subject, action, block_number,
           ROW_NUMBER() OVER (PARTITION BY token, subject ORDER BY block_number DESC) AS rn
    FROM `stablecoin_fds.view_freeze_events_enriched`
    WHERE action IN ('blacklist_on','blacklist_off')
  ) t WHERE t.rn = 1
)
SELECT 
  token,
  subject,
  CASE WHEN action = 'blacklist_on' THEN TRUE ELSE FALSE END AS is_blacklisted,
  block_number AS last_update_block
FROM last_action;


