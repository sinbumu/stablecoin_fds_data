-- Params:
--   usdc_addr: STRING
--   usdt_addr: STRING
--   freeze_event_topics: ARRAY<STRING>
--   date_start: DATE
--   date_end: DATE

SELECT
  l.address AS token_contract,
  l.topics[SAFE_OFFSET(0)] AS event_sig,
  CASE
    WHEN l.topics[SAFE_OFFSET(1)] IS NOT NULL THEN CONCAT('0x', SUBSTR(l.topics[SAFE_OFFSET(1)], 27))
    ELSE NULL
  END AS subject_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE LOWER(l.address) IN (@usdc_addr, @usdt_addr)
  AND l.topics[SAFE_OFFSET(0)] IN UNNEST(@freeze_event_topics)
  AND DATE(b.timestamp) BETWEEN @date_start AND @date_end;


