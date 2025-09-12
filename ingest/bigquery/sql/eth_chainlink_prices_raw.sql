-- Params (use bq --parameter):
--   feed_addresses: ARRAY<STRING>   e.g. ["0x8fff...f6","0x3e7d...2d"] (lower/hex)
--   answer_updated_topic: STRING    e.g. "0x..." (keccak topic0)
--   date_start: DATE                e.g. 2023-01-01
--   date_end: DATE                  e.g. 2025-12-31

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices_raw` AS
SELECT
  l.address AS feed_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index,
  l.data AS data
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE LOWER(l.address) IN UNNEST(@feed_addresses)
  AND l.topics[SAFE_OFFSET(0)] = @answer_updated_topic
  AND DATE(b.timestamp) BETWEEN @date_start AND @date_end;


