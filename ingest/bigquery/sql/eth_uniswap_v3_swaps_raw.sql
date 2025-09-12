-- Params:
--   target_pools: ARRAY<STRING>
--   univ3_swap_topic: STRING
--   date_start: DATE
--   date_end: DATE

CREATE OR REPLACE TABLE `stablecoin_fds.fact_univ3_swaps_raw` AS
SELECT
  l.address AS pool_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE l.topics[SAFE_OFFSET(0)] = @univ3_swap_topic
  AND LOWER(l.address) IN UNNEST(@target_pools)
  AND DATE(b.timestamp) BETWEEN @date_start AND @date_end;


