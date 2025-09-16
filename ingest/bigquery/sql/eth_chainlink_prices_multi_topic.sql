-- Params:
--   topics: ARRAY<STRING>
--   date_start: DATE
--   date_end: DATE

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices_raw_all` AS
WITH aggs AS (
  SELECT DISTINCT LOWER(aggregator) AS agg
  FROM `stablecoin_fds.chainlink_proxy_aggregator_timeline`
), logs AS (
  SELECT
    l.address AS feed_address,
    l.block_number,
    b.timestamp AS block_timestamp_utc,
    l.transaction_hash AS tx_hash,
    l.log_index,
    l.topics[SAFE_OFFSET(0)] AS topic0,
    l.data AS data,
    CASE 
      WHEN REGEXP_CONTAINS(l.data, r'^0x[0-9A-Fa-f]*$') THEN FROM_HEX(SUBSTR(l.data, 3))
      ELSE NULL
    END AS data_bytes
  FROM `bigquery-public-data.crypto_ethereum.logs` l
  JOIN aggs ON LOWER(l.address) = aggs.agg
  JOIN `bigquery-public-data.crypto_ethereum.blocks` b
    ON b.number = l.block_number
  WHERE DATE(b.timestamp) BETWEEN @date_start AND @date_end
)
SELECT * FROM logs;


