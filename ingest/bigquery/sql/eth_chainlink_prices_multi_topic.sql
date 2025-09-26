-- Params:
--   topics: ARRAY<STRING>
--   date_start: DATE
--   date_end: DATE

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices_raw_all` AS
WITH aggs AS (
  -- Aggregator set from dim_oracle_feeds (as-of mapping)
  SELECT DISTINCT LOWER(aggregator) AS addr
  FROM `stablecoin_fds.dim_oracle_feeds`
), proxies AS (
  -- Known mainnet proxies (expandable or parameterized later)
  SELECT * FROM UNNEST([
    LOWER('0x8fffffd4afb6115b954bd326cbe7b4ba576818f6'), -- USDC/USD proxy
    LOWER('0x3e7d1eab13ad0104d2750b8863b489d65364e32d'), -- USDT/USD proxy
    LOWER('0xaed0c38402a5d19df6e4c03f4e2dced6e29c1ee9'), -- DAI/USD proxy
    LOWER('0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419')  -- ETH/USD proxy
  ]) AS addr
), addresses AS (
  SELECT addr FROM aggs
  UNION DISTINCT
  SELECT addr FROM proxies
), logs AS (
  SELECT
    l.address AS feed_address,
    l.block_number,
    l.block_timestamp AS block_timestamp_utc,
    l.transaction_hash AS tx_hash,
    l.log_index,
    l.topics[SAFE_OFFSET(0)] AS topic0,
    l.data AS data,
    CASE 
      WHEN REGEXP_CONTAINS(l.data, r'^0x[0-9A-Fa-f]*$') THEN FROM_HEX(SUBSTR(l.data, 3))
      ELSE NULL
    END AS data_bytes
  FROM `bigquery-public-data.crypto_ethereum.logs` l
  JOIN addresses a ON LOWER(l.address) = a.addr
    AND DATE(l.block_timestamp) BETWEEN @date_start AND @date_end
)
SELECT * FROM logs;


