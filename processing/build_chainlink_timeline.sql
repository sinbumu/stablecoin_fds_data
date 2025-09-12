-- Build proxy→aggregator timeline from AggregatorUpdated events
-- Params: proxies ARRAY<STRING>, agg_updated_topic STRING

CREATE OR REPLACE TABLE `stablecoin_fds.chainlink_proxy_aggregator_timeline` AS
WITH updates AS (
  SELECT
    LOWER(l.address) AS proxy,
    CONCAT('0x', SUBSTR(l.topics[SAFE_OFFSET(2)], 27)) AS aggregator,
    l.block_number,
    b.timestamp AS block_timestamp_utc
  FROM `bigquery-public-data.crypto_ethereum.logs` l
  JOIN `bigquery-public-data.crypto_ethereum.blocks` b
    ON b.number = l.block_number
  WHERE LOWER(l.address) IN UNNEST(@proxies)
    AND l.topics[SAFE_OFFSET(0)] = @agg_updated_topic
), ordered AS (
  SELECT
    proxy,
    LOWER(aggregator) AS aggregator,
    block_number AS from_block,
    LEAD(block_number) OVER (PARTITION BY proxy ORDER BY block_number) AS to_block_excl,
    block_timestamp_utc AS from_ts,
    LEAD(block_timestamp_utc) OVER (PARTITION BY proxy ORDER BY block_number) AS to_ts_excl
  FROM updates
)
SELECT
  proxy,
  aggregator,
  from_block,
  COALESCE(to_block_excl, 9223372036854775807) AS to_block_excl,
  from_ts,
  to_ts_excl
FROM ordered;


