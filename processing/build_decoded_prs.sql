-- Decode Chainlink AnswerUpdated → fact_oracle_prices (price_scaled)
-- Assumes fact_oracle_prices_raw has .data with uint256 answer in first slot

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices` AS
SELECT
  feed_address,
  block_number,
  block_timestamp_utc,
  tx_hash,
  log_index,
  CAST(`stablecoin_fds.udf_uint256_slot`(data, 0) AS BIGNUMERIC) AS answer_uint,
  -- decimals 보정은 조인으로 처리(피드 메타)
  NULL AS price_scaled
FROM `stablecoin_fds.fact_oracle_prices_raw`;

-- Join with feed decimals to compute price_scaled (example using configs/oracle_feeds.yaml ingested to BQ is pending)


