-- Create small dimension table for Chainlink feeds (aggregator → symbol/decimals)

CREATE OR REPLACE TABLE `stablecoin_fds.dim_oracle_feeds` AS
WITH feeds AS (
  SELECT LOWER('0xAb5c40fC42Cd89fB7065fEC1dB9Daf5cfBD51cb0') AS aggregator, 'usdc_usd' AS symbol, 8 AS decimals UNION ALL
  SELECT LOWER('0x7f38c422b99075f63C9c919ECD200DF8D2E4de95'), 'usdt_usd', 8 UNION ALL
  SELECT LOWER('0x804B04176Bc926B73a612C6c8f60fA3D67fA9E7D'), 'dai_usd', 8 UNION ALL
  SELECT LOWER('0xF79D6aFbB6dA890132F9D7c355e3015f15F3406F'), 'eth_usd', 8
)
SELECT * FROM feeds;


