-- Build Uniswap v3 normalized swap view with amount signs and USD values

CREATE OR REPLACE VIEW `stablecoin_fds.view_univ3_swaps_norm` AS
WITH base AS (
  SELECT
    LOWER(pool_address) AS pool,
    block_number,
    block_timestamp_utc,
    tx_hash,
    log_index,
    topics,
    data,
    -- Decode amounts and sqrtPriceX96 from log data
    SAFE_CAST(`stablecoin_fds.udf_int256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 0) AS BIGNUMERIC) AS amount0,
    SAFE_CAST(`stablecoin_fds.udf_int256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 1) AS BIGNUMERIC) AS amount1,
    CAST(`stablecoin_fds.udf_uint256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 2) AS BIGNUMERIC) AS sqrtPriceX96,
    CAST(`stablecoin_fds.udf_uint256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 3) AS BIGNUMERIC) AS liquidity,
    CAST(`stablecoin_fds.udf_int256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 4) AS BIGNUMERIC) AS tick
  FROM `stablecoin_fds.fact_univ3_swaps_raw`
), price_ratio AS (
  SELECT
    *,
    `stablecoin_fds.udf_univ3_price_ratio`(sqrtPriceX96) AS price_token1_per_token0
  FROM base
)
SELECT * FROM price_ratio;

-- Optional: whitelist view for stable pools (user to populate table later)
CREATE OR REPLACE TABLE `stablecoin_fds.univ3_pool_whitelist` (
  pool STRING,
  token0 STRING,
  token1 STRING,
  fee_tier INT64,
  is_stable_pool BOOL
);

CREATE OR REPLACE VIEW `stablecoin_fds.view_univ3_swaps_norm_stable` AS
SELECT s.*
FROM `stablecoin_fds.view_univ3_swaps_norm` s
JOIN `stablecoin_fds.univ3_pool_whitelist` w
  ON w.pool = s.pool
WHERE w.is_stable_pool = TRUE;


