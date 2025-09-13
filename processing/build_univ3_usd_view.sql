-- Build Uniswap v3 USD normalized view by joining oracle prices

CREATE OR REPLACE VIEW `stablecoin_fds.view_univ3_swaps_usd` AS
WITH pools AS (
  SELECT DISTINCT pool, token0, token1 FROM `stablecoin_fds.univ3_pool_whitelist`
), tokens AS (
  SELECT p.pool,
         LOWER(p.token0) AS token0,
         LOWER(p.token1) AS token1,
         o0.symbol AS token0_symbol,
         o1.symbol AS token1_symbol
  FROM pools p
  LEFT JOIN `stablecoin_fds.dim_token_oracles` o0 ON o0.token = LOWER(p.token0)
  LEFT JOIN `stablecoin_fds.dim_token_oracles` o1 ON o1.token = LOWER(p.token1)
), prices AS (
  SELECT symbol, block_timestamp_utc, price_scaled
  FROM `stablecoin_fds.fact_oracle_prices`
), s AS (
  SELECT n.pool, n.block_timestamp_utc, n.tx_hash, n.log_index,
         n.amount0, n.amount1, n.price_token1_per_token0,
         t.token0_symbol, t.token1_symbol
  FROM `stablecoin_fds.view_univ3_swaps_norm` n
  JOIN tokens t ON t.pool = n.pool
)
SELECT
  s.pool,
  s.block_timestamp_utc,
  s.tx_hash,
  s.log_index,
  s.amount0,
  s.amount1,
  s.price_token1_per_token0,
  p0.price_scaled AS token0_price_usd,
  p1.price_scaled AS token1_price_usd,
  (s.amount0 * p0.price_scaled) AS amount0_usd,
  (s.amount1 * p1.price_scaled) AS amount1_usd,
  (ABS(s.amount0) * p0.price_scaled + ABS(s.amount1) * p1.price_scaled) AS notional_usd
FROM s
LEFT JOIN prices p0
  ON p0.symbol = s.token0_symbol
 AND TIMESTAMP_TRUNC(s.block_timestamp_utc, MINUTE) = TIMESTAMP_TRUNC(p0.block_timestamp_utc, MINUTE)
LEFT JOIN prices p1
  ON p1.symbol = s.token1_symbol
 AND TIMESTAMP_TRUNC(s.block_timestamp_utc, MINUTE) = TIMESTAMP_TRUNC(p1.block_timestamp_utc, MINUTE);


