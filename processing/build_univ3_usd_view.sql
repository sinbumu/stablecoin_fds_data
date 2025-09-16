-- Build Uniswap v3 USD normalized view by joining oracle prices

CREATE OR REPLACE VIEW `stablecoin_fds.view_univ3_swaps_usd` AS
WITH pools AS (
  SELECT DISTINCT pool, token0, token1 FROM `stablecoin_fds.univ3_pool_whitelist`
), tokens AS (
  SELECT p.pool,
         LOWER(p.token0) AS token0,
         LOWER(p.token1) AS token1,
         o0.symbol AS token0_symbol,
         o1.symbol AS token1_symbol,
         COALESCE(t0.decimals, 18) AS token0_decimals,
         COALESCE(t1.decimals, 18) AS token1_decimals
  FROM pools p
  LEFT JOIN `stablecoin_fds.dim_token_oracles` o0 ON o0.token = LOWER(p.token0)
  LEFT JOIN `stablecoin_fds.dim_token_oracles` o1 ON o1.token = LOWER(p.token1)
  LEFT JOIN `stablecoin_fds.dim_tokens` t0 ON t0.token = LOWER(p.token0)
  LEFT JOIN `stablecoin_fds.dim_tokens` t1 ON t1.token = LOWER(p.token1)
), prices AS (
  SELECT symbol, block_timestamp_utc, price_scaled
  FROM `stablecoin_fds.fact_oracle_prices`
), s AS (
  SELECT n.pool, n.block_timestamp_utc, n.tx_hash, n.log_index,
         n.amount0, n.amount1, n.price_token1_per_token0,
         t.token0_symbol, t.token1_symbol,
         t.token0_decimals, t.token1_decimals
  FROM `stablecoin_fds.view_univ3_swaps_norm` n
  JOIN tokens t ON t.pool = n.pool
), refs0 AS (
  SELECT DISTINCT token0_symbol AS symbol, block_timestamp_utc AS ts
  FROM s WHERE token0_symbol IS NOT NULL
), p0 AS (
  SELECT symbol, ts,
         price_scaled,
         ROW_NUMBER() OVER (PARTITION BY symbol, ts ORDER BY block_timestamp_utc DESC) AS rn
  FROM prices p
  JOIN refs0 r USING (symbol)
  WHERE p.block_timestamp_utc <= r.ts
), refs1 AS (
  SELECT DISTINCT token1_symbol AS symbol, block_timestamp_utc AS ts
  FROM s WHERE token1_symbol IS NOT NULL
), p1 AS (
  SELECT symbol, ts,
         price_scaled,
         ROW_NUMBER() OVER (PARTITION BY symbol, ts ORDER BY block_timestamp_utc DESC) AS rn
  FROM prices p
  JOIN refs1 r USING (symbol)
  WHERE p.block_timestamp_utc <= r.ts
)
SELECT
  s.pool,
  s.block_timestamp_utc,
  s.tx_hash,
  s.log_index,
  s.amount0,
  s.amount1,
  s.price_token1_per_token0,
  CAST(p0.price_scaled AS NUMERIC) AS token0_price_usd,
  CAST(p1.price_scaled AS NUMERIC) AS token1_price_usd,
  SAFE_CAST((CAST(s.amount0 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token0_decimals)) AS BIGNUMERIC)) AS NUMERIC) AS amount0_norm,
  SAFE_CAST((CAST(s.amount1 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token1_decimals)) AS BIGNUMERIC)) AS NUMERIC) AS amount1_norm,
  (SAFE_CAST((CAST(s.amount0 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token0_decimals)) AS BIGNUMERIC)) AS NUMERIC) * CAST(p0.price_scaled AS NUMERIC)) AS amount0_usd,
  (SAFE_CAST((CAST(s.amount1 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token1_decimals)) AS BIGNUMERIC)) AS NUMERIC) * CAST(p1.price_scaled AS NUMERIC)) AS amount1_usd,
  (
    ABS(SAFE_CAST((CAST(s.amount0 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token0_decimals)) AS BIGNUMERIC)) AS NUMERIC)) * CAST(p0.price_scaled AS NUMERIC)
    + ABS(SAFE_CAST((CAST(s.amount1 AS BIGNUMERIC) / CAST(CONCAT('1', REPEAT('0', s.token1_decimals)) AS BIGNUMERIC)) AS NUMERIC)) * CAST(p1.price_scaled AS NUMERIC)
  ) AS notional_usd
FROM s
LEFT JOIN p0
  ON p0.symbol = s.token0_symbol AND p0.ts = s.block_timestamp_utc AND p0.rn = 1
LEFT JOIN p1
  ON p1.symbol = s.token1_symbol AND p1.ts = s.block_timestamp_utc AND p1.rn = 1;


