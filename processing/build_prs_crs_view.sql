-- v_prs_crs_features (초기 스켈레톤)
-- 파라미터: date_start, date_end

CREATE OR REPLACE VIEW `stablecoin_fds.v_prs_crs_features` AS
WITH t AS (
  SELECT *
  FROM `stablecoin_fds.classified_transfers`
  WHERE ts BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31')
), m AS (
  SELECT LOWER(token) AS token, symbol
  FROM `stablecoin_fds.dim_token_oracles`
), p AS (
  SELECT symbol, block_timestamp_utc, price_scaled
  FROM `stablecoin_fds.fact_oracle_prices`
)
SELECT
  t.tx_hash,
  t.log_index,
  t.block_number,
  t.ts AS block_timestamp_utc,
  t.token,
  t.amount_norm,
  t.from_addr,
  t.to_addr,
  -- CRS
  IFNULL(s.address IS NOT NULL, FALSE) AS direct_sanctions_risk,
  IFNULL(f.tx_hash IS NOT NULL, FALSE) AS freeze_event_risk,
  -- PRS: stablecoin 가격이 $1과 얼마나 벗어났는지 (bps)
  CASE
    WHEN pr.symbol IN ('USDC','USDT','DAI') AND pr.price_scaled IS NOT NULL THEN ABS(pr.price_scaled - 1) * 10000
    ELSE NULL
  END AS oracle_deviation_bps,
  CAST(NULL AS NUMERIC) AS liquidity_risk_score
FROM t
LEFT JOIN `stablecoin_fds.dim_sanctions_offchain` s
  ON LOWER(t.from_addr) = LOWER(s.address) OR LOWER(t.to_addr) = LOWER(s.address)
LEFT JOIN `stablecoin_fds.fact_freeze_events` f
  ON f.block_number <= t.block_number
  AND (LOWER(f.subject_address) = LOWER(t.from_addr) OR LOWER(f.subject_address) = LOWER(t.to_addr))
LEFT JOIN m ON m.token = LOWER(t.token)
LEFT JOIN (
  SELECT symbol, block_timestamp_utc, price_scaled,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY block_timestamp_utc DESC) AS rn,
         block_timestamp_utc AS ref_ts
  FROM p
) latest ON latest.symbol = m.symbol AND latest.rn = 1
LEFT JOIN (
  SELECT x.symbol, x.price_scaled, x.block_timestamp_utc, x.tx_ts
  FROM (
    SELECT m.symbol, p.price_scaled, p.block_timestamp_utc, t.ts AS tx_ts,
           ROW_NUMBER() OVER (PARTITION BY m.symbol, t.ts ORDER BY p.block_timestamp_utc DESC) AS rn
    FROM t
    JOIN m ON m.token = LOWER(t.token)
    JOIN p ON p.symbol = m.symbol AND p.block_timestamp_utc <= t.ts
  ) x WHERE x.rn = 1
) pr ON pr.symbol = m.symbol AND pr.tx_ts = t.ts;


