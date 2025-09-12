-- v_prs_crs_features (초기 스켈레톤)
-- 파라미터: date_start, date_end

CREATE OR REPLACE VIEW `stablecoin_fds.v_prs_crs_features` AS
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
  -- PRS placeholders (추후 디코딩 후 채움)
  CAST(NULL AS NUMERIC) AS oracle_deviation_bps,
  CAST(NULL AS NUMERIC) AS liquidity_risk_score
FROM `stablecoin_fds.classified_transfers` t
LEFT JOIN `stablecoin_fds.dim_sanctions_offchain` s
  ON LOWER(t.from_addr) = LOWER(s.address) OR LOWER(t.to_addr) = LOWER(s.address)
LEFT JOIN `stablecoin_fds.fact_freeze_events` f
  ON f.block_number <= t.block_number
  AND (LOWER(f.subject_address) = LOWER(t.from_addr) OR LOWER(f.subject_address) = LOWER(t.to_addr))
WHERE t.ts BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31');


