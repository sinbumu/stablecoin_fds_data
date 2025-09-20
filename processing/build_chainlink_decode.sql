-- Decode Chainlink logs into standardized prices

-- AnswerUpdated(old,int256 indexed current, uint256 roundId, uint256 updatedAt?)
-- But actual layouts vary; we start with common two-slot data: answer(int256), updatedAt(uint256)

CREATE OR REPLACE VIEW `stablecoin_fds.view_oracle_prices_decoded` AS
WITH src AS (
  SELECT
    LOWER(feed_address) AS aggregator,
    block_number,
    block_timestamp_utc,
    tx_hash,
    log_index,
    topic0,
    data_bytes
  FROM `stablecoin_fds.fact_oracle_prices_raw_all`
  WHERE BYTE_LENGTH(data_bytes) >= 32
), decoded AS (
  SELECT
    s.*,
    `stablecoin_fds.udf_int256_slot`(data_bytes, 0) AS answer_raw_str,
    `stablecoin_fds.udf_uint256_slot`(data_bytes, 1) AS updated_at_raw_str
  FROM src s
)
SELECT * FROM decoded;

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices` PARTITION BY DATE(block_timestamp_utc) AS
WITH d AS (
  SELECT d.*, f.symbol, f.decimals,
         SAFE_CAST(d.answer_raw_str AS BIGNUMERIC) AS answer_raw,
         SAFE_CAST(d.updated_at_raw_str AS BIGNUMERIC) AS updated_at_raw
  FROM `stablecoin_fds.view_oracle_prices_decoded` d
  LEFT JOIN `stablecoin_fds.dim_oracle_feeds` f
    ON f.aggregator = d.aggregator
), scaled AS (
  SELECT
    d.*,
    -- Build exact BIGNUMERIC scale = 10^decimals without floating conversion
    CASE WHEN d.answer_raw IS NULL THEN NULL
         ELSE (
           SAFE_CAST(d.answer_raw AS BIGNUMERIC) /
           SAFE_CAST(CONCAT('1', REPEAT('0', COALESCE(d.decimals, 8))) AS BIGNUMERIC)
         )
    END AS price_scaled_bn
  FROM d
), final AS (
  SELECT
    d.*, SAFE_CAST(price_scaled_bn AS NUMERIC) AS price_scaled
  FROM scaled d
)
SELECT * FROM final;


