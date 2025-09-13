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
    data
  FROM `stablecoin_fds.fact_oracle_prices_raw_all`
  WHERE topic0 = '0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f'  -- AnswerUpdated
), decoded AS (
  SELECT
    s.*,
    CAST(`stablecoin_fds.udf_int256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 0) AS BIGNUMERIC) AS answer_raw,
    CAST(`stablecoin_fds.udf_uint256_slot`(`stablecoin_fds.udf_hex_to_bytes`(data), 1) AS BIGNUMERIC) AS updated_at_raw
  FROM src s
)
SELECT * FROM decoded;

CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices` PARTITION BY DATE(block_timestamp_utc) AS
WITH d AS (
  SELECT d.*, f.symbol, f.decimals
  FROM `stablecoin_fds.view_oracle_prices_decoded` d
  LEFT JOIN `stablecoin_fds.dim_oracle_feeds` f
    ON f.aggregator = d.aggregator
), scaled AS (
  SELECT
    d.*, 
    CASE WHEN d.answer_raw IS NULL OR d.decimals IS NULL THEN NULL
         ELSE SAFE_DIVIDE(CAST(d.answer_raw AS NUMERIC), POW(10, d.decimals)) END AS price_scaled
  FROM d
)
SELECT * FROM scaled;


