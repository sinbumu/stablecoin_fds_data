-- dim_blocks + block_at_or_before UDF

CREATE OR REPLACE TABLE `stablecoin_fds.dim_blocks` AS
SELECT
  number AS block_number,
  timestamp AS block_timestamp_utc,
  DATE(timestamp) AS block_date,
  EXTRACT(HOUR FROM timestamp) AS block_hour
FROM `bigquery-public-data.crypto_ethereum.blocks`
WHERE DATE(timestamp) >= '2023-01-01';

CREATE OR REPLACE FUNCTION `stablecoin_fds.block_at_or_before`(ts TIMESTAMP)
RETURNS INT64 AS ((
  SELECT block_number
  FROM `stablecoin_fds.dim_blocks`
  WHERE block_timestamp_utc <= ts
  ORDER BY block_timestamp_utc DESC
  LIMIT 1
));


