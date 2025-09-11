-- PLAN2 정합을 위해 log_index 포함 및 주소 소문자 비교
SELECT
  block_timestamp AS ts,
  transaction_hash AS tx_hash,
  log_index,
  token_address,
  from_address AS from_addr,
  to_address   AS to_addr,
  value        AS amount_raw,
  block_number
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE LOWER(token_address) IN (
  '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- USDC
  '0xdac17f958d2ee523a2206206994597c13d831ec7', -- USDT
  '0x6b175474e89094c44da98b954eedeac495271d0f'  -- DAI
)
AND block_timestamp >= TIMESTAMP('2023-01-01');
