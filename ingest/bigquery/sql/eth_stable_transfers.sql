-- Ethereum: USDC/USDT/DAI 전송 로그 (2023-01-01 이후)
SELECT
  block_timestamp AS ts,
  transaction_hash AS tx_hash,
  token_address,
  from_address AS from_addr,
  to_address   AS to_addr,
  value        AS amount_raw,
  block_number
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE token_address IN (
  '0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48', -- USDC
  '0xdAC17F958D2ee523a2206206994597C13D831ec7', -- USDT
  '0x6B175474E89094C44Da98b954EedeAC495271d0F'  -- DAI
)
AND block_timestamp >= TIMESTAMP('2023-01-01');
