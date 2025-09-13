-- Map token contract → oracle feed symbol (for USD pricing)

CREATE OR REPLACE TABLE `stablecoin_fds.dim_token_oracles` AS
WITH m AS (
  SELECT LOWER('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') AS token, 'usdc_usd' AS symbol UNION ALL -- USDC
  SELECT LOWER('0xdac17f958d2ee523a2206206994597c13d831ec7'), 'usdt_usd' UNION ALL -- USDT
  SELECT LOWER('0x6b175474e89094c44da98b954eedeac495271d0f'), 'dai_usd' UNION ALL -- DAI
  SELECT LOWER('0xC02aaA39b223FE8D0A0E5C4F27eAD9083C756Cc2'), 'eth_usd' -- WETH
)
SELECT * FROM m;


