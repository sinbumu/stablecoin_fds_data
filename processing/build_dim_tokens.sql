-- Basic token decimals for USD normalization

CREATE OR REPLACE TABLE `stablecoin_fds.dim_tokens` AS
SELECT * FROM UNNEST([
  STRUCT(LOWER('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') AS token, 6 AS decimals),  -- USDC
  STRUCT(LOWER('0xdAC17F958D2ee523a2206206994597C13D831ec7') AS token, 6 AS decimals),  -- USDT
  STRUCT(LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F') AS token, 18 AS decimals), -- DAI
  STRUCT(LOWER('0xC02aaA39b223FE8D0A0E5C4F27eAD9083C756Cc2') AS token, 18 AS decimals)  -- WETH
]);


