-- BigQuery JS UDFs for decoding

CREATE OR REPLACE FUNCTION `stablecoin_fds.udf_hex_to_bignum`(hex STRING)
RETURNS BIGNUMERIC
LANGUAGE js AS r"""
  if (!hex) return null;
  const s = hex.startsWith('0x') ? hex.slice(2) : hex;
  // BigInt → string → return; BIGNUMERIC precision assumed sufficient for uint256 ranges subset
  return BigInt('0x' + s).toString();
""";

CREATE OR REPLACE FUNCTION `stablecoin_fds.udf_uint256_slot`(data BYTES, slot INT64)
RETURNS BIGNUMERIC
LANGUAGE js AS r"""
  if (!data) return null;
  const buf = data;  // Uint8Array
  const start = Number(slot) * 32;
  const end = start + 32;
  const slice = buf.slice(start, end);
  let hex = '0x';
  for (let i=0;i<slice.length;i++) {
    const v = slice[i].toString(16).padStart(2,'0');
    hex += v;
  }
  return BigInt(hex).toString();
""";

-- sqrtPriceX96^2 / 2^192 as BIGNUMERIC (decimals adjustment applied outside)
-- Note: 2^96 = 79228162514264337593543950336 fits in BIGNUMERIC
CREATE OR REPLACE FUNCTION `stablecoin_fds.udf_univ3_price_ratio`(sqrtPriceX96 BIGNUMERIC)
RETURNS BIGNUMERIC AS (
  (
    SAFE_DIVIDE(CAST(sqrtPriceX96 AS BIGNUMERIC), CAST('79228162514264337593543950336' AS BIGNUMERIC))
  ) * (
    SAFE_DIVIDE(CAST(sqrtPriceX96 AS BIGNUMERIC), CAST('79228162514264337593543950336' AS BIGNUMERIC))
  )
);


