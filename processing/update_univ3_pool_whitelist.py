import json
import os
import subprocess
import sys
from typing import Any, Dict, List

import yaml


def run_command(command: List[str], *, input_text: str | None = None) -> None:
    print("[run]", " ".join(command))
    subprocess.run(command, input=input_text, text=True, check=True)


def load_pools_config(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    pools = data.get("uniswap_v3_pools", {})
    rows: List[Dict[str, Any]] = []
    # known token symbol → address map (Ethereum mainnet)
    symbol_to_addr = {
        "USDC": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "USDT": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "DAI":  "0x6b175474e89094c44da98b954eedeac495271d0f",
        # ETH on Uniswap v3 pools is wrapped ETH
        "ETH":  "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "WETH": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
    }

    for key, info in pools.items():
        if not isinstance(info, dict):
            continue
        address = str(info.get("address", "")).lower()
        token0_raw = str(info.get("token0", ""))
        token1_raw = str(info.get("token1", ""))
        # accept either address or symbol; map symbol → address
        token0 = symbol_to_addr.get(token0_raw.upper(), token0_raw).lower()
        token1 = symbol_to_addr.get(token1_raw.upper(), token1_raw).lower()
        fee = int(info.get("fee", 0))
        decimals0 = int(info.get("decimals0", 18))
        decimals1 = int(info.get("decimals1", 18))
        # require valid addresses for pool and tokens
        if not address or not token0.startswith("0x") or not token1.startswith("0x"):
            continue
        rows.append(
            {
                "pool": address,
                "token0": token0,
                "token1": token1,
                "fee": fee,
                "decimals0": decimals0,
                "decimals1": decimals1,
                "source_key": key,
            }
        )
    return rows


def build_create_table_sql(dataset: str, rows: List[Dict[str, Any]]) -> str:
    # Build an array of STRUCT literals
    def lit(value: Any) -> str:
        if isinstance(value, str):
            # Escape single quotes
            return "'" + value.replace("'", "\\'") + "'"
        return str(int(value))

    struct_rows = []
    for r in rows:
        struct_rows.append(
            "STRUCT("
            f"{lit(r['pool'])} AS pool, {lit(r['token0'])} AS token0, {lit(r['token1'])} AS token1, "
            f"{lit(r['fee'])} AS fee, {lit(r['decimals0'])} AS decimals0, {lit(r['decimals1'])} AS decimals1, "
            f"{lit(r['source_key'])} AS source_key"
            ")"
        )

    array_literal = ",\n  ".join(struct_rows) if struct_rows else ""

    sql = f"""
CREATE OR REPLACE TABLE `{dataset}.univ3_pool_whitelist` AS
SELECT * FROM UNNEST([
  {array_literal}
]);
"""
    return sql


def main() -> None:
    project_id = os.environ.get("GCP_PROJECT", "")
    dataset = os.environ.get("BQ_DATASET", "stablecoin_fds")
    config_path = os.path.join("configs", "dex_pools.yaml")
    rows = load_pools_config(config_path)
    if not rows:
        print("[error] no pools found in configs/dex_pools.yaml", file=sys.stderr)
        sys.exit(2)

    sql = build_create_table_sql(dataset, rows)
    cmd = [
        "bq",
        "--project_id",
        project_id,
        "query",
        "--use_legacy_sql=false",
    ]
    run_command(cmd, input_text=sql)
    print(f"[done] {len(rows)} pools written to {dataset}.univ3_pool_whitelist")


if __name__ == "__main__":
    main()


