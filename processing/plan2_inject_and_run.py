import argparse
import os
import shlex
import subprocess
import sys
import yaml
import json


ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
UNIV3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
FREEZE_EVENT_TOPICS = [
    "0xffa4e6181777692565cf28528fc88fd1516ea86b56da075235fa575af6a4b855",  # Blacklisted(address)
    "0x117e3210bb9aa7d9baff172026820255c6f6c30ba8999d1c2fd88e2848137c4e",  # UnBlacklisted(address)
    "0x42e160154868087d6bfdc0ca23d96a1c1cfa32f1b72ba9ba27b69b98a0d819dc",  # AddedBlackList(address)
    "0xd7e9ec6e6ecd65492dce6bf513cd6867560d49544421d0783ddf06e76c24470c",  # RemovedBlackList(address)
    "0x61e6e66b0d6339b2980aecc6ccc0039736791f0ccde9ed512e789a7fbdd698c6",  # DestroyedBlackFunds(address,uint256)
]


def run(cmd: str) -> None:
    print(f"[run] {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def bq_query_with_params(sql_path: str, params: dict[str, object]) -> None:
    sql_text = open(sql_path, "r", encoding="utf-8").read()
    cmd: list[str] = [
        "bq", "--project_id", os.environ.get("GCP_PROJECT", ""),
        "query", "--use_legacy_sql=false", "--quiet"
    ]
    # encode parameters (no extra shell quoting; provide JSON array values directly)
    for k, v in params.items():
        if isinstance(v, list):
            json_array = json.dumps([str(x) for x in v])  # e.g. ["a","b"]
            cmd += ["--parameter", f"{k}:ARRAY<STRING>:{json_array}"]
        elif k in ("date_start", "date_end"):
            cmd += ["--parameter", f"{k}:DATE:{v}"]
        else:
            cmd += ["--parameter", f"{k}:STRING:{v}"]
    # Pass SQL via stdin to avoid flag misparse on '--' within SQL comments
    print(f"[run] bq query with {len(params)} params and SQL {sql_path}")
    subprocess.run(cmd, input=sql_text, check=True, text=True)


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def main() -> None:
    ap = argparse.ArgumentParser(description="PLAN2 param-injected runner")
    ap.add_argument("--date-start", default="2023-01-01")
    ap.add_argument("--date-end", default="2025-12-31")
    ap.add_argument("--skip-chainlink", action="store_true")
    ap.add_argument("--skip-univ3", action="store_true")
    ap.add_argument("--skip-freeze", action="store_true")
    ap.add_argument("--skip-view", action="store_true")
    args = ap.parse_args()

    # Helpers/UDFs
    try:
        run(f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_udfs.sql")
        run(f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_block_helpers.sql")
    except subprocess.CalledProcessError as e:
        print(f"[error] UDF/helpers failed: {e}", file=sys.stderr)
        return

    # Load configs
    oracle_cfg = load_yaml("configs/oracle_feeds.yaml").get("ethereum_feeds", {})
    dex_cfg = load_yaml("configs/dex_pools.yaml").get("uniswap_v3_pools", {})
    eth_cfg = load_yaml("configs/ethereum_only.yaml") if os.path.exists("configs/ethereum_only.yaml") else {}
    usdc = (eth_cfg.get("ethereum", {}).get("usdc", {}).get("address", "") if isinstance(eth_cfg, dict) else "").lower() or os.environ.get("ETH_USDC", "").lower()
    usdt = (eth_cfg.get("ethereum", {}).get("usdt", {}).get("address", "") if isinstance(eth_cfg, dict) else "").lower() or os.environ.get("ETH_USDT", "").lower()

    feed_addresses = [v.get("address", "").lower() for v in oracle_cfg.values() if v.get("address")]
    pool_addresses = [v.get("address", "").lower() for v in dex_cfg.values() if v.get("address")]

    # 1) Chainlink raw
    if not args.skip_chainlink:
        try:
            bq_query_with_params(
                "ingest/bigquery/sql/eth_chainlink_prices_raw.sql",
                {
                    "feed_addresses": feed_addresses,
                    "answer_updated_topic": ANSWER_UPDATED_TOPIC,
                    "date_start": args.date_start,
                    "date_end": args.date_end,
                },
            )
        except subprocess.CalledProcessError as e:
            print(f"[error] chainlink raw failed: {e}", file=sys.stderr)

    # 2) Uniswap v3 swaps raw
    if not args.skip_univ3:
        try:
            bq_query_with_params(
                "ingest/bigquery/sql/eth_uniswap_v3_swaps_raw.sql",
                {
                    "target_pools": pool_addresses,
                    "univ3_swap_topic": UNIV3_SWAP_TOPIC,
                    "date_start": args.date_start,
                    "date_end": args.date_end,
                },
            )
        except subprocess.CalledProcessError as e:
            print(f"[error] uniswap v3 raw failed: {e}", file=sys.stderr)

    # 3) Freeze/Blacklist events
    if not args.skip_freeze:
        try:
            bq_query_with_params(
                "ingest/bigquery/sql/eth_freeze_events.sql",
                {
                    "usdc_addr": usdc,
                    "usdt_addr": usdt,
                    "freeze_event_topics": FREEZE_EVENT_TOPICS,
                    "date_start": args.date_start,
                    "date_end": args.date_end,
                },
            )
        except subprocess.CalledProcessError as e:
            print(f"[error] freeze events failed: {e}", file=sys.stderr)

    # 4) Build decoded/view
    if not args.skip_view:
        try:
            run(f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_decoded_prs.sql")
            run(f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_prs_crs_view.sql")
        except subprocess.CalledProcessError as e:
            print(f"[error] build view failed: {e}", file=sys.stderr)

    print("[done] PLAN2 inject-and-run finished")


if __name__ == "__main__":
    main()


