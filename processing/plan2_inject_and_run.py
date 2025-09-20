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


def _format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    idx = 0
    while size >= 1024.0 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.2f} {units[idx]}"


def bq_query_with_params(
    sql_path: str,
    params: dict[str, object],
    *,
    max_bytes_billed: int | None = None,
    dry_run_first: bool = False,
    only_dry_run: bool = False,
) -> None:
    sql_text = open(sql_path, "r", encoding="utf-8").read()
    base_cmd: list[str] = [
        "bq", "--project_id", os.environ.get("GCP_PROJECT", ""),
        "query", "--use_legacy_sql=false", "--quiet"
    ]
    # encode parameters (no extra shell quoting; provide JSON array values directly)
    param_flags: list[str] = []
    for k, v in params.items():
        if isinstance(v, list):
            json_array = json.dumps([str(x) for x in v])
            param_flags += ["--parameter", f"{k}:ARRAY<STRING>:{json_array}"]
        elif k in ("date_start", "date_end"):
            param_flags += ["--parameter", f"{k}:DATE:{v}"]
        else:
            param_flags += ["--parameter", f"{k}:STRING:{v}"]

    # Optional dry-run estimation
    if dry_run_first or only_dry_run:
        dr_cmd = base_cmd + ["--dry_run", "--format=prettyjson"] + param_flags
        print(f"[dry-run] estimating bytes for {sql_path}")
        dr = subprocess.run(dr_cmd, input=sql_text, text=True, capture_output=True)
        if dr.returncode != 0:
            print(dr.stdout)
            print(dr.stderr, file=sys.stderr)
            raise subprocess.CalledProcessError(dr.returncode, dr_cmd, dr.stdout, dr.stderr)
        # Parse totalBytesProcessed from JSON output
        try:
            info = json.loads(dr.stdout)
            # bq may return a list or a single object depending on version
            stats = (info[0] if isinstance(info, list) else info).get("statistics", {})
            total_bytes = int(stats.get("totalBytesProcessed", 0))
        except Exception:
            total_bytes = 0
        est_cost_usd = (total_bytes / 1_000_000_000_000) * 5.0  # $5 per TB on-demand
        print(f"[dry-run] estimated scan: {_format_bytes(total_bytes)} (~${est_cost_usd:,.2f})")
        if max_bytes_billed is not None and total_bytes > max_bytes_billed:
            print(
                f"[guard] abort: estimated bytes {_format_bytes(total_bytes)} exceed limit {_format_bytes(max_bytes_billed)}",
                file=sys.stderr,
            )
            if only_dry_run:
                return
            raise subprocess.CalledProcessError(2, dr_cmd, dr.stdout, "bytes limit exceeded")
        if only_dry_run:
            return

    # Real execution guarded by maximum_bytes_billed
    run_cmd = list(base_cmd)
    if max_bytes_billed is not None:
        run_cmd += ["--maximum_bytes_billed", str(max_bytes_billed)]
    run_cmd += param_flags
    print(f"[run] bq query with {len(params)} params and SQL {sql_path}")
    subprocess.run(run_cmd, input=sql_text, check=True, text=True)


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
    ap.add_argument("--dry-run-first", action="store_true", help="Estimate bytes before running queries")
    ap.add_argument(
        "--only-dry-run",
        action="store_true",
        help="Only perform dry-run estimation without executing queries",
    )
    ap.add_argument(
        "--max-bytes-billed",
        type=int,
        default=int(os.environ.get("BQ_MAX_BYTES_BILLED", "5000000000")),
        help="Maximum bytes billed per query (default from env BQ_MAX_BYTES_BILLED or 5GB)",
    )
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
                max_bytes_billed=args.max_bytes_billed,
                dry_run_first=args.dry_run_first,
                only_dry_run=args.only_dry_run,
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
                max_bytes_billed=args.max_bytes_billed,
                dry_run_first=args.dry_run_first,
                only_dry_run=args.only_dry_run,
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
                max_bytes_billed=args.max_bytes_billed,
                dry_run_first=args.dry_run_first,
                only_dry_run=args.only_dry_run,
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


