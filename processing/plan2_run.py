import argparse
import json
import os
import subprocess


def run_bq_query_file(sql_path: str, params: dict | None = None) -> None:
    cmd = [
        "bq", "--project_id", os.environ.get("GCP_PROJECT", ""),
        "query", "--use_legacy_sql=false",
    ]
    if params:
        for k, v in params.items():
            if isinstance(v, list):
                cmd += ["--parameter", f"{k}:ARRAY<STRING>:" + ",".join(v)]
            else:
                cmd += ["--parameter", f"{k}:STRING:{v}"]
    cmd += ["<", sql_path]
    subprocess.run(" ".join(cmd), shell=True, check=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="PLAN2 runner")
    ap.add_argument("--date-start", default="2023-01-01")
    ap.add_argument("--date-end", default="2025-12-31")
    args = ap.parse_args()

    # 1) block helpers
    subprocess.run(
        f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_block_helpers.sql",
        shell=True, check=True
    )

    # 2) chainlink raw
    feeds = [
        "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6".lower(),
        "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D".lower(),
        "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9".lower(),
    ]
    params = {
        "feed_addresses": feeds,
        "answer_updated_topic": "0x",  # TODO: 채우기
        "date_start": args.date_start,
        "date_end": args.date_end,
    }
    run_bq_query_file("ingest/bigquery/sql/eth_chainlink_prices_raw.sql", params)

    # 3) uniswap v3 swaps raw
    pools = [
        "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640".lower(),
        "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8".lower(),
        "0x5777d92f208679db4b9778590fa3cab3ac9e2168".lower(),
    ]
    params = {
        "target_pools": pools,
        "univ3_swap_topic": "0x",  # TODO: 채우기
        "date_start": args.date_start,
        "date_end": args.date_end,
    }
    run_bq_query_file("ingest/bigquery/sql/eth_uniswap_v3_swaps_raw.sql", params)

    # 4) freeze events
    params = {
        "usdc_addr": "0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48".lower(),
        "usdt_addr": "0xdAC17F958D2ee523a2206206994597C13D831ec7".lower(),
        "freeze_event_topics": ["0x"],  # TODO: 채우기
        "date_start": args.date_start,
        "date_end": args.date_end,
    }
    run_bq_query_file("ingest/bigquery/sql/eth_freeze_events.sql", params)

    # 5) build view
    subprocess.run(
        f"bq --project_id={os.environ.get('GCP_PROJECT','')} query --use_legacy_sql=false < processing/build_prs_crs_view.sql",
        shell=True, check=True
    )

    print("[done] PLAN2 basic ingestion/view build complete")


if __name__ == "__main__":
    main()


