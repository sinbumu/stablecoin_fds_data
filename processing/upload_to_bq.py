import argparse
import os
import subprocess


def run(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, text=True)
    if proc.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(cmd)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GCP_PROJECT", ""))
    ap.add_argument("--dataset", default=os.environ.get("BQ_DATASET", "stablecoin_fds"))
    ap.add_argument("--bucket", default=os.environ.get("GCS_BUCKET_PROCESSED", ""))
    ap.add_argument("--prefix", default="std/chain=eth")
    ap.add_argument("--upload", nargs="*", default=["classified","agg","std"], help="which outputs to upload: classified, agg, std")
    args = ap.parse_args()

    if not args.project:
        raise SystemExit("--project or GCP_PROJECT is required")
    if not args.bucket:
        raise SystemExit("--bucket or GCS_BUCKET_PROCESSED is required")

    # classified uploads
    if "classified" in args.upload:
        uri = f"gs://{args.bucket}/{args.prefix}/classified/*.parquet"
        run([
            "bash","bin/bq_load.sh",
            "classified_transfers",
            uri,
            "ts",
            "token,from_addr,to_addr",
        ])

    # agg upload (single parquet)
    if "agg" in args.upload:
        # 먼저 GCS로 업로드
        local_agg = "out/agg/daily_flows.parquet"
        if not os.path.exists(local_agg):
            raise SystemExit("local agg not found: out/agg/daily_flows.parquet")
        uri = f"gs://{args.bucket}/{args.prefix}/agg/daily_flows.parquet"
        run(["gcloud","storage","cp", local_agg, uri])
        run([
            "bash","bin/bq_load.sh",
            "daily_flows",
            uri,
            "date",
            "token,category,direction",
        ])

    # std uploads (optional, heavy)
    if "std" in args.upload:
        uri = f"gs://{args.bucket}/{args.prefix}/std/eth/*.parquet"
        # 선택: 로컬 std를 먼저 업로드
        # gcloud storage cp -r out/std/eth/*.parquet gs://$bucket/$prefix/std/eth/
        run([
            "bash","bin/bq_load.sh",
            "std_token_transfers",
            uri,
            "ts",
            "token,from_addr,to_addr",
        ])

if __name__ == "__main__":
    main()


