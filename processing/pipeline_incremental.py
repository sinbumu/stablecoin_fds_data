import argparse
import os
import sys
import subprocess
from typing import List
import pandas as pd

try:
    import fsspec  # type: ignore
except Exception:
    fsspec = None


def log(line: str, log_path: str) -> None:
    print(line, flush=True)
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def list_files(pattern: str, count: int | None, log_path: str) -> List[str]:
    files: List[str] = []
    if pattern.startswith("gs://"):
        if fsspec is None:
            raise RuntimeError("gcsfs/fsspec 필요. requirements.txt 설치 확인.")
        fs = fsspec.filesystem("gcs")
        files = fs.glob(pattern)
        # gcsfs.glob는 'bucket/path/..' 형태를 반환할 수 있으므로 gs:// 접두부 보정
        norm: List[str] = []
        for p in files:
            if not p.startswith("gs://"):
                if "://" not in p:
                    p = "gs://" + p
            norm.append(p)
        files = norm
    else:
        import glob

        files = sorted(glob.glob(pattern))
    if count and count > 0:
        files = files[:count]
    log(f"[info] files={len(files)} (pattern={pattern})", log_path)
    for p in files[:5]:
        log(f"  - {p}", log_path)
    return files


def run_cmd(cmd: list[str], log_path: str) -> None:
    log(f"[cmd] {' '.join(cmd)}", log_path)
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.stdout:
        log(proc.stdout.rstrip(), log_path)
    if proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}")


def partial_aggregate(classified_path: str, agg_tmp: str, final_out: str, log_path: str) -> None:
    log(f"[partial-agg] from {classified_path}", log_path)
    df = pd.read_parquet(classified_path)
    df["date"] = pd.to_datetime(df["ts"], utc=True).dt.date
    group_cols = ["date", "chain", "token", "category", "direction"]
    agg = (
        df.groupby(group_cols, dropna=False, observed=False)
        .agg(tx_count=("tx_hash", "nunique"), total_amount=("amount_norm", "sum"))
        .reset_index()
    )
    if os.path.exists(agg_tmp):
        prev = pd.read_parquet(agg_tmp)
        both = pd.concat([prev, agg], ignore_index=True)
        both = (
            both.groupby(group_cols, dropna=False, observed=False)
            .agg(tx_count=("tx_count", "sum"), total_amount=("total_amount", "sum"))
            .reset_index()
        )
        both.to_parquet(agg_tmp, index=False)
    else:
        os.makedirs(os.path.dirname(agg_tmp), exist_ok=True)
        agg.to_parquet(agg_tmp, index=False)
    # keep final always in sync for inspection
    if os.path.exists(agg_tmp):
        os.makedirs(os.path.dirname(final_out), exist_ok=True)
        pd.read_parquet(agg_tmp).to_parquet(final_out, index=False)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--pattern",
        default=os.environ.get(
            "GCS_PATTERN",
            f"gs://{os.environ.get('GCS_BUCKET_RAW', '')}/raw/chain=eth/token=stable/date=2023p/*.parquet",
        ),
        help="입력 파일 패턴 (gs://.. 또는 로컬 글롭)",
    )
    ap.add_argument("--count", type=int, default=0, help="처리할 최대 파일 수 (0=전부)")
    ap.add_argument("--clear-agg", action="store_true", help="기존 누적 집계 초기화")
    ap.add_argument("--log", default="out/logs/pipeline_incremental.log", help="로그 파일 경로")
    args = ap.parse_args()

    log_path = args.log
    log("[start] pipeline_incremental", log_path)
    os.makedirs("out/std/eth", exist_ok=True)
    os.makedirs("out/labeled/eth", exist_ok=True)
    os.makedirs("out/classified/eth", exist_ok=True)
    os.makedirs("out/agg", exist_ok=True)

    files = list_files(args.pattern, args.count if args.count > 0 else None, log_path)
    agg_tmp = "out/agg/daily_flows_incremental.parquet"
    final_out = "out/agg/daily_flows.parquet"
    if args.clear_agg and os.path.exists(agg_tmp):
        os.remove(agg_tmp)
        log(f"[info] cleared previous aggregate: {agg_tmp}", log_path)

    failed: list[str] = []
    for uri in files:
        base = os.path.basename(uri[:-8]) if uri.endswith(".parquet") else os.path.basename(uri)
        std = f"out/std/eth/{base}_std.parquet"
        lab = f"out/labeled/eth/{base}_labeled.parquet"
        cls = f"out/classified/eth/{base}_classified.parquet"

        try:
            if not os.path.exists(std):
                log(f"[std] -> {std}", log_path)
                run_cmd([
                    sys.executable,
                    "processing/standardize.py",
                    "--input",
                    uri,
                    "--out",
                    std,
                    "--chain",
                    "eth",
                    "--token",
                    "unknown",
                ], log_path)

            if os.path.exists(std) and not os.path.exists(lab):
                log(f"[label] -> {lab}", log_path)
                run_cmd([
                    sys.executable,
                    "processing/label_merge.py",
                    "--transfers",
                    std,
                    "--labels",
                    "configs/labels_seed.csv",
                    "--out",
                    lab,
                ], log_path)

            if os.path.exists(lab) and not os.path.exists(cls):
                log(f"[classify] -> {cls}", log_path)
                run_cmd([
                    sys.executable,
                    "processing/classify_context.py",
                    "--input",
                    lab,
                    "--out",
                    cls,
                ], log_path)

            if os.path.exists(cls):
                partial_aggregate(cls, agg_tmp, final_out, log_path)
        except Exception as e:
            log(f"[error] {uri} -> {e}", log_path)
            failed.append(uri)

    if failed:
        fail_list = "out/logs/failed_files.txt"
        with open(fail_list, "w", encoding="utf-8") as f:
            f.write("\n".join(failed))
        log(f"[warn] failed files saved: {fail_list}", log_path)

    log("[done] pipeline_incremental", log_path)


if __name__ == "__main__":
    main()


