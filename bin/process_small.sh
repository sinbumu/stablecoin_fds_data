#!/usr/bin/env bash
set -euo pipefail

# .env 로드
if [ -f .env ]; then set -a; . ./.env; set +a; fi

: "${GCS_BUCKET_RAW:?set in .env}"

mkdir -p out/std/eth out/labeled/eth out/classified/eth out/agg out/logs

# 1) 표준화: GCS 원시 상위 N개 파일만 로컬로 처리 (기본 10)
COUNT=${COUNT:-10}
LIST=$(gcloud storage ls gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/ | head -n ${COUNT})

echo "[info] standardize small batch (${COUNT} files)"
for f in $LIST; do
  base=$(basename "${f%.parquet}")
  python3 processing/standardize.py \
    --input "$f" \
    --out "out/std/eth/${base}_std.parquet" \
    --chain eth --token unknown || echo "[warn] standardize failed: $f" | tee -a out/logs/process_small.log
done

# 2) 라벨 병합
for f in out/std/eth/*_std.parquet; do
  [ -e "$f" ] || continue
  base=$(basename "${f%_std.parquet}")
  python3 processing/label_merge.py \
    --transfers "$f" \
    --labels configs/labels_seed.csv \
    --out "out/labeled/eth/${base}_labeled.parquet" || echo "[warn] label_merge failed: $f" | tee -a out/logs/process_small.log
done

# 3) 컨텍스트 분류
for f in out/labeled/eth/*_labeled.parquet; do
  [ -e "$f" ] || continue
  base=$(basename "${f%_labeled.parquet}")
  python3 processing/classify_context.py \
    --input "$f" \
    --out "out/classified/eth/${base}_classified.parquet" || echo "[warn] classify failed: $f" | tee -a out/logs/process_small.log
done

# 4) 집계(샘플 합본→집계)
python3 - <<'PY'
import glob, pandas as pd, os
files = glob.glob("out/classified/eth/*_classified.parquet")
if files:
  df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
  os.makedirs("out/classified/eth_all", exist_ok=True)
  df.to_parquet("out/classified/eth_all/all.parquet", index=False)
PY

if [ -f out/classified/eth_all/all.parquet ]; then
  python3 processing/aggregate_daily.py \
    --input out/classified/eth_all/all.parquet \
    --out   out/agg/daily_flows.parquet
fi

echo "[done] process_small finished"

