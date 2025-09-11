#!/usr/bin/env bash
set -euo pipefail

# .env 로드
if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${GCS_BUCKET_RAW:?set in .env}"

mkdir -p out/std/eth out/labeled/eth out/classified/eth out/agg out/logs

LIST=$(gcloud storage ls gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/)

# 부분 집계 누적 파일
AGG_TMP=out/agg/daily_flows_incremental.parquet

# 1) 표준화/라벨/분류를 파일 단위로 처리하면서, 즉시 부분 집계 생성 및 누적
for f in $LIST; do
  base=$(basename "${f%.parquet}")
  std="out/std/eth/${base}_std.parquet"
  lab="out/labeled/eth/${base}_labeled.parquet"
  cls="out/classified/eth/${base}_classified.parquet"

  # 표준화
  if [ ! -f "$std" ]; then
    python3 processing/standardize.py --input "$f" --out "$std" --chain eth --token unknown \
      || echo "[warn] standardize failed: $f" | tee -a out/logs/process_all_incremental.log
  fi

  # 라벨
  if [ -f "$std" ] && [ ! -f "$lab" ]; then
    python3 processing/label_merge.py --transfers "$std" --labels configs/labels_seed.csv --out "$lab" \
      || echo "[warn] label_merge failed: $std" | tee -a out/logs/process_all_incremental.log
  fi

  # 분류
  if [ -f "$lab" ] && [ ! -f "$cls" ]; then
    python3 processing/classify_context.py --input "$lab" --out "$cls" \
      || echo "[warn] classify failed: $lab" | tee -a out/logs/process_all_incremental.log
  fi

  # 부분 집계(개별 파일) → 누적 병합
  if [ -f "$cls" ]; then
    python3 - <<PY
import pandas as pd, os
cls = "${cls}"
agg_tmp = "${AGG_TMP}"
df = pd.read_parquet(cls)
df['date'] = pd.to_datetime(df['ts'], utc=True).dt.date
agg = df.groupby(['date','chain','token','category','direction'], dropna=False, observed=False).agg(
    tx_count=('tx_hash','nunique'), total_amount=('amount_norm','sum')
).reset_index()
if os.path.exists(agg_tmp):
    prev = pd.read_parquet(agg_tmp)
    both = pd.concat([prev, agg], ignore_index=True)
    both = both.groupby(['date','chain','token','category','direction'], dropna=False, observed=False).agg(
        tx_count=('tx_count','sum'), total_amount=('total_amount','sum')
    ).reset_index()
    both.to_parquet(agg_tmp, index=False)
else:
    agg.to_parquet(agg_tmp, index=False)
PY
    # 옵션: 중간 산출물 정리로 용량 확보 가능
    # rm -f "$std" "$lab" "$cls"
  fi

done

# 최종 산출물 이름으로 저장
if [ -f "$AGG_TMP" ]; then
  mv "$AGG_TMP" out/agg/daily_flows.parquet
fi

echo "[done] process_all_incremental finished"
