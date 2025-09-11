#!/usr/bin/env bash
set -euo pipefail

# .env 로드
if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${GCS_BUCKET_RAW:?set in .env}"

mkdir -p out/std/eth out/labeled/eth out/classified/eth out/agg out/logs

# 디버그/로깅 설정
LOG=out/logs/process_all_incremental.log
if [ "${DEBUG:-0}" = "1" ]; then
  echo "[debug] enabling xtrace -> $LOG"
  exec > >(tee -a "$LOG") 2>&1
  set -x
fi

COUNT=${COUNT:-}
FILES_CMD="gcloud storage ls gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/"
if [ -n "${COUNT}" ]; then
  echo "[info] incremental COUNT limit: ${COUNT}"
  FILES_CMD="${FILES_CMD} | head -n ${COUNT}"
fi

# 부분 집계 누적 파일
AGG_TMP=out/agg/daily_flows_incremental.parquet

# CLEAR_AGG=1 이면 기존 누적 집계 초기화
if [ "${CLEAR_AGG:-0}" = "1" ] && [ -f "$AGG_TMP" ]; then
  rm -f "$AGG_TMP"
  echo "[info] cleared previous incremental aggregate: $AGG_TMP"
fi

# 1) 표준화/라벨/분류를 파일 단위로 처리하면서, 즉시 부분 집계 생성 및 누적
eval "$FILES_CMD" | while IFS= read -r f; do
  [ -z "$f" ] && continue
  echo "[file] $f"
  base=$(basename "${f%.parquet}")
  std="out/std/eth/${base}_std.parquet"
  lab="out/labeled/eth/${base}_labeled.parquet"
  cls="out/classified/eth/${base}_classified.parquet"

  # 표준화
  if [ ! -f "$std" ]; then
    echo "[std] -> $std"
    python3 processing/standardize.py --input "$f" --out "$std" --chain eth --token unknown \
      2>&1 | tee -a out/logs/process_all_incremental.log
  fi

  # 라벨
  if [ -f "$std" ] && [ ! -f "$lab" ]; then
    echo "[label] -> $lab"
    python3 processing/label_merge.py --transfers "$std" --labels configs/labels_seed.csv --out "$lab" \
      2>&1 | tee -a out/logs/process_all_incremental.log
  fi

  # 분류
  if [ -f "$lab" ] && [ ! -f "$cls" ]; then
    echo "[classify] -> $cls"
    python3 processing/classify_context.py --input "$lab" --out "$cls" \
      2>&1 | tee -a out/logs/process_all_incremental.log
  fi

  # 부분 집계(개별 파일) → 누적 병합
  if [ -f "$cls" ]; then
    echo "[partial-agg] from $cls"
    python3 - <<PY 2>&1 | tee -a out/logs/process_all_incremental.log
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
    # 항상 최종 산출물도 갱신(중단되어도 최신 상태 유지)
    if [ -f "$AGG_TMP" ]; then
      cp -f "$AGG_TMP" out/agg/daily_flows.parquet
    fi
    # 옵션: 중간 산출물 정리로 용량 확보 가능
    # rm -f "$std" "$lab" "$cls"
  fi
done

# 최종 산출물 이름으로 저장
if [ -f "$AGG_TMP" ]; then
  mv "$AGG_TMP" out/agg/daily_flows.parquet
fi

echo "[done] process_all_incremental finished"
