#!/usr/bin/env bash
set -euo pipefail

# 0) .env 자동 로드(존재 시). export(-a)로 환경변수 승격
if [ -f ".env" ]; then
  set -a
  . ".env"
  set +a
fi

: "${GCP_PROJECT:?set in env}"
: "${GCS_BUCKET_RAW:?bucket name (no gs://)}"
DS=${BQ_DATASET:-stablecoin_fds}
TABLE_OUT="${DS}.eth_stable_transfers_2023p"
BUCKET_URI="gs://${GCS_BUCKET_RAW}"

echo "[info] project=${GCP_PROJECT} dataset=${DS} bucket=${GCS_BUCKET_RAW}"

# 1) 임시 테이블 생성 (USDC/USDT/DAI, 2023-01-01 이후)
bq --project_id="${GCP_PROJECT}" query --use_legacy_sql=false \
  --destination_table="${TABLE_OUT}" --replace --quiet \
  < ingest/bigquery/sql/eth_stable_transfers.sql | cat

echo "[info] wrote table: ${TABLE_OUT}"

# 2) GCS로 Parquet 내보내기 (리전 일치 필요: 데이터셋/버킷 모두 US 권장)
bq --project_id="${GCP_PROJECT}" extract \
  --destination_format=PARQUET \
  "${TABLE_OUT}" \
  "${BUCKET_URI}/raw/chain=eth/token=stable/date=2023p/part-*.parquet"

echo "[done] exported to ${BUCKET_URI}/raw/chain=eth/token=stable/date=2023p/"

