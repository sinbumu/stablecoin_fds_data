#!/usr/bin/env bash
set -euo pipefail

# .env 로드
if [ -f .env ]; then set -a; . ./.env; set +a; fi

: "${GCP_PROJECT:?set in .env}"
: "${BQ_DATASET:?set in .env or export BQ_DATASET}"

echo "[info] optimizing BigQuery tables in ${BQ_DATASET}"

# classified_transfers: ts 파티션, token/from_addr/to_addr 클러스터, require_partition_filter
bq --project_id="$GCP_PROJECT" update \
  --time_partitioning_field ts \
  --clustering_fields token,from_addr,to_addr \
  --require_partition_filter=true \
  "${BQ_DATASET}.classified_transfers"

# daily_flows: date 파티션, token/category/direction 클러스터, require_partition_filter
bq --project_id="$GCP_PROJECT" update \
  --time_partitioning_field date \
  --clustering_fields token,category,direction \
  --require_partition_filter=true \
  "${BQ_DATASET}.daily_flows"

echo "[done] table optimization applied"


