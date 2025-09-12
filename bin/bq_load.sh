#!/usr/bin/env bash
set -euo pipefail

# .env 로드
if [ -f .env ]; then set -a; . ./.env; set +a; fi

: "${GCP_PROJECT:?set in .env}"
: "${BQ_DATASET:?set in .env or export BQ_DATASET}"

TABLE=${1:?table name}
URI=${2:?gcs uri or wildcard}
PARTITION_FIELD=${3:-}
CLUSTER_FIELDS=${4:-}

echo "[info] loading to ${BQ_DATASET}.${TABLE} from: ${URI}"

bq --project_id="${GCP_PROJECT}" load \
  --source_format=PARQUET \
  --autodetect \
  ${PARTITION_FIELD:+--time_partitioning_field=${PARTITION_FIELD}} \
  ${CLUSTER_FIELDS:+--clustering_fields=${CLUSTER_FIELDS}} \
  "${BQ_DATASET}.${TABLE}" "${URI}"

echo "[done] loaded ${BQ_DATASET}.${TABLE}"


