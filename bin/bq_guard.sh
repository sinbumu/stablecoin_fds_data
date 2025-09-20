#!/usr/bin/env bash
set -euo pipefail

# bq_guard.sh — Safe wrapper for BigQuery queries with dry-run and max bytes billed
# Usage:
#   bq_guard.sh --sql <path.sql> [--project <id>] [--max-bytes <int>] [--dry-run-only]
#               [--param name:type:value]... [--stdin]
# Notes:
#   - If --stdin is provided, reads SQL from stdin (ignore --sql)
#   - type examples: STRING, DATE, ARRAY<STRING>

PROJECT_ID=${GCP_PROJECT:-}
SQL_FILE=""
MAX_BYTES=${BQ_MAX_BYTES_BILLED:-5000000000} # 5 GB default
DRY_ONLY=0
USE_STDIN=0
declare -a PARAMS

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT_ID="$2"; shift 2;;
    --sql)
      SQL_FILE="$2"; shift 2;;
    --max-bytes)
      MAX_BYTES="$2"; shift 2;;
    --dry-run-only)
      DRY_ONLY=1; shift;;
    --stdin)
      USE_STDIN=1; shift;;
    --param)
      PARAMS+=("$2"); shift 2;;
    *)
      echo "[error] unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ $USE_STDIN -eq 0 && -z "$SQL_FILE" ]]; then
  echo "[error] --sql <path.sql> or --stdin is required" >&2
  exit 2
fi

SQL_TEXT=""
if [[ $USE_STDIN -eq 1 ]]; then
  SQL_TEXT=$(cat)
else
  SQL_TEXT=$(cat "$SQL_FILE")
fi

BASE=(bq ${PROJECT_ID:+--project_id="$PROJECT_ID"} query --use_legacy_sql=false --quiet)

# Build parameter flags
PF=()
if (( ${#PARAMS[@]} > 0 )); then
  for p in "${PARAMS[@]}"; do
    PF+=(--parameter "$p")
  done
fi

echo "[dry-run] estimating bytes..."
DR_CMD=("${BASE[@]}" --dry_run --format=prettyjson)
if (( ${#PF[@]:-0} > 0 )); then DR_CMD+=("${PF[@]}"); fi
if ! OUT=$(printf "%s" "$SQL_TEXT" | "${DR_CMD[@]}" 2>&1); then
  echo "$OUT" >&2
  exit 1
fi

TOTAL=$(echo "$OUT" | python3 - <<'PY'
import json,sys
s=sys.stdin.read()
try:
  o=json.loads(s)
  if isinstance(o,list): o=o[0]
  print(int(o.get('statistics',{}).get('totalBytesProcessed',0)))
except Exception:
  print(0)
PY
)

hum() {
  python3 - "$1" <<'PY'
import sys
n=int(sys.argv[1])
u=['B','KB','MB','GB','TB','PB']
i=0; s=float(n)
while s>=1024 and i<len(u)-1:
  s/=1024; i+=1
print(f"{s:.2f} {u[i]}")
PY
}

echo "[dry-run] estimated scan: $(hum "$TOTAL") (~$$(python3 - <<PY
print(f"{($TOTAL/1_000_000_000_000)*5:.2f}")
PY
))"

if [[ "$TOTAL" -gt "$MAX_BYTES" ]]; then
  echo "[guard] abort: estimated bytes $(hum "$TOTAL") exceed limit $(hum "$MAX_BYTES")" >&2
  exit 3
fi

if [[ $DRY_ONLY -eq 1 ]]; then
  echo "[ok] dry-run only; not executing"
  exit 0
fi

echo "[run] executing with maximum_bytes_billed=$MAX_BYTES"
RUN_CMD=("${BASE[@]}" --maximum_bytes_billed "$MAX_BYTES")
if (( ${#PF[@]:-0} > 0 )); then RUN_CMD+=("${PF[@]}"); fi
printf "%s" "$SQL_TEXT" | "${RUN_CMD[@]}"


