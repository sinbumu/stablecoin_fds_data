set -euo pipefail
set -a; [ -f .env ] && . ./.env; set +a
F=$(gcloud storage ls gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/ | head -n 1)
base=$(basename ${F%.parquet})
mkdir -p out/std/eth out/labeled/eth out/classified/eth out/agg out/logs

python3 processing/standardize.py --input "$F" \
  --out "out/std/eth/${base}_std.parquet" --chain eth --token unknown \
  2>&1 | tee -a out/logs/process_all_incremental.log

python3 processing/label_merge.py --transfers "out/std/eth/${base}_std.parquet" \
  --labels configs/labels_seed.csv \
  --out "out/labeled/eth/${base}_labeled.parquet" \
  2>&1 | tee -a out/logs/process_all_incremental.log

python3 processing/classify_context.py --input "out/labeled/eth/${base}_labeled.parquet" \
  --out "out/classified/eth/${base}_classified.parquet" \
  2>&1 | tee -a out/logs/process_all_incremental.log

python3 - <<'PY' 2>&1 | tee -a out/logs/process_all_incremental.log
import pandas as pd, os, sys
cls = sys.argv[1]
agg_tmp = sys.argv[2]
df = pd.read_parquet(cls)
df['date'] = pd.to_datetime(df['ts'], utc=True).dt.date
agg = df.groupby(['date','chain','token','category','direction'], dropna=False, observed=False)\
        .agg(tx_count=('tx_hash','nunique'), total_amount=('amount_norm','sum')).reset_index()
if os.path.exists(agg_tmp):
    prev = pd.read_parquet(agg_tmp)
    both = pd.concat([prev, agg], ignore_index=True)
    both = both.groupby(['date','chain','token','category','direction'], dropna=False, observed=False)\
               .agg(tx_count=('tx_count','sum'), total_amount=('total_amount','sum')).reset_index()
    both.to_parquet(agg_tmp, index=False)
else:
    agg.to_parquet(agg_tmp, index=False)
print("partial-agg ok")
PY "out/classified/eth/${base}_classified.parquet" "out/agg/daily_flows_incremental.parquet"