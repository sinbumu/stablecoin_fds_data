#!/usr/bin/env bash
set -euo pipefail

# .env 로드 (선택)
if [ -f .env ]; then set -a; . ./.env; set +a; fi

ok() { printf "\033[32m✔ %s\033[0m\n" "$1"; }
warn() { printf "\033[33m! %s\033[0m\n" "$1"; }
err() { printf "\033[31m✘ %s\033[0m\n" "$1"; }

# 1) 파일 수
STD_N=$(ls -1 out/std/eth/*_std.parquet 2>/dev/null | wc -l | xargs || true)
LAB_N=$(ls -1 out/labeled/eth/*_labeled.parquet 2>/dev/null | wc -l | xargs || true)
CLS_N=$(ls -1 out/classified/eth/*_classified.parquet 2>/dev/null | wc -l | xargs || true)
[ "$STD_N" != "0" ] && ok "std files: $STD_N" || err "std files: 0"
[ "$LAB_N" != "0" ] && ok "labeled files: $LAB_N" || warn "labeled files: 0"
[ "$CLS_N" != "0" ] && ok "classified files: $CLS_N" || warn "classified files: 0"

# 2) 집계 파일 존재
if [ -f out/agg/daily_flows.parquet ]; then
  ok "agg file present: out/agg/daily_flows.parquet"
else
  warn "missing: out/agg/daily_flows.parquet"
fi

# 3) 스키마/샘플/유일성/라벨 커버리지/카테고리 분포 요약 (파이썬 사용)
python3 - <<'PY'
import glob, pandas as pd, os

summary = []

def safe_read(path):
    try:
        return pd.read_parquet(path)
    except Exception as e:
        return None

# 표준화 샘플
std_files = sorted(glob.glob('out/std/eth/*_std.parquet'))
if std_files:
    df = safe_read(std_files[0])
    if df is not None:
        summary.append(f"std shape={df.shape} cols={list(df.columns)}")
        ndup = df.duplicated(subset=['tx_hash','log_index']).sum() if {'tx_hash','log_index'}.issubset(df.columns) else 'NA'
        summary.append(f"std duplicates(tx_hash,log_index)={ndup}")

# 라벨 샘플
lab_files = sorted(glob.glob('out/labeled/eth/*_labeled.parquet'))
if lab_files:
    df = safe_read(lab_files[0])
    if df is not None:
        c_from = df['from_type'].notna().mean() if 'from_type' in df.columns else 0
        c_to = df['to_type'].notna().mean() if 'to_type' in df.columns else 0
        summary.append(f"label coverage from={c_from:.2%} to={c_to:.2%}")

# 분류 샘플
cls_files = sorted(glob.glob('out/classified/eth/*_classified.parquet'))
if cls_files:
    df = safe_read(cls_files[0])
    if df is not None:
        vc_cat = df['category'].value_counts().head(5).to_dict() if 'category' in df.columns else {}
        vc_dir = df['direction'].value_counts().head(5).to_dict() if 'direction' in df.columns else {}
        summary.append(f"category top5={vc_cat}")
        summary.append(f"direction top5={vc_dir}")

# 집계 샘플
if os.path.exists('out/agg/daily_flows.parquet'):
    df = safe_read('out/agg/daily_flows.parquet')
    if df is not None:
        summary.append(f"agg shape={df.shape}")
        summary.append(str(df.head(10)))

print("\n".join(summary) or "no outputs to summarize")
PY

# 4) 용량 요약
STD_S=$(du -sh out/std/eth 2>/dev/null | awk '{print $1}')
LAB_S=$(du -sh out/labeled/eth 2>/dev/null | awk '{print $1}')
CLS_S=$(du -sh out/classified/eth 2>/dev/null | awk '{print $1}')
AGG_S=$(du -sh out/agg 2>/dev/null | awk '{print $1}')
printf "sizes: std=%s labeled=%s classified=%s agg=%s\n" "${STD_S:-0}" "${LAB_S:-0}" "${CLS_S:-0}" "${AGG_S:-0}"

exit 0
