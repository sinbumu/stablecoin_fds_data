#!/usr/bin/env bash
set -euo pipefail

# .env 로드 (선택)
if [ -f .env ]; then set -a; . ./.env; set +a; fi

SAMPLE=${SAMPLE:-20}

echo "[info] files(count):"
STD_N=$(ls -1 out/std/eth/*_std.parquet 2>/dev/null | wc -l | xargs || true)
LAB_N=$(ls -1 out/labeled/eth/*_labeled.parquet 2>/dev/null | wc -l | xargs || true)
CLS_N=$(ls -1 out/classified/eth/*_classified.parquet 2>/dev/null | wc -l | xargs || true)
echo "  std=$STD_N labeled=$LAB_N classified=$CLS_N"

if [ -f out/agg/daily_flows.parquet ]; then
  echo "[info] agg present: out/agg/daily_flows.parquet"
else
  echo "[warn] agg missing: out/agg/daily_flows.parquet"
fi

# 샘플 파일 목록 준비
mapfile -t CLS_FILES < <(ls -1 out/classified/eth/*_classified.parquet 2>/dev/null | head -n ${SAMPLE})
if [ ${#CLS_FILES[@]} -eq 0 ]; then
  echo "[warn] no classified sample files"
  exit 0
fi

python3 - <<'PY'
import os, re, sys, glob, pandas as pd

def head(path, n=3):
    df = pd.read_parquet(path)
    print(f"== {path} :: shape={df.shape}")
    print(df.head(n))

cls_files = sorted(glob.glob('out/classified/eth/*_classified.parquet'))[:int(os.environ.get('SAMPLE','20'))]
df = pd.concat([pd.read_parquet(p) for p in cls_files], ignore_index=True)

print("[schema] columns:", list(df.columns))
expect = {'ts','chain','tx_hash','log_index','token','token_address','from_addr','to_addr','amount_norm','block_number','category','direction'}
print("[schema] missing:", list(expect - set(df.columns)))

# 값 유효성
print("[checks] neg_amount_norm:", int((df['amount_norm']<0).sum()))
addr_re = re.compile(r'^0x[0-9a-fA-F]{40}$')
bad_addr = (~df['token_address'].fillna('').map(lambda x: bool(addr_re.match(str(x))))).sum()
print("[checks] bad_token_address:", int(bad_addr))

# 전역 중복
dups = df.duplicated(subset=['tx_hash','log_index']).sum()
print("[dedup] global_duplicates:", int(dups))

# 분포
print("[dist] category:")
print(df['category'].value_counts().head(5))
print("[dist] direction:")
print(df['direction'].value_counts().head(5))

# 날짜 범위
dts = pd.to_datetime(df['ts'], utc=True, errors='coerce')
print("[range] ts:", dts.min(), "->", dts.max(), "NaT%:", float(dts.isna().mean()))

# 표본 데이터 미리보기
for p in cls_files[:2]:
    head(p, n=5)

# 집계 대조(표본 기간 상위 7일)
if os.path.exists('out/agg/daily_flows.parquet'):
    agg = pd.read_parquet('out/agg/daily_flows.parquet')
    grp = df.assign(date=dts.dt.date).groupby(['date','chain','token','category','direction'], dropna=False).agg(
        tx_count=('tx_hash','nunique'), total_amount=('amount_norm','sum')
    ).reset_index()
    merged = grp.merge(agg, on=['date','chain','token','category','direction'], how='left', suffixes=('_calc','_agg'))
    mism = (merged['tx_count_calc']!=merged['tx_count']) | ((merged['total_amount_calc']-merged['total_amount']).abs()>1e-6)
    print("[agg-compare] sample rows:")
    print(merged.head(10))
    print("[agg-compare] mismatches:", int(mism.sum()))
else:
    print('[agg-compare] agg file not found')
PY

echo "[done] detailed checks complete"

