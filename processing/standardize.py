import os
import sys
import argparse
import pandas as pd
from datetime import timezone


def normalize_amount(amount_raw: int, decimals: int) -> float:
    return float(amount_raw) / (10 ** int(decimals))


def standardize_file(input_path: str, chain: str, token: str) -> pd.DataFrame:
    df = pd.read_parquet(input_path) if input_path.endswith('.parquet') else pd.read_csv(input_path)
    # 기대 컬럼: ts, tx_hash, from_addr/from, to_addr/to, amount_raw, decimals, block/block_number
    rename_map = {
        'from': 'from_addr', 'to': 'to_addr', 'block': 'block_number'
    }
    df = df.rename(columns=rename_map)
    if 'from_addr' not in df.columns and 'from' in df.columns:
        df['from_addr'] = df['from']
    if 'to_addr' not in df.columns and 'to' in df.columns:
        df['to_addr'] = df['to']
    if 'block_number' not in df.columns and 'block' in df.columns:
        df['block_number'] = df['block']
    # 필수 컬럼 체크
    required = ['ts', 'tx_hash', 'from_addr', 'to_addr', 'amount_raw', 'decimals', 'block_number']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing} in {input_path}")
    # 타입/정규화
    df['ts'] = pd.to_datetime(df['ts'], unit='ns', errors='coerce') if df['ts'].dtype == 'int64' else pd.to_datetime(df['ts'], errors='coerce')
    df['ts'] = df['ts'].dt.tz_localize('UTC', nonexistent='shift_forward', ambiguous='NaT', errors='ignore') if df['ts'].dt.tz is None else df['ts'].dt.tz_convert('UTC')
    df['chain'] = chain
    df['token'] = token
    df['amount_norm'] = df.apply(lambda r: normalize_amount(r['amount_raw'], r['decimals']), axis=1)
    cols = ['ts','chain','tx_hash','log_index','from_addr','to_addr','token','amount_raw','decimals','amount_norm','block_number']
    for c in ['log_index']:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True, help='입력 파일(.csv/.parquet)')
    ap.add_argument('--chain', required=True, choices=['eth','bsc','tron'])
    ap.add_argument('--token', required=True, choices=['usdt','usdc','dai'])
    ap.add_argument('--out', required=True, help='출력 경로(.parquet 권장)')
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = standardize_file(args.input, args.chain, args.token)
    if args.out.endswith('.parquet'):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)


if __name__ == '__main__':
    main()
