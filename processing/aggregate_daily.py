import argparse
import pandas as pd


def aggregate(input_path: str, out_path: str):
    df = pd.read_parquet(input_path) if input_path.endswith('.parquet') else pd.read_csv(input_path)
    df['date'] = pd.to_datetime(df['ts']).dt.tz_convert('UTC').dt.date if hasattr(pd.to_datetime(df['ts']), 'dt') else pd.to_datetime(df['ts']).dt.date
    group_cols = ['date','chain','token','category','direction']
    agg = df.groupby(group_cols, dropna=False).agg(
        tx_count=('tx_hash','nunique'),
        total_amount=('amount_norm','sum')
    ).reset_index()
    if out_path.endswith('.parquet'):
        agg.to_parquet(out_path, index=False)
    else:
        agg.to_csv(out_path, index=False)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    aggregate(args.input, args.out)
