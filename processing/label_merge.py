import argparse
import pandas as pd

def load_labels(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    assert {'address','label'}.issubset(set(df.columns)), 'labels_seed.csv must have address,label columns'
    df['address'] = df['address'].str.lower()
    return df[['address','label']]


def merge_labels(transfers_path: str, labels_path: str, out_path: str):
    t = pd.read_parquet(transfers_path) if transfers_path.endswith('.parquet') else pd.read_csv(transfers_path)
    labels = load_labels(labels_path)
    for col in ['from_addr','to_addr']:
        t[col] = t[col].str.lower()
    t = t.merge(labels.rename(columns={'address':'from_addr', 'label':'from_type'}), on='from_addr', how='left')
    t = t.merge(labels.rename(columns={'address':'to_addr', 'label':'to_type'}), on='to_addr', how='left')
    if out_path.endswith('.parquet'):
        t.to_parquet(out_path, index=False)
    else:
        t.to_csv(out_path, index=False)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--transfers', required=True)
    ap.add_argument('--labels', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    merge_labels(args.transfers, args.labels, args.out)
