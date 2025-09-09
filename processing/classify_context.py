import argparse
import pandas as pd

CATEGORY_DIRECT = 'DIRECT_TRANSFER'
CATEGORY_DEX = 'DEX_SWAP'
CATEGORY_DEFI = 'DEFI_OTHER'

DIRECTION_P2P = 'P2P'
DIRECTION_DEX_IN = 'DEX_IN'
DIRECTION_DEX_OUT = 'DEX_OUT'
DIRECTION_CEX_IN = 'CEX_IN'
DIRECTION_CEX_OUT = 'CEX_OUT'


def classify(row) -> tuple[str, str]:
    from_t = row.get('from_type')
    to_t = row.get('to_type')
    # 기본값
    category = CATEGORY_DIRECT
    direction = DIRECTION_P2P

    dex_types = {'DEX_POOL','DEX_ROUTER'}

    if (from_t in dex_types) or (to_t in dex_types):
        category = CATEGORY_DEX
        if to_t in dex_types:
            direction = DIRECTION_DEX_IN
        elif from_t in dex_types:
            direction = DIRECTION_DEX_OUT
    elif (from_t == 'CEX') or (to_t == 'CEX'):
        category = CATEGORY_DIRECT
        if to_t == 'CEX':
            direction = DIRECTION_CEX_IN
        elif from_t == 'CEX':
            direction = DIRECTION_CEX_OUT

    return category, direction


def run(input_path: str, out_path: str):
    df = pd.read_parquet(input_path) if input_path.endswith('.parquet') else pd.read_csv(input_path)
    cats, dirs = zip(*df.apply(classify, axis=1))
    df['category'] = cats
    df['direction'] = dirs
    if out_path.endswith('.parquet'):
        df.to_parquet(out_path, index=False)
    else:
        df.to_csv(out_path, index=False)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    run(args.input, args.out)
