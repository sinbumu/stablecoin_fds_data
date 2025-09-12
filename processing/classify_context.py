import argparse
import pandas as pd

# Category ENUM
CATEGORY_DIRECT = 'DIRECT_TRANSFER'
CATEGORY_DEX = 'DEX_SWAP'
CATEGORY_DEFI = 'DEFI_OTHER'
CATEGORY_MINT = 'MINT'
CATEGORY_BURN = 'BURN'

# Direction ENUM
DIRECTION_P2P = 'P2P'
DIRECTION_DEX_IN = 'DEX_IN'
DIRECTION_DEX_OUT = 'DEX_OUT'
DIRECTION_CEX_IN = 'CEX_IN'
DIRECTION_CEX_OUT = 'CEX_OUT'
DIRECTION_ISSUER_IN = 'ISSUER_IN'
DIRECTION_ISSUER_OUT = 'ISSUER_OUT'


def classify(row) -> tuple[str, str]:
    from_t = row.get('from_type')
    to_t = row.get('to_type')
    from_addr = str(row.get('from_addr') or '')
    to_addr = str(row.get('to_addr') or '')

    # 기본값
    category = CATEGORY_DIRECT
    direction = DIRECTION_P2P

    # ENUM 세트(안전망)
    valid_categories = {
        CATEGORY_DIRECT, CATEGORY_DEX, CATEGORY_DEFI, CATEGORY_MINT, CATEGORY_BURN
    }
    valid_directions = {
        DIRECTION_P2P, DIRECTION_DEX_IN, DIRECTION_DEX_OUT,
        DIRECTION_CEX_IN, DIRECTION_CEX_OUT, DIRECTION_ISSUER_IN, DIRECTION_ISSUER_OUT
    }

    # 1) Mint/Burn (제로어드레스 기반)
    ZERO = '0x0000000000000000000000000000000000000000'
    if from_addr == ZERO and to_addr != ZERO:
        return CATEGORY_MINT, DIRECTION_ISSUER_OUT
    if to_addr == ZERO and from_addr != ZERO:
        return CATEGORY_BURN, DIRECTION_ISSUER_IN

    # 2) DEX 관련
    dex_types = {'DEX_POOL', 'DEX_ROUTER'}
    if (from_t in dex_types) or (to_t in dex_types):
        category = CATEGORY_DEX
        if to_t in dex_types:
            direction = DIRECTION_DEX_IN
        elif from_t in dex_types:
            direction = DIRECTION_DEX_OUT
    # 3) CEX 관련
    elif (from_t == 'CEX') or (to_t == 'CEX'):
        category = CATEGORY_DIRECT
        if to_t == 'CEX':
            direction = DIRECTION_CEX_IN
        elif from_t == 'CEX':
            direction = DIRECTION_CEX_OUT
    # 4) 그 외는 기본값 유지(DIRECT_TRANSFER, P2P)

    # ENUM 고정(유효성 보정)
    if category not in valid_categories:
        category = CATEGORY_DIRECT
    if direction not in valid_directions:
        direction = DIRECTION_P2P
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
