import os
import argparse
import pandas as pd
from typing import Dict, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None


def normalize_amount(amount_raw: int, decimals: int) -> float:
    return float(amount_raw) / (10 ** int(decimals))


def load_token_metadata(config_path: str) -> Dict[str, Tuple[str, int]]:
    if yaml is None:
        raise RuntimeError("pyyaml이 필요합니다. requirements.txt에 pyyaml 추가 후 설치하세요.")
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
    mapping: Dict[str, Tuple[str, int]] = {}
    eth = data.get("ethereum", {}).get("tokens", {})
    for token, meta in eth.items():
        addr = str(meta.get("address", "")).lower()
        dec = int(meta.get("decimals", 0))
        if addr:
            mapping[addr] = (token, dec)
    return mapping


def standardize_file(input_path: str, chain: str, token_fallback: str, config_path: str) -> pd.DataFrame:
    df = pd.read_parquet(input_path) if input_path.endswith('.parquet') else pd.read_csv(input_path)

    # 기대 컬럼: ts, tx_hash, log_index(옵션), token_address, from_addr, to_addr, amount_raw, block_number
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
    if 'log_index' not in df.columns:
        df['log_index'] = pd.NA

    # 주소/컨트랙트 소문자 통일
    for c in ['from_addr', 'to_addr', 'token_address']:
        if c in df.columns:
            df[c] = df[c].astype(str).str.lower()

    # 필수 컬럼 체크 (decimals는 이후 매핑으로 보강)
    required = ['ts', 'tx_hash', 'from_addr', 'to_addr', 'amount_raw', 'block_number']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing} in {input_path}")

    # ts UTC 통일
    df['ts'] = pd.to_datetime(df['ts'], utc=True, errors='coerce')

    # decimals/token 보강: ethereum_only.yaml의 token_address 매핑
    mapping = load_token_metadata(config_path) if os.path.exists(config_path) else {}
    if 'token_address' in df.columns and mapping:
        meta = df['token_address'].map(mapping)
        if 'decimals' not in df.columns or df['decimals'].isna().any():
            df['decimals'] = meta.map(lambda x: x[1] if isinstance(x, tuple) else pd.NA)
        if 'token' not in df.columns or df['token'].isna().any():
            df['token'] = meta.map(lambda x: x[0] if isinstance(x, tuple) else pd.NA)

    # token_fallback 적용
    if 'token' not in df.columns or df['token'].isna().all():
        df['token'] = token_fallback

    # 정규화 금액
    if 'decimals' not in df.columns or df['decimals'].isna().any():
        raise ValueError("decimals를 결정할 수 없습니다. 입력 또는 configs/ethereum_only.yaml을 확인하세요.")
    df['amount_norm'] = df.apply(lambda r: normalize_amount(r['amount_raw'], r['decimals']), axis=1)

    # 체인 고정
    df['chain'] = chain

    # 출력 컬럼 순서
    cols = [
        'ts','chain','tx_hash','log_index',
        'token','token_address','from_addr','to_addr',
        'amount_raw','decimals','amount_norm','block_number'
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--input', required=True, help='입력 파일(.csv/.parquet) 또는 GCS URI')
    ap.add_argument('--chain', default='eth', choices=['eth'])
    ap.add_argument('--token', default='unknown', choices=['usdt','usdc','dai','unknown'])
    ap.add_argument('--config', default='configs/ethereum_only.yaml')
    ap.add_argument('--out', required=True, help='출력 경로(.parquet 권장)')
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df = standardize_file(args.input, args.chain, args.token, args.config)
    if args.out.endswith('.parquet'):
        df.to_parquet(args.out, index=False)
    else:
        df.to_csv(args.out, index=False)


if __name__ == '__main__':
    main()
