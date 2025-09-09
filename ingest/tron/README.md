# Tron TRC-20 전송 수집(TronScan API)

- 환경: `.env`에 `TRON_API_KEY`(선택) 설정 권장
- 실행 예시:
  - 2023-01-01 ~ 2023-01-07(UTC) 범위 수집:

```bash
python ingest/tron/tronscan_pull.py \
  --start 1672531200000 --end 1673136000000 \
  --out out/tron/usdt/trc20_2023-01-01_2023-01-07.csv
```

- 출력 스키마: `ts, tx_hash, from, to, amount_raw, decimals, block`
