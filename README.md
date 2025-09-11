# Stablecoin FDS 데이터 수집/전처리 (Ethereum Only)

이 저장소는 Ethereum 메인넷의 USDT/USDC/DAI 전송 로그를 BigQuery로 추출하여 GCS/BigQuery에 적재하고, 표준화·라벨·집계까지 수행하기 위한 스크립트/구성을 포함합니다.

- GCS 버킷: `stablecoin-fds-raw`, `stablecoin-fds-processed`
- BigQuery 데이터셋: `stablecoin_fds`

자세한 아키텍처와 일정은 `PLAN.md` 참고.

## 1) 최초 수집 경로 (Ethereum)
- BigQuery 공개 테이블 `bigquery-public-data.crypto_ethereum.token_transfers`에서 USDT/USDC/DAI 전송 로그를 SQL로 추출 후 GCS Parquet로 내보냅니다.

## 2) 비용/계정 선택
- BigQuery/GCS는 사용량 기반 유료. 소규모 POC는 개인 계정+무료 할당으로 시작 가능, 본격 수집·정기화면 회사 계정 권장.
- 비용 절감: 날짜 파티션 필터, 필요한 컬럼만 SELECT, Parquet 압축, GCS→BQ 파티션 로드.

## 3) 환경변수(.env)
`.env.example`을 복사하여 `.env` 생성 후 값 설정:

필수
- `GCP_PROJECT`, `GCS_BUCKET_RAW`, `GCS_BUCKET_PROCESSED`, `BQ_DATASET`
- `ETHERSCAN_API_KEY` (옵션: 라벨/보조 수집)

토큰 주소(기본값 포함, 필요 시 덮어쓰기)
- `ETH_USDC=0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48` (6)
- `ETH_USDT=0xdAC17F958D2ee523a2206206994597C13D831ec7` (6)
- `ETH_DAI=0x6B175474E89094C44Da98b954EedeAC495271d0F` (18)

## 4) 실행 절차
사전 준비
- GCS 버킷/BigQuery 데이터셋 생성
- 도커 사용 권장(또는 로컬 파이썬)

도커
```bash
cp .env.example .env && vi .env
docker compose -f docker/docker-compose.yml build
# 주피터(옵션)
docker compose -f docker/docker-compose.yml up -d jupyter
```

과거 데이터 수집 (Ethereum)
```bash
bash ingest/bigquery/export_to_gcs.sh
```

정규화/라벨/집계
```bash
# 표준화
python3 processing/standardize.py \
  --input out/eth/usdc/eth_usdc_logs.parquet \
  --chain eth --token usdc \
  --out out/std/eth/usdc_2023.parquet

# 라벨 병합
python3 processing/label_merge.py \
  --transfers out/std/eth/usdc_2023.parquet \
  --labels configs/labels_seed.csv \
  --out out/labeled/eth/usdc_2023.parquet

# 컨텍스트 분류
python3 processing/classify_context.py \
  --input out/labeled/eth/usdc_2023.parquet \
  --out out/classified/eth/usdc_2023.parquet

# 일별 집계
python3 processing/aggregate_daily.py \
  --input out/classified/eth/usdc_2023.parquet \
  --out out/agg/daily_flows.parquet
```

GCS/BQ 적재
```bash
# 로컬→GCS 동기화 (원시/표준화 산출물 경로에 맞춰 실행)
# (선택) gcloud storage 권장
gcloud storage cp out/std/eth/stable_2023.parquet gs://$GCS_BUCKET_PROCESSED/std/chain=eth/token=stable/date=2023p/

# GCS→BigQuery 로드(예)
echo $GCP_PROJECT
bash bin/bq_load.sh raw_token_transfers gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/*.parquet ts token,from_addr,to_addr
```

## 5) 품질/검증
- `notebooks/sanity_checks.ipynb`로 스키마/결측 확인
- `notebooks/event_replay.ipynb`로 2023-03 USDC 디페그 등 리플레이 확인

## 6) 주의사항
- 쿼리 파티션/필터 필수, Parquet 압축 권장
- 주소 라벨은 핵심(CEX/DEX/발행사) 수기 검증

## 7) 산출물 스키마(예상)
- 표준화(`out/std/eth/*_std.parquet`)
  - cols: `ts(UTC), chain('eth'), tx_hash, log_index, token('usdt|usdc|dai|stable'), token_address, from_addr, to_addr, amount_raw, decimals, amount_norm, block_number`
  - 특이: `amount_norm = amount_raw / 10^decimals`, `tx_hash+log_index` 유일키
- 라벨 병합(`out/labeled/eth/*_labeled.parquet`)
  - 표준화 + `from_type, to_type` (예: CEX, DEX_POOL, DEX_ROUTER, ISSUER, USER)
- 컨텍스트 분류(`out/classified/eth/*_classified.parquet`)
  - 라벨 + `category(DIRECT_TRANSFER|DEX_SWAP|DEFI_OTHER)`, `direction(P2P|DEX_IN|DEX_OUT|CEX_IN|CEX_OUT)`
- 일별 집계(`out/agg/daily_flows.parquet`)
  - cols: `date, chain, token, category, direction, tx_count, total_amount`

## 8) 대용량 처리 옵션(누적 집계)
원시가 수십 GB 이상이면 한 번에 합치지 말고 파일 단위로 부분 집계→누적 병합 권장. `bin/process_all_incremental.sh` 사용.

```bash
bash bin/process_all_incremental.sh
```
