# Stablecoin FDS 데이터 수집/전처리

이 저장소는 USDT/USDC/DAI를 대상으로 ETH/BSC/Tron 네트워크의 전송 로그를 수집·전처리하여 GCS/BigQuery로 적재하기 위한 스크립트/구성을 포함합니다.

- GCS: gs://stablecoin-fds-raw, gs://stablecoin-fds-processed
- BigQuery: dataset stablecoin_fds

자세한 계획은 PLAN.md 참고.

## 1) 최초 수집은 BigQuery로 가능한가?
- Ethereum: `bigquery-public-data.crypto_ethereum.token_transfers`에서 과거 전송 로그를 SQL로 추출해 GCS로 내보내는 방식이 가장 빠르고 안정적입니다.
- BSC: 공개 BigQuery 테이블이 표준화되어 있지 않으므로 Cryo+RPC로 `Transfer` 로그를 대량 수집합니다.
- Tron: 공식 공개 테이블이 제한적이므로 TronScan API로 기간 슬라이싱 수집을 권장합니다.

정리: ETH/Tron(가능 범위)은 BigQuery/공개 데이터 우선, BSC/보완은 Cryo+RPC/Explorer를 병행합니다.

## 2) 비용/계정 선택 (개인 vs 회사)
- BigQuery/GCS는 사용량 기반 유료 서비스입니다. 소규모 테스트는 무료 할당량/크레딧(또는 Sandbox)로 시작 가능하나, 본격 수집·정기 실행은 비용이 발생합니다.
- 기준: 이벤트 로그 전량 추출·정규화·집계를 계획한다면 회사 비즈니스 계정 권장. 소규모 POC/샘플 검증은 개인 계정으로 Sandbox/무료 크레딧 활용 → 비용 모니터링 후 전환.
- 비용 절감 팁:
  - 파티션 필터 사용(날짜 범위), 필요한 컬럼만 SELECT
  - Parquet 압축, GCS→BQ 로드 시 파티션 필드 지정
  - 재실행 대비 중간 산출물(GCS) 캐싱
- 최신 요금은 공식 문서 참고: [BigQuery Pricing](https://cloud.google.com/bigquery/pricing), [GCS Pricing](https://cloud.google.com/storage/pricing)

## 3) 환경변수(.env)와 실제 주소
`.env.example`을 복사하여 `.env`를 만들고 값 채우기:

필수
- GCP_PROJECT, GCS_RAW_BUCKET, GCS_PROCESSED_BUCKET, BQ_DATASET
- ETH_RPC_URL, BSC_RPC_URL (Cryo/보완 수집 시 필요)
- ETHERSCAN_API_KEY, BSCSCAN_API_KEY (옵션: 라벨 보강/보조 수집)
- TRON_API_KEY (옵션: TronScan 레이트리밋 완화)

토큰 주소(기본값 포함, 필요 시 덮어쓰기)
- ETH_USDC=0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48 (6)
- ETH_USDT=0xdAC17F958D2ee523a2206206994597C13D831ec7 (6)
- ETH_DAI=0x6B175474E89094C44Da98b954EedeAC495271d0F (18)
- BSC_USDT=0x55d398326f99059fF775485246999027B3197955 (18)
- BSC_USDC=0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d (18)
- BSC_DAI=0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3 (18)
- TRON_USDT=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t (6)

## 4) 최초 수집 절차 (권장 플로우)

사전 준비
- `.env` 작성: GCP/키/주소 설정
- GCS 버킷/BigQuery 데이터셋 생성 (예: stablecoin-fds-raw/processed, dataset stablecoin_fds)
- 로컬 파이썬/도커 중 택1 (도커 권장)

도커 사용 시
```bash
# 1) 의존성 설치/환경 준비
cp .env.example .env && vi .env
docker compose -f docker/docker-compose.yml build

# 2) 주피터(옵션)
docker compose -f docker/docker-compose.yml up -d jupyter
```

과거 데이터 수집
- Ethereum(USDC/USDT/DAI):
```bash
bash ingest/bigquery/export_to_gcs.sh
```
- BSC(USDT 우선):
```bash
bash ingest/cryo/run_cryo_bsc.sh
```
- Tron(USDT):
```bash
python3 ingest/tron/tronscan_pull.py \
  --start 1672531200000 --end 1673136000000 \
  --out out/tron/usdt/trc20_2023-01-01_2023-01-07.csv
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
# 로컬→GCS 동기화
bash bin/gcs_sync.sh

# GCS→BigQuery 파티션 로드
echo $GCP_PROJECT
bash bin/bq_load.sh raw_token_transfers gs://stablecoin-fds-raw/raw/chain=eth/token=usdc/*.parquet
```

## 5) 품질/검증 체크
- 타임존/중복/결측 확인 (`notebooks/sanity_checks.ipynb`)
- 사건 리플레이(USDC 2023-03, USDT Curve 3pool 등)로 스파이크 확인 (`notebooks/event_replay.ipynb`)
- 라벨 샘플 수기 검증(주요 CEX/DEX/발행사)

## 6) 주의사항
- API/RPC 레이트리밋 → 블록/시간 슬라이싱, 재시도/백오프 적용
- Tron 대량 구간은 기간을 더 잘게 분할하여 수집
- 비용 관리: 쿼리 파티션/프루닝, Parquet 압축, 필요한 컬럼만 조회
