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

### 3-1) .env 로딩 방법(택1)
로컬 셸에서 환경변수를 간단히 올리는 방법입니다.

```bash
# 방법 A: POSIX 표준 (임시 export)
set -a; source .env; set +a

# 방법 B: xargs (빈칸/주석 제외)
export $(grep -v '^#' .env | xargs)

# 방법 C: direnv 사용 권장(자동 로드)
brew install direnv
echo 'use dotnev' > .envrc  # 또는: echo 'dotenv' > .envrc
direnv allow

# 확인
echo $GCP_PROJECT $BQ_DATASET $GCS_BUCKET_RAW $GCS_BUCKET_PROCESSED
```

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

### 4-1) 로컬 파이썬(venv) 실행 가이드
도커 대신 로컬 파이썬으로 실행하려면 가상환경을 권장합니다.

```bash
# 가상환경 생성 및 활성화
python3 -m venv .venv
source .venv/bin/activate

# 의존성 설치
pip install -r requirements.txt

# .env 로딩 (위 3-1 참조)
set -a; source .env; set +a

# 비용 가드 기본 상한(예: 2GB)
export BQ_MAX_BYTES_BILLED=$((2*1024*1024*1024))

# PLAN2 실행기(드라이런 먼저 권장)
python processing/plan2_inject_and_run.py \
  --date-start 2023-01-01 --date-end 2023-01-31 \
  --dry-run-first --max-bytes-billed $BQ_MAX_BYTES_BILLED \
  --skip-view

# 안전하면 상한 조정 후 실제 실행
python processing/plan2_inject_and_run.py \
  --date-start 2023-01-01 --date-end 2025-12-31 \
  --max-bytes-billed $((20*1024*1024*1024))
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
  - 라벨 + `category(DIRECT_TRANSFER|DEX_SWAP|DEFI_OTHER|MINT|BURN)`, `direction(P2P|DEX_IN|DEX_OUT|CEX_IN|CEX_OUT|ISSUER_IN|ISSUER_OUT)`
- 일별 집계(`out/agg/daily_flows.parquet`)
  - cols: `date, chain, token, category, direction, tx_count, total_amount`

## 8) 대용량 처리 옵션(누적 집계)
원시가 수십 GB 이상이면 한 번에 합치지 말고 파일 단위로 부분 집계→누적 병합 권장. `bin/process_all_incremental.sh` 사용.

```bash
bash bin/process_all_incremental.sh
```

## 9) bin 스크립트 가이드
- process_small.sh: GCS 원시 상위 N개만 로컬 파이프라인 실행(표준화→라벨→분류→샘플 집계)
  - 기본 N=10, 변경: `COUNT=50 bash bin/process_small.sh`
  - 로그: `out/logs/process_small.log`
- process_all.sh: 전체 파일 대상 일괄 처리(메모리/디스크 여유 필요)
  - `bash bin/process_all.sh`
  - 로그: `out/logs/process_all.log`
- process_all_incremental.sh: 파일 단위 부분 집계→누적 병합(대용량 안전)
  - 스몰 테스트: `CLEAR_AGG=1 COUNT=50 bash bin/process_all_incremental.sh`
  - 누적 확장: `COUNT=200 bash bin/process_all_incremental.sh`
  - 전체 실행: `bash bin/process_all_incremental.sh`
  - 로그: `out/logs/process_all_incremental.log`
- check_outputs.sh: 산출물 점검(파일 수/스키마/라벨 커버리지/카테고리 분포/용량)
  - `bash bin/check_outputs.sh`
- bq_load.sh: GCS Parquet → BigQuery 로드(파티션/클러스터 지정 가능)
  - `bash bin/bq_load.sh <table> <gs://uri/*.parquet> [partition_field] [cluster_fields_csv]`
  - 예: `bash bin/bq_load.sh std_token_transfers gs://$GCS_BUCKET_PROCESSED/std/chain=eth/token=*/date=*/*.parquet ts token,from_addr,to_addr`
- gcs_sync.sh(옵션): 로컬 디렉터리 → GCS 동기화(gsutil 기반). 최신 권장은 `gcloud storage cp/rsync`.
  - 예: `gcloud storage cp out/agg/daily_flows.parquet gs://$GCS_BUCKET_PROCESSED/std/chain=eth/agg/`

## 10) PLAN2(PRs/CRs) 확장 Quickstart

### 10.1 구성 요소
- UDF/헬퍼: `processing/build_udfs.sql`, `processing/build_block_helpers.sql`
- 원시 인제스트 SQL:
  - 체인링크: `ingest/bigquery/sql/eth_chainlink_prices_raw.sql` → `fact_oracle_prices_raw`
  - Uniswap v3: `ingest/bigquery/sql/eth_uniswap_v3_swaps_raw.sql` → `fact_univ3_swaps_raw`
  - 동결 이벤트: `ingest/bigquery/sql/eth_freeze_events.sql` → `fact_freeze_events`
- 디코딩/뷰: `processing/build_decoded_prs.sql`, `processing/build_prs_crs_view.sql`
- 실행기(초안): `processing/plan2_run.py`

### 10.2 토픽 해시/ABI 주입값(필수)
- 이벤트의 topic0는 "이벤트 시그니처 문자열"의 Keccak-256입니다.
  - 예시 계산(Python web3):
    ```bash
    python3 - <<'PY'
from eth_utils import keccak
def h(s):
    print('0x'+keccak(text=s).hex())
print('Chainlink AnswerUpdated:', end=' '); h('AnswerUpdated(int256,uint256,uint256)')
print('UniswapV3 Swap:', end=' '); h('Swap(address,address,int256,int256,uint160,uint128,int24)')
print('USDC Blacklisted:', end=' '); h('Blacklisted(address)')
print('USDC UnBlacklisted:', end=' '); h('UnBlacklisted(address)')
print('USDT AddedBlackList:', end=' '); h('AddedBlackList(address)')
print('USDT RemovedBlackList:', end=' '); h('RemovedBlackList(address)')
print('USDT DestroyedBlackFunds:', end=' '); h('DestroyedBlackFunds(address,uint256)')
PY
    ```
- 실행 시 아래 파라미터를 채워 전달하세요:
  - 체인링크: `answer_updated_topic=0x...`, `feed_addresses=[...]` (configs/oracle_feeds.yaml 참고)
  - Uniswap v3: `univ3_swap_topic=0x...`, `target_pools=[...]` (configs/dex_pools.yaml 참고)
  - 동결/블랙리스트: `freeze_event_topics=[0x...,0x...,...]`, `usdc_addr`, `usdt_addr`
- 권장 원칙
  - 블록 정합: 조인 시 항상 `ON fact.block_number <= t.block_number` (미래 누수 방지)
  - 오프체인→블록 매핑: `stablecoin_fds.block_at_or_before(ts)` 사용(직전 블록)

### 10.3 실행 예시
```bash
# 1) UDF/블록 헬퍼 등록
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false < processing/build_udfs.sql
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false < processing/build_block_helpers.sql

# 2) 체인링크 원시(날짜/토픽/피드 주소 지정)
bq query --use_legacy_sql=false \
  --parameter feed_addresses:ARRAY<STRING>:0x8fffffd4afb6115b954bd326cbe7b4ba576818f6,0x3e7d1eab13ad0104d2750b8863b489d65364e32d \
  --parameter answer_updated_topic:STRING:0x... \
  --parameter date_start:DATE:2023-01-01 --parameter date_end:DATE:2025-12-31 \
  < ingest/bigquery/sql/eth_chainlink_prices_raw.sql

# 3) Uniswap v3 원시(스왑 토픽/풀 주소 지정)
bq query --use_legacy_sql=false \
  --parameter target_pools:ARRAY<STRING>:0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640,0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8 \
  --parameter univ3_swap_topic:STRING:0x... \
  --parameter date_start:DATE:2023-01-01 --parameter date_end:DATE:2025-12-31 \
  < ingest/bigquery/sql/eth_uniswap_v3_swaps_raw.sql

# 4) 동결/블랙리스트(USDC/USDT 주소/토픽 지정)
bq query --use_legacy_sql=false \
  --parameter usdc_addr:STRING:0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48 \
  --parameter usdt_addr:STRING:0xdAC17F958D2ee523a2206206994597C13D831ec7 \
  --parameter freeze_event_topics:ARRAY<STRING>:0x...,0x...,0x... \
  --parameter date_start:DATE:2023-01-01 --parameter date_end:DATE:2025-12-31 \
  < ingest/bigquery/sql/eth_freeze_events.sql

# 5) 디코딩/뷰(초기 스켈레톤)
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false < processing/build_decoded_prs.sql
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false < processing/build_prs_crs_view.sql

# (옵션) 전체 실행기
python3 processing/plan2_run.py --date-start 2023-01-01 --date-end 2025-12-31
```

### 10.4 유의사항(쿼리 비용/정합)
- 모든 쿼리에 날짜/블록 파티션 필터 필수(`require_partition_filter=true` 권장)
- 원시→디코딩→피처 3계층으로 CTAS 분리(성능/재현성)
- 주소 비교는 소문자 기준(LOWER) 통일

### 10.5 zsh/ bash 파라미터 주입 주의(중요)
- zsh에선 `ARRAY<STRING>`의 `<`가 리다이렉션으로 오인될 수 있습니다. 아래처럼 타입+값 전체를 작은따옴표로 감싸고, 값(변수)은 큰따옴표로 치환하세요.
```bash
# 예: FEEDS/POOLS/FREEZE 는 JSON 배열 문자열
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
  --parameter='feed_addresses:ARRAY<STRING>:'"$FEEDS" \
  --parameter='answer_updated_topic:STRING:'"$ANSWER_TOPIC" \
  --parameter='date_start:DATE:'"$DATE_START" \
  --parameter='date_end:DATE:'"$DATE_END" \
  < ingest/bigquery/sql/eth_chainlink_prices_raw.sql
```

### 10.6 결과 테이블 빠른 점검(bq CLI)
```bash
# 체인링크 원시
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_oracle_prices_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# Uniswap v3 원시
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_univ3_swaps_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# 동결/블랙리스트 원시
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_freeze_events\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"
```
