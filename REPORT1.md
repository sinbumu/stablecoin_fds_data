# REPORT1 — Ethereum Raw Data 수집 중간 보고

## 1) 환경/프로젝트 준비
- macOS에 Google Cloud CLI 설치 및 인증
  - `gcloud auth login && gcloud auth application-default login`
- 프로젝트/리전
  - Project ID: `$GCP_PROJECT`
  - BigQuery Dataset: `stablecoin_fds` (US)
  - GCS Buckets: `stablecoin-fds-raw`, `stablecoin-fds-processed` (US)
- API 활성화: BigQuery / Cloud Storage

## 2) 레포/환경 구성
- `.env` 작성 (핵심 키)
  - `GCP_PROJECT`, `GCS_BUCKET_RAW`, `GCS_BUCKET_PROCESSED`, `BQ_DATASET`
- Ethereum Only 설정: `configs/ethereum_only.yaml`
- 수집 SQL: `ingest/bigquery/sql/eth_stable_transfers.sql`
  - `LOWER(token_address)`로 주소 대소문자 이슈 제거
  - PLAN2 정합 대비 `log_index` 포함(유일키: `tx_hash + log_index`)
- 내보내기 스크립트: `ingest/bigquery/export_to_gcs.sh`
  - `.env` 자동 로드(`set -a`), BQ→GCS Parquet 내보내기

## 3) 실행 (요약)
```bash
bash ingest/bigquery/export_to_gcs.sh
```
출력 경로
```
gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/part-*.parquet
```

## 4) 결과 검증
- BigQuery 카운트
```bash
bq --project_id=$GCP_PROJECT query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `stablecoin_fds.eth_stable_transfers_2023p`'
# 예) 301,391,292 행
```
- GCS 파일 확인
```bash
gcloud storage ls gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/ | head
```
- 랜덤 3행(외부 테이블 미리보기)
```bash
bq --project_id=$GCP_PROJECT query --use_legacy_sql=false \
  "CREATE OR REPLACE EXTERNAL TABLE \`$BQ_DATASET._tmp_preview_gcs\`
   OPTIONS (format='PARQUET', uris=['gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/*.parquet']);"

bq --project_id=$GCP_PROJECT query --use_legacy_sql=false \
  "SELECT ts, tx_hash, log_index, token_address, from_addr, to_addr, amount_raw, block_number
   FROM \`$BQ_DATASET._tmp_preview_gcs\`
   ORDER BY RAND() LIMIT 3"

bq --project_id=$GCP_PROJECT rm -f -t $BQ_DATASET._tmp_preview_gcs
```

## 5) 용량/비용 참고
- GCS 총 용량
```bash
gcloud storage du -s gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/
```
- BQ 테이블 바이트 수
```bash
bq show --format=prettyjson stablecoin_fds.eth_stable_transfers_2023p | jq .numBytes
```

## 6) 이슈 및 해결
- `.env` 미로딩 → 스크립트에 자동 로드 추가(set -a)
- 0건 이슈 → `LOWER(token_address)`로 수정, 재실행
- 작업 동기 여부 → `Waiting on bqjob ... DONE` 기준 완료

## 7) 재현 절차(요약)
```bash
# 1) 환경 변수/인증
cp .env.example .env && vi .env
gcloud auth application-default login

# 2) 추출 및 내보내기
bash ingest/bigquery/export_to_gcs.sh

# 3) 결과 검증
bq --project_id=$GCP_PROJECT query --use_legacy_sql=false \
  'SELECT COUNT(*) FROM `stablecoin_fds.eth_stable_transfers_2023p`'
```

---

## 다음 단계(로드맵)
1) 표준화(Standardize)
```bash
python3 processing/standardize.py \
  --input gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/*.parquet \
  --out out/std/eth/stable_2023.parquet
```
- `configs/ethereum_only.yaml`로 `decimals/token` 매핑, `amount_norm` 계산

2) 라벨 병합 / 컨텍스트 분류 / 집계
```bash
python3 processing/label_merge.py \
  --transfers out/std/eth/stable_2023.parquet \
  --labels configs/labels_seed.csv \
  --out out/labeled/eth/stable_2023.parquet

python3 processing/classify_context.py \
  --input out/labeled/eth/stable_2023.parquet \
  --out out/classified/eth/stable_2023.parquet

python3 processing/aggregate_daily.py \
  --input out/classified/eth/stable_2023.parquet \
  --out out/agg/daily_flows.parquet
```

3) BigQuery 로드(파티션/클러스터)
```bash
bash bin/bq_load.sh std_token_transfers \
  gs://$GCS_BUCKET_PROCESSED/std/chain=eth/token=*/date=*/*.parquet \
  ts token,from_addr,to_addr
```

4) PLAN2 대비(선택)
- `processing/build_dim_blocks.sql` 실행(블록 차원/UDF)
- 보조 팩트 인제스트(오라클/Uniswap/동결 이벤트) 쿼리 추가
- 통합 피처 뷰(`v_prs_crs_features`) 및 학습셋 추출

5) 노트북 검증
- `notebooks/sanity_checks.ipynb`, `notebooks/event_replay.ipynb`
