# REPORT2 — Ethereum 데이터 가공(표준화→라벨→컨텍스트→일별집계) 중간 보고

REPORT1(로우 데이터 수집) 이후 단계의 가공 파이프라인 실행 결과와 검증/업로드 현황을 정리합니다.

## 1) 표준화(Standardize)
- 스크립트: `processing/standardize.py`
- 입력: BigQuery→GCS 추출 Parquet (`gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/*.parquet`)
- 출력: `out/std/eth/*_std.parquet`
- 처리 요약:
  - 스키마 통일: `ts, tx_hash, log_index, token_address, from_addr, to_addr, amount_raw, block_number`
  - `configs/ethereum_only.yaml`로 `token`, `decimals` 매핑
  - `amount_norm = amount_raw / 10^decimals` 계산, 주소 소문자 정규화, `log_index` 보장

예시 실행:
```bash
python3 processing/standardize.py \
  --input gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/part-000000000000.parquet \
  --chain eth --token stable \
  --out out/std/eth/part-000000000000_std.parquet
```

## 2) 라벨 병합(Label Merge)
- 스크립트: `processing/label_merge.py`
- 입력: `out/std/eth/*_std.parquet`, 라벨 시드 `configs/labels_seed.csv`
- 출력: `out/labeled/eth/*_labeled.parquet`
- 처리 요약: 송수신자 주소 라벨(`from_type`, `to_type`) 병합

예시 실행:
```bash
python3 processing/label_merge.py \
  --transfers out/std/eth/part-000000000000_std.parquet \
  --labels configs/labels_seed.csv \
  --out out/labeled/eth/part-000000000000_labeled.parquet
```

## 3) 컨텍스트 분류(Classify Context)
- 스크립트: `processing/classify_context.py`
- 입력: `out/labeled/eth/*_labeled.parquet`
- 출력: `out/classified/eth/*_classified.parquet`
- 처리 요약: 규칙 기반 `category`, `direction` 분류

예시 실행:
```bash
python3 processing/classify_context.py \
  --input out/labeled/eth/part-000000000000_labeled.parquet \
  --out   out/classified/eth/part-000000000000_classified.parquet
```

## 4) 일별 집계(Daily Aggregation)
- 스크립트: `processing/aggregate_daily.py`
- 입력: `out/classified/eth/*.parquet`
- 출력: `out/agg/daily_flows.parquet`
- 처리 요약: `date, chain, token, category, direction` 단위로 `tx_count`, `total_amount` 집계
  - `groupby(..., observed=False)` 적용(경고 억제)

예시 실행:
```bash
python3 processing/aggregate_daily.py \
  --input out/classified/eth/part-000000000000_classified.parquet \
  --out   out/agg/daily_flows.parquet
```

## 5) 대용량 처리(증분 파이프라인)
- 쉘: `bin/process_all_incremental.sh` (파일 단위 누적 집계)
- 파이썬 오케스트레이터: `processing/pipeline_incremental.py`
- 동작:
  - GCS 파일을 순차 처리(표준화→라벨→분류)
  - 매 파일 집계→`out/agg/daily_flows_incremental.parquet`에 누적, 수시로 `out/agg/daily_flows.parquet` 스냅샷 갱신
- 장점: 메모리 고정, 실패 파일 로깅/재시도 용이

소량 테스트:
```bash
python3 processing/pipeline_incremental.py \
  --pattern "gs://$GCS_BUCKET_RAW/raw/chain=eth/token=stable/date=2023p/*.parquet" \
  --count 5 --clear-agg
```

## 6) 품질/검증 체크 결과
- 기본 체크: `bin/check_outputs.sh` — 파일 수/스키마/간단 요약 OK
- 확장 체크: `bin/check_outputs_more_detail.sh`
  - 스키마/유효성(음수/주소) 이상 없음
  - 전역 중복(`tx_hash+log_index`) 0
  - 분포/시간범위 정상
  - 표본(SAMPLE=20/200) 집계 vs 최종 집계 비교
    - `missing_keys_in_global=0`, `monotonic_violations=0` 확인(일관성 OK)

## 7) 업로드(진행 중)
- 스크립트: `processing/upload_to_bq.py`
- 목적: 로컬 Parquet → GCS 업로드 → BigQuery 로드(자동 스키마)
- 대상
  - 분류: `out/classified/eth/*_classified.parquet`
  - 집계: `out/agg/daily_flows.parquet`(최종본; incremental은 작업용)
  - 표준화(선택): `out/std/eth/*_std.parquet`

예시 실행:
```bash
python3 processing/upload_to_bq.py \
  --project "$GCP_PROJECT" \
  --bucket  "$GCS_BUCKET_PROCESSED" \
  --dataset "$BQ_DATASET" \
  --upload classified agg
# 표준화 포함 시: --upload classified agg std
```

- 로드 결과(확인됨):
  - `$BQ_DATASET.classified_transfers`: 301,391,292 행
  - `$BQ_DATASET.daily_flows`: 985 행

## 8) 현재 상태 요약
- 표준화/라벨/분류 산출물: 파티션 파일 다수(예: 각 ~1,500개)
- 일별 집계: `out/agg/daily_flows.parquet` 최신화
- 품질 체크: 주요 이상 없음, 일관성 지표 OK
- 업로드: Parquet 업로드 및 BigQuery 로드 진행 중

## 9) 다음 단계(PLAN2 연계)
- 블록 차원/근접 블록 UDF: `processing/build_dim_blocks.sql`
- 오라클/DEX Pool/동결/OFAC 데이터 인제스트 및 블록 정렬 조인
- 통합 피처 뷰 및 PRS/CRS 학습셋 추출

---

문의/오류 로그: `out/logs/pipeline_incremental.log`, `out/logs/failed_files.txt` 참고.
