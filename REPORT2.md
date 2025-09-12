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
  - `$BQ_DATASET.daily_flows`: 8,865 행

## 8) 현재 상태 요약
- 표준화/라벨/분류 산출물: 파티션 파일 다수(예: 각 ~1,500개)
- 일별 집계: `out/agg/daily_flows.parquet` 최신화
- 품질 체크: 주요 이상 없음, 일관성 지표 OK
- 업로드: GCS 업로드 및 BigQuery 로드 완료(아래 “정상화 조치” 반영)

### 정상화 조치(중복 로드 이슈 해결)
- 증상: 파티션/필터 없이 2회 로드되어 `classified_transfers≈6.03e8`, `daily_flows≈9,850`가 됨
- 조치: 테이블 삭제 후 `--replace` 로 재로드, 파티션/클러스터/`require_partition_filter=true` 적용
- 결과(파티션 필터 기준):
  - `classified_transfers`: 301,391,292 행 (2023-01-01~2025-12-31)
  - `daily_flows`: 8,865 행 (2023-01-01~2025-12-31)
  - 최근 30일 분포 예시: DIRECT_TRANSFER/P2P 우세, MINT/BURN 소수 발생

## 9) 다음 단계(PLAN2 연계)
- 블록 차원/근접 블록 UDF: `processing/build_dim_blocks.sql`
- 오라클/DEX Pool/동결/OFAC 데이터 인제스트 및 블록 정렬 조인
- 통합 피처 뷰 및 PRS/CRS 학습셋 추출

---

문의/오류 로그: `out/logs/pipeline_incremental.log`, `out/logs/failed_files.txt` 참고.

---

## 피드백 요약 및 반영 계획

### 그대로 훌륭한 부분(유지)
- 키 보존: `tx_hash + log_index` 유지(이벤트 유일성)
- 정규화/정합: `amount_norm = amount_raw / 10^decimals`, 주소 소문자화
- 증분 파이프라인: 파일 단위 순차 처리 + 누적 집계 스냅샷
- 품질 검증 루틴: 전역 유니크/시간범위/표본 집계 비교 포함
- BQ 업로드: `classified_transfers`, `daily_flows` 로드 완료

### 체크리스트(빠른 개선 권장)
1) 카운트 정합성 게이트(raw==std==labeled==classified)
```sql
-- 단계별 건수 비교
SELECT 'raw' src, COUNT(*) c FROM `stablecoin_fds.eth_stable_transfers_2023p`
UNION ALL SELECT 'std', COUNT(*) FROM `stablecoin_fds.std_token_transfers`
UNION ALL SELECT 'labeled', COUNT(*) FROM `stablecoin_fds.labeled_transfers`
UNION ALL SELECT 'classified', COUNT(*) FROM `stablecoin_fds.classified_transfers`;
```

2) `daily_flows` 건수 sanity(조합 누락 여부)
```sql
SELECT token, category, direction, COUNT(*) days
FROM `stablecoin_fds.daily_flows`
GROUP BY 1,2,3 ORDER BY days DESC;
```

3) 제로 어드레스(Mint/Burn) 분류 규칙 추가
- from=0x0 → Mint(ISSUER_OUT), to=0x0 → Burn(ISSUER_IN)

4) 라벨 커버리지 지표화
```sql
SELECT SAFE_DIVIDE(
  COUNTIF(from_type IS NOT NULL OR to_type IS NOT NULL),
  COUNT(*)
) AS label_coverage
FROM `stablecoin_fds.classified_transfers`;
```

5) 카테고리/방향 ENUM 고정 및 0 채움 옵션(분석 편의)
- category ∈ {DIRECT_TRANSFER, DEX_SWAP, DEFI_OTHER, MINT, BURN}
- direction ∈ {CEX_IN, CEX_OUT, DEX_IN, DEX_OUT, P2P, ISSUER_IN, ISSUER_OUT}

6) 수치형 타입 고정
- amount_raw: STRING/BIGNUMERIC 권장, amount_norm: NUMERIC/BIGNUMERIC 강제

7) 표준화 BQ내 CTAS 전략 검토(다음 증분 적용 후보)

8) BQ 로드 설정 강화
- classified: ts 파티션 + (token, from_addr, to_addr) 클러스터
- daily_flows: date 파티션 + (token, category, direction) 클러스터
- require_partition_filter=true 권장

9) 증분 안전장치
- processed_manifest.json(파일 해시/행수/버전) 기록, 재시도/격리 폴더

10) 시간대/정렬 메타
- 모든 스테이지 UTC 보장 + 최종 산출물에 min(ts), max(ts) 기록

11) 이상치/스팸 태깅 레일
- 더스트 컷오프/대량 전송 outlier 플래그(제외X, 태그)

12) 테스트 골든세트
- 소량 구간(예: 2023-03-10~14, USDC)로 전파 테스트 자동화

### 바로 써먹을 점검/수정 쿼리
중복 여부(최종 테이블)
```sql
SELECT COUNT(*) AS total,
       COUNT(DISTINCT CONCAT(tx_hash,'-',CAST(log_index AS STRING))) AS distinct_logs
FROM `stablecoin_fds.classified_transfers`;
```

민트/번 카운트
```sql
SELECT
  COUNTIF(from_addr='0x0000000000000000000000000000000000000000') AS mints,
  COUNTIF(to_addr  ='0x0000000000000000000000000000000000000000') AS burns
FROM `stablecoin_fds.classified_transfers`;
```

금액 스케일 sanity
```sql
SELECT token,
       APPROX_QUANTILES(amount_norm, 100)[OFFSET(50)] AS p50,
       APPROX_QUANTILES(amount_norm, 100)[OFFSET(90)] AS p90
FROM `stablecoin_fds.classified_transfers`
GROUP BY token;
```

집계 누락 조합 탐지
```sql
WITH d AS (
  SELECT DISTINCT DATE(ts) AS d FROM `stablecoin_fds.classified_transfers`
), combos AS (
  SELECT d.d, t.token, c.category, r.direction
  FROM d
  CROSS JOIN UNNEST(['usdc','usdt','dai']) AS t(token)
  CROSS JOIN UNNEST(['DIRECT_TRANSFER','DEX_SWAP','DEFI_OTHER','MINT','BURN']) AS c(category)
  CROSS JOIN UNNEST(['CEX_IN','CEX_OUT','DEX_IN','DEX_OUT','P2P','ISSUER_IN','ISSUER_OUT']) AS r(direction)
)
SELECT * FROM combos
LEFT JOIN `stablecoin_fds.daily_flows` f
  ON f.date = combos.d AND f.token = combos.token
 AND f.category = combos.category AND f.direction = combos.direction
WHERE f.date IS NULL
LIMIT 100;
```

### 결론
- 현재 구조/품질/증분/업로드 루틴은 적절하며, 위 체크리스트를 순차 반영하면 안정성·재현성·해석성이 강화됩니다.
- 다음 단계: ENUM/민트·번 분류 반영, BQ 파티션/클러스터 설정 보강, CTAS 표준화 검토, PLAN2(PRS/CRS) 피처 인제스트로 진행.
