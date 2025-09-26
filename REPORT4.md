### PLAN2 중간 점검: Pre-Sign FDS(디페그·동결) 현황/이슈/방향성

**TL;DR**
- **체인링크 원시 수집/디코딩 이슈**가 핵심 병목. 프록시↔애그리게이터 매핑을 확정적으로 복원해야 함.
- 휴리스틱(p50 등)은 보조 지표로만 사용. 우선순위는 **확정 매핑(Route A/B)**과 **원시→디코딩 데이터 라인리지 정립**.
- 비용은 `bin/bq_guard.sh` 기반 드라이런/바이트가드로 통제. 검증은 일 단위→3일→주 단위로 점진 확장.

---

### 1) 현재 이슈 요약

- **Chainlink (가장 중요)**
  - `fact_oracle_prices` 0건 또는 `symbol` NULL 문제.
  - 원인: 프록시 주소만으로는 로그가 거의 없거나 AggregatorUpdated 타임라인을 재구성하지 못함. 토픽/주소/날짜 구간 혼선.
  - 결과: `view_univ3_swaps_usd`에서 USD 변환이 비어지거나 NULL 연쇄.

- **Uniswap v3**
  - 정규화/디코딩(view_univ3_swaps_norm) 안정화. USD 변환 뷰는 `SAFE_CAST`, 윈도우 `LAST_VALUE`로 오버플로/NULL 개선.
  - `univ3_pool_whitelist`, `dim_token_oracles`, `dim_tokens` 연결로 가격/소수점 정합성 보완.

- **Freeze/Blacklist**
  - 원시 이벤트→타임라인/스냅샷 뷰 구성 완료. 정책 라벨 확장 뷰 도입.
  - 커버리지/라벨 세분화 검증 필요(이상치 표기, 이중 상태 전이 등).

- **비용/운영**
  - 드라이런/`maximum_bytes_billed` 가드 적용. 날짜 필터/파티셔닝 습관화.
  - 대용량 테이블 쿼리는 먼저 스키마/샘플 검증→슬라이싱 실행.

---

### 2) 방향성/전략

- **확정 매핑 우선 (휴리스틱은 보조)**
  - Route A: RPC `aggregator()`를 기준 블록에서 직접 호출해 프록시→애그리게이터를 확정.
  - Route B: BQ로 `AggregatorUpdated(address,address)` 이벤트를 스캔하여 타임라인 복원.
  - 우선은 비용이 낮고 오검이 없는 Route A로 일 단위 검증→신뢰 확보 후 구간 확장.

- **데이터 라인리지**
  - 레이어: raw → decoded → normalized → features.
  - 비정상/이상치는 수정보다 **플래그(예: is_anomalous, anomaly_reason)**로 보존.

- **비용 통제**
  - `bin/bq_guard.sh` + 날짜 슬라이싱 + 샘플 검증 루틴 일원화.
  - 점진 확장(일→3일→주)로 낭비 방지.

---

### 3) 바로 실행 체크리스트(권장 순서)

1. 기준일(as-of) 블록 획득(BQ UDF) 후, RPC로 프록시별 `aggregator()` 조회(Route A).
2. 당일 기준 `dim_oracle_feeds_day` 생성 → `fact_oracle_prices_raw_all`(CTAS) → `build_chainlink_decode.sql` 실행.
3. `fact_oracle_prices` 검증: 총건수, `symbol` NULL 비율, 심볼 분포.
4. 정상 확인 시 3일로 확장(예: 2023-03-10 ~ 03-12), `view_univ3_swaps_usd` 샘플링 검증.
5. Freeze 정책 라벨 뷰 커버리지 점검(토픽 추가/정의 보강 필요 시 반영).
6. PRS/CRS 1차 피처(`oracle_deviation_bps` 등) 계산 뷰 스켈레톤 연결.

---

### 4) 검증용 커맨드 샘플

아래는 문서화 목적의 샘플입니다(실행 전 프로젝트/데이터셋/일자 조정 필요).

```bash
# 4-1) (Route A) 기준 블록
cat <<'SQL' | bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false
SELECT `stablecoin_fds.block_at_or_before`(TIMESTAMP '2023-03-11 00:00:00+00') AS block_number;
SQL

# 4-2) (Route A) 프록시→애그리게이터 조회(ETH_RPC_URL 필요)
# 결과로 dim_oracle_feeds_day 생성 후 당일 raw 수집/디코딩

# 4-3) 체인링크 디코딩 검증
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-5000000000}"
SELECT COUNT(*) AS n, COUNTIF(symbol IS NULL) AS null_symbols,
       ARRAY_AGG(DISTINCT symbol IGNORE NULLS) AS symbols
FROM `$BQ_DATASET.fact_oracle_prices`
WHERE DATE(block_timestamp_utc)=DATE '2023-03-11';
SQL

# 4-4) Uniswap v3 USD 변환 샘플
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-5000000000}"
SELECT * FROM `$BQ_DATASET.view_univ3_swaps_usd`
WHERE DATE(block_timestamp_utc)=DATE '2023-03-11'
LIMIT 5;
SQL

# 4-5) Freeze 스냅샷 카운트
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-5000000000}"
SELECT COUNT(*) AS n FROM `$BQ_DATASET.view_freeze_policy_snapshot_latest`;
SQL
```

---

### 5) 휴리스틱(p50) 관련 메모

- p50≈1(스테이블) / p50≫100(ETH/USD) 등은 **식별 보조 지표**로만 사용.
- 우선은 프록시/피드 레지스트리/이벤트 타임라인 기반의 **결정적 매핑**이 1순위.
- 휴리스틱 적용 시에도 **출처/근거 컬럼**과 플래그를 함께 저장하여 다운스트림이 필터링 가능하게 유지.

---

### 6) 리스크/주의사항

- RPC 레이트 리밋/블록 재구성 지연 → 재시도/백오프.
- 일 단위 내 Aggregator 교체 가능성(희귀) → as-of 규칙 명시.
- BQ 퍼블릭셋 최신성/지연 → 시간 윈도 가드.
- 비용 한도 미스 → `--dry-run`과 `--maximum_bytes_billed`를 모든 쿼리에 강제.

---

### 7) 진행 중/남은 작업(요약)

- Chainlink 심볼 검증(`fact_oracle_prices` NULL율/분포) [진행 중]
- Chainlink 글로벌 로그 카운트(2020-2025) [진행 중]
- Raw sanity(카운트/토픽/샘플) [진행 중]
- Uniswap v3 USD 샘플 5건 점검 [대기]
- REPORT/SCHEMA 문서 갱신(매핑/정규화/비용가드) [대기]
- 비용가드/파티션 필터 준수 재확인 [대기]



### 8) 진행 로그(2025-09-24)

- 인증/환경
  - gcloud 인증 재설정 완료, `GCP_PROJECT/BQ_DATASET/BQ_MAX_BYTES_BILLED/ASOF_DATE` 환경 구성.
  - 비용 가드: 모든 실행 `bin/bq_guard.sh` 경유, 드라이런+상한 적용.

- Route B(타임라인)
  - `processing/build_chainlink_timeline.sql`로 2023-03, 2023-01, 2023-02 범위 생성 성공.
  - as-of(2023-03-11) 기준 `dim_oracle_feeds` 생성 시, 해당 기간에 업데이트가 적어 유의미 주소 확보가 제한적.

- 옵션 A(최신/과거 as-of 주소 시드)
  - 최신: RPC `aggregator()` 조회로 4개 프록시의 최신 애그리게이터 주소 확보/시드 → 2023-03-11 원시/디코딩 0건.
  - 과거 as-of: Etherscan API로 2023-03-11 기준 블록(16808257)에서 각 프록시의 최근 `AggregatorUpdated` 이벤트로 집계기 추출(동일 주소 `0x304d...`) 후 시드 → 당일 원시 0건.

- 원시 수집 SQL 수정
  - `ingest/bigquery/sql/eth_chainlink_prices_multi_topic.sql`를 애그리게이터만이 아닌 프록시+애그리게이터 합집합 대상으로 수집하도록 수정.
  - BigQuery 상호상관 서브쿼리 오류를 `JOIN addresses`로 변경하여 해결.
  - 재실행 결과, 2023-03-11 여전히 0건.

- 프록시 topic 분포 점검(2023-03)
  - 프록시 4개 주소에서 2023-03 기간 `topic0` 분포 조회 결과, 유의미 로그 없음.

결론(현 시점): 2023-03-11 하루 기준으로는 프록시/해당 as-of 애그리게이터 모두 BQ 퍼블릭 `logs`에서 이벤트가 관측되지 않았음. 토픽 필터를 제거한 채 주소 기준으로도 0건으로 확인됨.

### 9) 현재 이슈와 가설

- 이슈: `fact_oracle_prices_raw_all`가 2023-03-11에 0건 → `fact_oracle_prices`도 0건.
- 가설1: 해당 날짜에 해당 애그리게이터 주소에서 이벤트가 거의 없었을 가능성 → 월/분기 범위 집계 필요.
- 가설2: OCR계열 `NewTransmission` 등 특정 토픽이 다른 컨트랙트(예: Transmitter)에서 발생했거나, 우리가 찾은 as-of 애그리게이터 주소와 이벤트 발생 주소가 상이.
- 가설3: 체인링크 가격 피드가 일부 구간에서 프록시/애그리게이터가 미발행(또는 다른 주소로 이관)되었을 가능성.

리스크/비용 관점:
- 월/분기 범위 스캔은 20–40GB 상회 가능 → 구간 축소/슬라이싱 필요.

### 10) 재현/검증 커맨드(합의 필요)

- as-of 애그리게이터(0x304d6972...)의 2023-03 토픽 분포 확인:
```bash
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-2147483648}"
WITH aggs AS (
  SELECT * FROM UNNEST([LOWER('0x304d69727dd28ad6e1aa2c01db301db556c7b725')]) AS addr
)
SELECT l.topics[SAFE_OFFSET(0)] AS topic0, COUNT(*) n
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN aggs a ON LOWER(l.address)=a.addr
WHERE DATE(l.block_timestamp) BETWEEN DATE '2023-03-01' AND DATE '2023-03-31'
GROUP BY topic0 ORDER BY n DESC LIMIT 50;
SQL
```

- 최신 애그리게이터 4개(참고용)의 2023-03 토픽 분포:
```bash
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-2147483648}"
WITH aggs AS (
  SELECT * FROM UNNEST([
    LOWER('0xc9e1a09622afdb659913fefe800feae5dbbfe9d7'),
    LOWER('0x0d5f4aadf3fde31bbb55db5f42c080f18ad54df5'),
    LOWER('0x709783ab12b65fd6cd948214eee6448f3bdd72a3'),
    LOWER('0x7d4e742018fb52e48b08be73d041c18b21de6fb5')
  ]) AS addr
)
SELECT l.topics[SAFE_OFFSET(0)] AS topic0, COUNT(*) n
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN aggs a ON LOWER(l.address)=a.addr
WHERE DATE(l.block_timestamp) BETWEEN DATE '2023-03-01' AND DATE '2023-03-31'
GROUP BY topic0 ORDER BY n DESC LIMIT 50;
SQL
```

- 애그리게이터 대상, 토픽 필터 없이 2023-03-11 하루 샘플 수집(이미 시도, 0건 재현 확인용):
```bash
cat <<'SQL' | bash bin/bq_guard.sh --stdin --project "$GCP_PROJECT" --max-bytes "${BQ_MAX_BYTES_BILLED:-2147483648}"
CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices_raw_all` AS
WITH aggs AS (
  SELECT DISTINCT LOWER(aggregator) AS addr FROM `stablecoin_fds.dim_oracle_feeds`
)
SELECT
  l.address AS feed_address,
  l.block_number,
  l.block_timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index,
  l.topics[SAFE_OFFSET(0)] AS topic0,
  l.data AS data,
  CASE WHEN REGEXP_CONTAINS(l.data, r'^0x[0-9A-Fa-f]*$') THEN FROM_HEX(SUBSTR(l.data, 3)) END AS data_bytes
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN aggs a ON LOWER(l.address)=a.addr
WHERE DATE(l.block_timestamp)=DATE '2023-03-11';
SQL
```

### 11) 다음 액션(제안)

- A1. 위 토픽 분포 쿼리 결과를 바탕으로 실제 사용 토픽 식별 후, 디코딩 UDF/뷰 업데이트(`AnswerUpdated`/`NewTransmission` 등 다중 지원).
- A2. 2023-03 월 전체에 대해 애그리게이터 주소 원시 수집(토픽 무제한) 후, 일 단위 분포로 존재 여부 확인. 비용 초과 시 1주/반월로 슬라이스.
- A3. 필요 시 2022-12 ~ 2023-04 범위까지 확장 스캔(분기 단위), 드라이런으로 상한 확인 후 실행.
- A4. 결과 유무에 따라 `dim_oracle_feeds`를 as-of(타임라인) 기반으로 재구성하고 `fact_oracle_prices` 빌드 재시도.
- A5. 진행 내용과 쿼리/근거를 `REPORT3.md`/`SCHEMA.md`에도 반영.
