# PLAN2_DETAIL — PRS/CRS 단계별 실행 가이드 (Ethereum Only)

본 문서는 PLAN2.md를 실제로 실행 가능한 순서/명령/검증 기준으로 구체화한 체크리스트입니다.

## 0) 전제
- ETH 전송 원시 수집 및 기본 가공(표준화→라벨→컨텍스트→일별집계) 완료
- BigQuery 업로드/테이블 최적화 적용(`require_partition_filter=true`)
- 블록 기준 축(`dim_blocks`) 존재 또는 즉시 생성 가능

---

## 1) 기반 구축(공통)
1-1) 블록 차원/함수 생성(미생성 시)
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false < processing/build_dim_blocks.sql
```
검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT COUNT(*) n FROM \`$BQ_DATASET.dim_blocks\` WHERE block_date BETWEEN '2023-01-01' AND '2025-12-31'"
```

1-2) 설정 파일 작성(저장소 관리)
- `configs/oracle_feeds.yaml` (Chainlink 피드 주소/decimals)
- `configs/dex_pools.yaml` (Uniswap v3 주요 풀 주소/fee)
- `configs/sanctions_sources.yaml` (OFAC 소스 URL/주기)

---

## 2) PRS — 오라클/DEX 보조 데이터 인제스트
2-1) Chainlink 오라클 가격(최소 PoC)
- 목표: `fact_oracle_prices(feed_address, block_number, block_timestamp_utc, tx_hash, log_index, price_scaled[, round_id])`
- 초기 단계: 로그 원본만 추출 후, UDF 파싱은 2차에 적용

예시 쿼리(피드/토픽은 세션 파라미터로 주입):
```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices_raw` AS
SELECT
  l.address AS feed_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE l.address IN UNNEST(@feed_addresses)
  AND l.topics[OFFSET(0)] = @ANSWER_UPDATED_TOPIC
  AND DATE(b.timestamp) BETWEEN '2023-01-01' AND '2025-12-31';
```
검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_oracle_prices_raw\` \
 WHERE block_timestamp_utc BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31')"
```

2-2) Uniswap v3 풀 상태/스왑(최소 PoC)
- 목표: `fact_univ3_swaps_raw(pool_address, block_number, block_timestamp_utc, tx_hash, log_index[, data])`
- 초기 단계: 대상 풀의 Swap 이벤트만 추출(디코딩은 2차)

예시 쿼리(토픽/풀 주소 파라미터):
```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_univ3_swaps_raw` AS
SELECT
  l.address AS pool_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE l.topics[OFFSET(0)] = @UNIV3_SWAP_TOPIC
  AND l.address IN UNNEST(@target_pools)
  AND DATE(b.timestamp) BETWEEN '2023-01-01' AND '2025-12-31';
```
검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT pool_address, COUNT(*) c \
   FROM \`$BQ_DATASET.fact_univ3_swaps_raw\` \
  WHERE block_timestamp_utc BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31') \
  GROUP BY 1 ORDER BY c DESC LIMIT 10"
```

---

## 3) CRS — 동결 이벤트/OFAC 오프체인
3-1) 동결/블랙리스트 온체인 이벤트(최소 PoC)
- 목표: `fact_freeze_events(token_contract, event_sig, subject_address, block_number, block_timestamp_utc, tx_hash, log_index)`
- 초기 단계: 로그 원본 추출(토픽 배열로 필터), subject address는 topics에서 파싱

예시 쿼리(주소/토픽 파라미터):
```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_freeze_events` AS
SELECT
  l.address AS token_contract,
  l.topics[OFFSET(0)] AS event_sig,
  CONCAT('0x', SUBSTR(l.topics[OFFSET(1)], 27)) AS subject_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash AS tx_hash,
  l.log_index
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON b.number = l.block_number
WHERE l.address IN (@USDC_ADDR, @USDT_ADDR)
  AND l.topics[OFFSET(0)] IN UNNEST(@FREEZE_EVENT_TOPICS)
  AND DATE(b.timestamp) BETWEEN '2023-01-01' AND '2025-12-31';
```
검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT token_contract, COUNT(*) c \
   FROM \`$BQ_DATASET.fact_freeze_events\` \
  WHERE block_timestamp_utc BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31') \
  GROUP BY 1 ORDER BY c DESC"
```

3-2) OFAC 제재 주소(오프체인 → BQ 적재)
- 스크립트: `ingest/offchain/ofac_collector.py` (CSV 다운로드→EVM 주소 필터→BQ 로드)
- 테이블: `dim_sanctions_offchain(address, source, first_seen_ts, last_seen_ts, program, collected_at)`

검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT COUNT(*) n FROM \`$BQ_DATASET.dim_sanctions_offchain\`"
```

---

## 4) 통합 조인(피처 베이스)
- 입력: `fact_transfers`(= 현재 classified/std 기반 전송 베이스), `dim_blocks`, PRS/CRS 보조 테이블들
- 원칙: block_number 우선 조인, 블록없을 경우 `get_nearest_block(ts)`로 근사

스켈레톤(초기 베이스):
```sql
CREATE OR REPLACE VIEW `stablecoin_fds.v_prs_crs_features` AS
SELECT
  t.tx_hash,
  t.log_index,
  t.block_number,
  t.ts AS block_timestamp_utc,  -- t.ts는 UTC 가정
  t.token,
  t.amount_norm,
  t.from_addr,
  t.to_addr,
  -- PRS: 근접한 오라클 가격/DEX 신호(디코딩 완료 후 채움)
  NULL AS oracle_deviation_bps,
  NULL AS liquidity_risk_score,
  -- CRS: 직접 제재/동결 매칭
  IFNULL(s.address IS NOT NULL, FALSE) AS direct_sanctions_risk,
  IFNULL(f.tx_hash IS NOT NULL, FALSE) AS freeze_event_risk
FROM `stablecoin_fds.classified_transfers` t
LEFT JOIN `stablecoin_fds.dim_sanctions_offchain` s
  ON LOWER(t.from_addr) = LOWER(s.address) OR LOWER(t.to_addr) = LOWER(s.address)
LEFT JOIN `stablecoin_fds.fact_freeze_events` f
  ON f.block_number = t.block_number
  AND (LOWER(f.subject_address) = LOWER(t.from_addr) OR LOWER(f.subject_address) = LOWER(t.to_addr))
WHERE t.ts BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31');
```
검증:
```bash
bq --project_id="$GCP_PROJECT" query --use_legacy_sql=false \
"SELECT COUNT(*) n FROM \`$BQ_DATASET.v_prs_crs_features\` \
 WHERE block_timestamp_utc BETWEEN TIMESTAMP('2023-01-01') AND TIMESTAMP('2025-12-31')"
```

---

## 5) 소규모 리플레이(권장)
- 구간: USDC, 2023-03(은행 이슈 시기) 또는 최근 30일
- 목표: PRS/CRS 시그널이 직관적으로 반응하는지 확인

예시 추출:
```sql
CREATE OR REPLACE TABLE `stablecoin_fds.train_usdc_sample` AS
SELECT * FROM `stablecoin_fds.v_prs_crs_features`
WHERE token = 'usdc'
  AND block_timestamp_utc BETWEEN TIMESTAMP('2023-03-01') AND TIMESTAMP('2023-03-31');
```

---

## 6) 수치/품질 가드(최소)
- 카운트 정합성: 조인 후 건수 급감/폭증 감시(일자별 카운트 그래프)
- 커버리지: 오라클/DEX/동결/OFAC 매칭률(%)을 주기적으로 기록
- 비용: 모든 쿼리 파티션 필터 의무화, 드라이런 모드 체크 권장

---

## 7) 실행 순서(요약)
1) `dim_blocks` 확인/생성 → 2) Chainlink/Uniswap v3 Raw 인제스트 → 3) Freeze/OFAC 인제스트 → 4) `v_prs_crs_features` 생성 → 5) 리플레이 샘플 추출/육안 검증 → 6) 디코딩/UDF 확장 및 지표 보강(oracle_deviation/liquidity 등)

---

## 8) 다음 확장
- Chainlink/Uniswap 데이터 UDF 디코딩 추가(실제 가격/슬리피지/유동성 계산)
- Curve/Uniswap v2 보강, 근접 블록 매핑 정밀화(이전 블록 원칙 등)
- 그래프 기반 근접도/전파도 피처(지연 단계)

---

## 9) 보강 포인트(운영 관점)

### 9.1 블록 기준 정합/누수 방지
- 항상 “과거만”: 조인 시 `ON fact.block_number <= t.block_number`로 미래 정보 누수 방지
- 오프체인 매핑은 `nearest` 대신 “직전 블록” 사용
```sql
CREATE OR REPLACE FUNCTION `stablecoin_fds.block_at_or_before`(ts TIMESTAMP)
RETURNS INT64 AS ((
  SELECT block_number
  FROM `stablecoin_fds.dim_blocks`
  WHERE block_timestamp_utc <= ts
  ORDER BY block_timestamp_utc DESC
  LIMIT 1
));
```

### 9.2 PRS 인제스트: 원시→디코딩 2단계
- Chainlink AnswerUpdated: 1단계 raw(피드/tx/log/block) → 2단계 JS UDF로 answer 디코딩(decimals 반영)
```sql
-- BigQuery JS UDF 스켈레톤(실사용 시 16진→정수 파싱 구현)
CREATE OR REPLACE FUNCTION stablecoin_fds.udf_uint256_be(data BYTES, slot INT64)
RETURNS BIGNUMERIC AS ((
  -- 32바이트 슬롯 기준, slot=0 → 첫 슬롯
  CAST(NULL AS BIGNUMERIC)
));
```
- Uniswap v3 Swap: 1단계 raw → 2단계 UDF로 `amount0/1`, `sqrtPriceX96`, `liquidity`, `tick` 디코딩
- v3 가격 환산(뷰 내부 예):
```sql
SAFE_DIVIDE(
  POW(CAST(sqrt_price_x96 AS NUMERIC), 2),
  POW(2, 192)
) * POW(10, (decimals_token0 - decimals_token1)) AS price_t1_over_t0
```
- 풀 메타(토큰 순서/decimals)는 `configs/dex_pools.yaml`에 버전관리
- 보조: Uniswap v2 Sync(깊이/불균형 근사), Curve는 후순위

### 9.3 CRS 인제스트 강화
- 이벤트 집합 명시(ABI 기반, 하드코딩 지양)
  - USDC: Blacklisted/UnBlacklisted/(옵션 Paused)
  - USDT: AddedBlackList/RemovedBlackList/DestroyedBlackFunds
- `subject_address`는 topics[1]에서 파싱(0x40 포맷 정규화)
- 시점 구간화: `subject_address × [effective_from_block, effective_to_block]` 테이블로 조인 최적화
- OFAC: EVM 주소만 필터, `first_seen_ts/last_seen_ts/program/source/collected_at` 저장

### 9.4 피처: 간이판→고도화판 단계
- 간이 PRS: `|oracle_usd - 1.0|*1e4`, v3 liquidity 분위수, N분 `DEX_OUT` 유출속도
- 고도화 PRS: v3 가격 vs 1.0 괴리 + 가상 슬리피지(예: 10k/100k notional)
- 간이 CRS: OFAC/동결 직접 매칭(binary)
- 고도화 CRS: N-hop 근접도/허브성/전파도, allowance 급변 등(차후)

### 9.5 스키마/성능(CTAS 계층)
- raw → decoded → feature 3계층 분리(CTAS)
- 파티션: `DATE(block_timestamp_utc)` / 클러스터: feed/pool/subject 키
- `require_partition_filter=true` 고정 + 드라이런으로 스캔량 확인

### 9.6 데이터 품질 가드
- 커버리지 대시보드: 일자별 rowcount(oracle/pool/freeze)
- 조인 정합률: features JOIN transfers NULL 비율 0% 확인
- 라벨 커버리지: `from OR to 라벨 존재 비율`
- 골든 구간 테스트: 2023-03 USDC 주간 전파 테스트 자동화

### 9.7 스니펫(재사용)
```sql
-- 민트/번 태깅(컨텍스트)
CASE
  WHEN from_addr = '0x0000000000000000000000000000000000000000' THEN 'MINT'
  WHEN to_addr   = '0x0000000000000000000000000000000000000000' THEN 'BURN'
  ELSE category
END AS category2
```

### 9.8 운영 팁/우선순위
- 시그니처/오프셋 하드코딩 금지(ABI→해시/오프셋 빌드 스텝 주입)
- 주소 비교 규칙 통일(LOWER)
- Data Contract: `schema_version` 메타로 변경 추적
- 재시도/백오프, manifest(파일 해시/행수) 기록
- 우선순위: Freeze/OFAC → Uniswap v3 Raw→디코딩 → Chainlink → (선택) v2/Curve
