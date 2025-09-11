아래는 저장소 루트에 추가할 **`PLAN2.md`** 초안입니다. *(기존 `PLAN.md`가 “ETH 전송 원시 수집+기초 가공”이라면, `PLAN2.md`는 **PRS/CRS 학습용 데이터 생성**에 초점을 둡니다.)*

---

# PLAN2.md — PRS/CRS 학습용 추가 데이터 수집·정합·피처 생성 계획 (Ethereum Only)

> 목적: **Pre-Sign FDS(PRS+CRS)** 학습·평가를 위해, `PLAN.md`에서 수집한 **ETH 전송 원시 데이터**를 보강하는 **오라클/DEX/동결/제재** 보조 데이터를 **블록번호 기준**으로 정합하여 **학습 테이블**과 **피처 뷰**를 구축한다.

---

## 0) 이 문서에서 추가되는 것 (PLAN.md 대비)

* **PRS용**: 오라클 가격(Chainlink), DEX 풀 상태(Uniswap v3/v2, Curve), 슬리피지/풀 불균형 지표
* **CRS용**: **온체인 동결/블랙리스트 이벤트(USDC/USDT)**, **오프체인 OFAC 제재 주소**
* **정합 원칙**: `dim_blocks`(블록번호→UTC) 기준 **블록번호 우선** 조인, 블록번호 없는 외부 데이터는 **가까운 블록 매핑**
* **산출물**: `fact_*` 보조 팩트, `dim_*` 라벨/블록, 통합 피처 뷰 `v_prs_crs_features`

---

## 1) 전체 아키텍처

```
(PLAN.md) fact_transfers (ETH USDT/USDC/DAI)
                 │
                 ▼
         ┌───────────────────────┐
         │   추가 인제스트층     │
         ├───────────┬───────────┤
         │   PRS     │    CRS     │
         │ • 오라클  │ • OFAC     │
         │ • DEX풀   │ • 동결로그 │
         └───────────┴───────────┘
                 │ (block_number 정합)
                 ▼
           dim_blocks 기준 통합
                 │
                 ▼
      v_prs_crs_features (학습용 피처)
```

---

## 2) 테이블 설계(스키마 개요)

* `stablecoin_fds.dim_blocks`
  `{ block_number, block_timestamp_utc, block_date, block_hour }`
* `stablecoin_fds.fact_transfers` *(PLAN.md 산출물, 정규화 완료)*
  `{ ts, chain='eth', tx_hash, log_index, token, token_address, from_addr, to_addr, amount_norm, block_number, ... }`
* **PRS 보조**

  * `stablecoin_fds.fact_oracle_prices`
    `{ feed_address, block_number, block_timestamp_utc, price_scaled, round_id, tx_hash, log_index }`
  * `stablecoin_fds.fact_univ3_pool_state`
    `{ pool_address, block_number, block_timestamp_utc, sqrt_price_x96, liquidity, tick, tx_hash, log_index }`
  * `stablecoin_fds.fact_univ2_sync`
    `{ pair_address, block_number, block_timestamp_utc, reserve0, reserve1, tx_hash, log_index }`
  * `stablecoin_fds.fact_curve_pool` *(서브그래프/보조 API 인제스트 시)*
    `{ pool_address, block_number, block_timestamp_utc, virtual_price, ... }`
* **CRS 보조**

  * `stablecoin_fds.fact_freeze_events`
    `{ token_contract, event_sig, event_type, subject_address, block_number, block_timestamp_utc, tx_hash, log_index }`
  * `stablecoin_fds.dim_sanctions_offchain`
    `{ address, source, first_seen_ts, last_seen_ts, program, collected_at }`
  * `stablecoin_fds.dim_labels` *(CEX/DEX/발행사 등 주소 라벨)*
    `{ address, label, type, source, updated_at }`

> ⚠️ **주의**: 이벤트 시그니처/토픽은 **ABI로 확인 후 해시 생성**. 샘플 해시는 플레이스홀더로 두고 스크립트에서 자동 주입.

---

## 3) 정합 원칙 (Timestamp/Block)

1. **온체인=블록번호가 왕**: 모든 온체인 보조 팩트는 **반드시 `block_number` 보유** → `dim_blocks`로 UTC ts 부여
2. **오프체인=주소+시점**: 블록번호 없으면 `get_nearest_block(ts)` or `block_at_or_before(ts)`로 근사 매핑
3. **충돌 우선순위**: `onchain_freeze > ofac_offchain > community_label` / 라벨은 `effective_from~to` 구간 운용

---

## 4) 설정 파일 (예시)

`configs/oracle_feeds.yaml`

```yaml
ethereum_feeds:
  usdc_usd: { address: "0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6", decimals: 8 }
  usdt_usd: { address: "0x3E7d1eAB13ad0104d2750B8863b489D65364e32D", decimals: 8 }
  dai_usd:  { address: "0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9", decimals: 8 }
  eth_usd:  { address: "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419", decimals: 8 } # 보조
```

`configs/dex_pools.yaml`

```yaml
uniswap_v3_pools:
  usdc_eth_005: { address: "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640", fee: 500 }
  usdc_eth_030: { address: "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8", fee: 3000 }
  dai_usdc_001: { address: "0x5777d92f208679db4b9778590fa3cab3ac9e2168", fee: 100 }
curve_pools:
  three_pool:   { address: "0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7" }
```

`configs/sanctions_sources.yaml`

```yaml
ofac:
  sdn_csv: "https://home.treasury.gov/ofac/downloads/sdn.csv"
  consolidated_csv: "https://home.treasury.gov/ofac/downloads/consolidated/consolidated.csv"
  poll_interval_days: 7
```

---

## 5) 인제스트 SQL/스크립트 (요약)

### 5.1 공통: 블록 기준 테이블

`processing/build_dim_blocks.sql`

```sql
CREATE OR REPLACE TABLE `stablecoin_fds.dim_blocks` AS
SELECT
  number AS block_number,
  timestamp AS block_timestamp_utc,
  DATE(timestamp) AS block_date,
  EXTRACT(HOUR FROM timestamp) AS block_hour
FROM `bigquery-public-data.crypto_ethereum.blocks`
WHERE DATE(timestamp) >= '2023-01-01';

CREATE OR REPLACE FUNCTION `stablecoin_fds.get_nearest_block`(ts TIMESTAMP)
RETURNS INT64 AS ((
  SELECT block_number
  FROM `stablecoin_fds.dim_blocks`
  ORDER BY ABS(TIMESTAMP_DIFF(block_timestamp_utc, ts, SECOND))
  LIMIT 1
));
```

### 5.2 PRS — 오라클 가격 (Chainlink)

`ingest/bigquery/sql/eth_chainlink_prices.sql`

```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_oracle_prices` AS
SELECT
  l.address AS feed_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash, l.log_index,
  -- data 파싱은 피드별 ABI/decimals에 맞춰 UDF 사용 권장
  SAFE_CAST(NULL AS NUMERIC) AS price_scaled,  -- TODO: UDF로 대체
  SAFE_CAST(NULL AS INT64)   AS round_id       -- TODO: UDF로 대체
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON l.block_number = b.number
WHERE l.address IN UNNEST(@feed_addresses)
  AND l.topics[OFFSET(0)] = @ANSWER_UPDATED_TOPIC; -- keccak 이벤트해시(런타임 주입)
```

> *실행 전*: `feed_addresses`, `ANSWER_UPDATED_TOPIC`을 세션 파라미터로 주입하고, `UDF_decode_answerupdated`로 `price_scaled` 추출.

### 5.3 PRS — Uniswap v3 풀 상태

`ingest/bigquery/sql/eth_uniswap_v3_swaps.sql`

```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_univ3_pool_state` AS
SELECT
  l.address AS pool_address,
  l.block_number,
  b.timestamp AS block_timestamp_utc,
  l.transaction_hash, l.log_index,
  -- data에서 sqrtPriceX96/liq/tick 추출(UDF 권장)
  SAFE_CAST(NULL AS BIGNUMERIC) AS sqrt_price_x96, -- TODO
  SAFE_CAST(NULL AS BIGNUMERIC) AS liquidity,      -- TODO
  SAFE_CAST(NULL AS INT64)      AS tick           -- TODO
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON l.block_number = b.number
WHERE l.topics[OFFSET(0)] = @UNIV3_SWAP_TOPIC
  AND l.address IN UNNEST(@target_pools);
```

### 5.4 CRS — 동결/블랙리스트 이벤트

`ingest/bigquery/sql/eth_freeze_events.sql`

```sql
CREATE OR REPLACE TABLE `stablecoin_fds.fact_freeze_events` AS
SELECT
  l.address AS token_contract,
  l.topics[OFFSET(0)] AS event_sig,
  l.block_number, b.timestamp AS block_timestamp_utc,
  l.transaction_hash, l.log_index,
  -- indexed address (토픽 배열 위치는 토큰별 ABI로 확인)
  CONCAT('0x', SUBSTR(l.topics[OFFSET(1)], 27)) AS subject_address,
  CASE l.address
    WHEN @USDC_ADDR THEN 'USDC'
    WHEN @USDT_ADDR THEN 'USDT'
    ELSE 'UNKNOWN'
  END AS token_symbol
FROM `bigquery-public-data.crypto_ethereum.logs` l
JOIN `bigquery-public-data.crypto_ethereum.blocks` b
  ON l.block_number = b.number
WHERE l.address IN (@USDC_ADDR, @USDT_ADDR)
  AND l.topics[OFFSET(0)] IN UNNEST(@FREEZE_EVENT_TOPICS);
```

### 5.5 CRS — OFAC 제재 주소 (오프체인)

`ingest/offchain/ofac_collector.py`

```python
# 요지: OFAC CSV 다운로드 → EVM 주소 정규표현식으로 추출 → BQ dim_sanctions_offchain 적재
# - 주소형식 검증(0x[0-9a-fA-F]{40})
# - collected_at, source, program 등 메타 포함
```

---

## 6) 피처 생성(요약 스펙)

### 6.1 PRS (Peg-Risk)

* **오라클-현물 괴리(bps)**: `|oracle_usd - 1.0| * 1e4`
* **풀 유동성 위험**: 유동성 저수심 구간(예: v3 `liquidity` 임계), 유출속도(최근 N분 DEX\_IN/OUT)
* **예상 슬리피지(bps)**: v3 상태에서 고정 노치 주문 시뮬 *(간이 근사 → 후속 정교화)*
* **풀 불균형/가상가격 드리프트**: v2 reserve 비율, Curve `virtual_price` 변화율

### 6.2 CRS (Censorship-Risk)

* **직접 제재 위험**: 송/수신 주소가 OFAC/동결 리스트와 일치
* **근접도 위험(확장)**: N-hop 이내 제재 주소 존재, 허브성/전파도, allowance 급변, 신규주소 연쇄
  *(그래프 피처는 2단계 진행; 1차는 직접 매칭/동결 전후 윈도우 중심)*

---

## 7) 통합 뷰 (학습 입력 생성)

`processing/build_integrated_features.sql` *(요지)*

* `fact_transfers` + `dim_blocks` → **시간 정규화 기반** 전송 베이스
* PRS 조인: `fact_oracle_prices`(동일 블록) + `fact_univ3_pool_state`(동일/근접 블록)
* CRS 조인: `dim_sanctions_offchain`(주소매칭) + `fact_freeze_events`(동일/이전 블록)
* 출력: `v_prs_crs_features`

  ```
  { tx_hash, block_number, block_timestamp_utc,
    token, amount_norm, from_addr, to_addr,
    oracle_deviation_bps, liquidity_risk_score, slippage_bps (옵션),
    direct_sanctions_risk, freeze_event_risk,
    prs_score (초기 가중 합), crs_score (초기 가중 합) }
  ```

---

## 8) 실행 순서(권장 타임라인)

### Phase 1 (주 1): 기반 구축

* [ ] `dim_blocks` 생성
* [ ] 오라클 가격(체인링크) 인제스트
* [ ] OFAC 현재 제재 주소 수집 → `dim_sanctions_offchain`

### Phase 2 (주 2): PRS 데이터

* [ ] Uniswap v3 풀 상태 인제스트
* [ ] (선택) Uniswap v2 `Sync`, Curve 가상가격 보강
* [ ] 간이 **슬리피지 계산기** 구현 및 검증

### Phase 3 (주 3): CRS 데이터

* [ ] USDC/USDT 동결/블랙리스트 이벤트 인제스트
* [ ] (선택) 히스토리컬 제재 타임라인 구축(효력 시작/종료)

### Phase 4 (주 4): 통합·학습

* [ ] `v_prs_crs_features` 뷰 생성
* [ ] PRS/CRS 베이스라인 모델(XGBoost/LGBM) 학습 + SHAP 설명
* [ ] 사건 리플레이(USDC 2023-03)로 PRS/CRS 반응 검증

---

## 9) 품질/거버넌스 체크리스트

* [ ] **정합성**: 모든 온체인 보조 팩트에 `block_number` 존재, `dim_blocks`와 ts 일치
* [ ] **커버리지**: 피드/풀/이벤트 **누락율** 리포트(일자별 카운트/결측)
* [ ] **라벨 신뢰성**: 핵심 주소(CEX/DEX/발행사) 수기 검증 로그 유지
* [ ] **비용 관리**: BQ 파티션/클러스터링, 드라이런으로 쿼리량 확인
* [ ] **재현성**: 쿼리/스크립트 버전/실행시각·소스 URL 메타 저장

---

## 10) 리스크 & 대응

* **이벤트 해시/파싱 오류** → **ABI에서 해시 자동 생성**(스크립트), UDF로 데이터 파싱
* **오프체인 ts-블록 매핑 오차** → `nearest_block()` 기반 ±30s 정책·소스별 문서화
* **Curve/서브그래프 가용성** → v2/v3 지표로 대체 가능, 후속 보강
* **그래프 피처 비용** → 1차는 직접 매칭 위주, N-hop은 샘플/윈도우로 제한

---

## 부록 A) 디렉토리 추가/변경

```
configs/
  oracle_feeds.yaml
  dex_pools.yaml
  sanctions_sources.yaml
ingest/
  bigquery/sql/
    eth_chainlink_prices.sql
    eth_uniswap_v3_swaps.sql
    eth_freeze_events.sql
  offchain/
    ofac_collector.py
processing/
  build_dim_blocks.sql
  build_integrated_features.sql
  slippage_calculator.py
  temporal_sanctions.py   # (옵션) 제재 타임라인
analysis/
  prs_model_training.py
  crs_model_training.py
  data_quality_checks.py
```

---

## 부록 B) 학습 셋 추출 예시

```sql
-- 최근 12개월, USDC만, DEX 관련 전송 중심 학습셋
CREATE OR REPLACE TABLE `stablecoin_fds.train_usdc_12m_dex` AS
SELECT *
FROM `stablecoin_fds.v_prs_crs_features`
WHERE token = 'usdc'
  AND block_timestamp_utc >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 MONTH)
  AND category = 'DEX_SWAP';
```

---

**메모**

* 본 문서의 이벤트 시그니처/데이터 오프셋은 **런타임에 ABI로 검증**해 주입하세요.
* 주소/피드/풀 목록은 `configs/*.yaml`에서 **버전 관리**하고 갱신 로그를 남깁니다.

---

필요하면 위 스켈레톤을 **실행 가능한 쿼리/스크립트**로 더 구체화해 줄게요. (UDF 포함, 세션 파라미터 주입, 파티션 나누기 등)
