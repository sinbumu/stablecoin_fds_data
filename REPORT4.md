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


