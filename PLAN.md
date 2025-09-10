아래는 **Ethereum 단독 수집**을 전제로 한, 저장소 루트에 넣을 **`PLAN.md`** 초안입니다. 그대로 붙여 넣고 필요에 맞게 수정하면 됩니다.

---

# PLAN.md — Stablecoin FDS 데이터 수집·전처리 계획 (Ethereum Only)

> 목적: **석사 논문(1–2개월)** 내 마무리를 위해, **Ethereum 메인넷**에서 **USDT/USDC/DAI**의 전송 및 DEX 컨텍스트 데이터를 **빠르고 재현 가능하게** 수집·정제하여, **Pre-Sign FDS(디페그·동결 이중 리스크)** 연구에 필요한 학습/평가용 빅데이터를 구축한다.

---

## 0) 개요 요약

* **체인 범위**: Ethereum **단독** (EVM, BigQuery 공개데이터 활용 → 빠르고 안정적)
* **토큰 범위**: **USDT(ERC-20), USDC(ERC-20), DAI**
* **기간 권장**: `2023-01-01` \~ `현재` (최소 12–20개월 커버; 필요 시 `2024-01-01`\~현재로 축소 가능)
* **핵심 산출물(9/13 마감용)**

  1. 원시 전송 로그(3토큰) Parquet/CSV (GCS 저장)
  2. 표준화 스키마 테이블(정규화, decimals 적용)
  3. 주소 라벨링 초안(CEX/DEX 풀·라우터·발행사/트레저리)
  4. 일/시간 단위 집계표(카테고리별 In/Out)
  5. 사건 리플레이 샘플(예: 2023-03 USDC 디페그) 시각화 노트북

---

## 1) 아키텍처 & 스택

```
[BigQuery Public Datasets]  →  [GCS 버킷(raw/processed)]  →  [BigQuery(표준화·집계)]
          │                              │                           │
          └─(SQL 추출/Export)            └─(Parquet 저장)           └─(분석/시각화/모델 피처)
```

* **수집층**: BigQuery 공개 데이터셋(`crypto_ethereum.token_transfers` / 필요 시 `logs`)
* **저장층**: **GCS** 버킷

  * `gs://stablecoin-fds-raw/` (원시)
  * `gs://stablecoin-fds-processed/` (표준화/집계)
* **분석층**: **BigQuery**(파티션/클러스터링) + 노트북(Colab/Local)

---

## 2) 리포지토리 구조 (이 저장소 기준)

```
stablecoin_fds_data/
├─ PLAN.md
├─ README.md
├─ .env.example
├─ configs/
│  ├─ ethereum_only.yaml         # 토큰 주소/decimals/기간
│  ├─ providers.yaml             # (옵션) API 키/서브그래프 엔드포인트
│  └─ labels_seed.csv            # 주소 라벨 시드(CEX/DEX/발행사…)
├─ ingest/
│  └─ bigquery/
│     ├─ sql/
│     │  └─ eth_stable_transfers.sql
│     └─ export_to_gcs.sh        # BQ → GCS Parquet 내보내기
├─ processing/
│  ├─ standardize.py             # 스키마 통일/amount_norm 계산
│  ├─ label_merge.py             # 라벨 병합(Etherscan 등)
│  ├─ classify_context.py        # DIRECT/DEX + In/Out 태깅
│  ├─ aggregate_daily.py         # 일/시간 집계 생성
│  └─ feature_specs.md           # PRS/CRS 피처 목록 정의
├─ notebooks/
│  ├─ sanity_checks.ipynb        # 품질 점검
│  └─ event_replay.ipynb         # 사건 리플레이(USDC 2023-03 등)
└─ bin/
   ├─ gcs_sync.sh                # 로컬→GCS 동기화
   └─ bq_load.sh                 # GCS→BQ 로드(파티션)
```

---

## 3) 설정 파일

### 3.1 `configs/ethereum_only.yaml` (예시)

```yaml
ethereum:
  start_date: "2023-01-01"
  end_date: null  # 현재 시점까지
  tokens:
    usdc:
      address: "0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48"
      decimals: 6
    usdt:
      address: "0xdAC17F958D2ee523a2206206994597C13D831ec7"
      decimals: 6
    dai:
      address: "0x6B175474E89094C44Da98b954EedeAC495271d0F"
      decimals: 18
```

> **주의**: 주소/decimals는 공식 문서/검증된 소스로 고정 후 커밋.

### 3.2 `.env.example`

```
GCP_PROJECT=stablecoin-fds
GCS_BUCKET_RAW=stablecoin-fds-raw
GCS_BUCKET_PROCESSED=stablecoin-fds-processed
BQ_DATASET=stablecoin_fds
```

---

## 4) 수집 단계 (Ethereum Only)

### 4.1 BigQuery 쿼리 (토큰 전송 로그 추출)

`ingest/bigquery/sql/eth_stable_transfers.sql`

```sql
-- Ethereum: USDC/USDT/DAI 전송 로그 (2023-01-01 이후)
SELECT
  block_timestamp AS ts,
  transaction_hash AS tx_hash,
  token_address,
  from_address AS from_addr,
  to_address   AS to_addr,
  value        AS amount_raw,
  block_number
FROM `bigquery-public-data.crypto_ethereum.token_transfers`
WHERE token_address IN (
  '0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48', -- USDC
  '0xdAC17F958D2ee523a2206206994597C13D831ec7', -- USDT
  '0x6B175474E89094C44Da98b954EedeAC495271d0F'  -- DAI
)
AND block_timestamp >= TIMESTAMP('2023-01-01');
```

* **확장**: 필요 시 `logs` 테이블에서 **블랙리스트/동결 이벤트**(USDT/USDC) topic으로 추가 추출.

### 4.2 BigQuery → GCS 내보내기

`ingest/bigquery/export_to_gcs.sh` (예시)

```bash
#!/usr/bin/env bash
set -euo pipefail

PROJECT=${GCP_PROJECT}
DATASET_TEMP="${BQ_DATASET}_temp"
TABLE_SRC="bigquery-public-data.crypto_ethereum.token_transfers"
TABLE_OUT="${DATASET_TEMP}.eth_stable_transfers_2023p"

# 1) 임시 테이블 생성
bq --project_id="$PROJECT" query --use_legacy_sql=false \
  --destination_table="$TABLE_OUT" --replace=true \
  "$(cat ingest/bigquery/sql/eth_stable_transfers.sql)"

# 2) GCS로 내보내기 (Parquet)
gsutil -m rm -r gs://${GCS_BUCKET_RAW}/raw/chain=eth/**
bq --project_id="$PROJECT" extract \
  --destination_format=PARQUET \
  "$TABLE_OUT" \
  gs://${GCS_BUCKET_RAW}/raw/chain=eth/token=stable/date=2023p/*.parquet
```

> **주의**: 실제 운영에선 토큰별/월별 파티션으로 나눠 내보내는 것을 권장.

---

## 5) 표준화·라벨링·집계

### 5.1 표준 스키마 정의

출력 테이블(또는 Parquet)의 공통 필드:

```
ts(UTC timestamp), chain='eth', tx_hash, log_index (옵션),
token ∈ {usdt, usdc, dai}, token_address,
from_addr, to_addr,
amount_raw (uint), decimals, amount_norm (double), block_number
```

* **정규화**: `amount_norm = amount_raw / 10^decimals`

`processing/standardize.py` 수행 결과는 GCS `processed/std/` 및 BQ `stablecoin_fds.std_token_transfers`로 저장.

### 5.2 주소 라벨링

* `configs/labels_seed.csv`에 **CEX(핫월렛/입금)**, **DEX 풀/라우터(유니스왑/커브)**, **발행사/트레저리**, **주요 프로토콜** 등 핵심 주소를 수기/반자동 수집.
* `processing/label_merge.py`로 라벨 머지 → `from_type`/`to_type` 부여.

라벨 예시 컬럼:

```
address, label, type
0x..., BINANCE_HOT, CEX
0x..., UNISWAP_V3_POOL_USDC/ETH, DEX_POOL
0x..., UNISWAP_V3_ROUTER, DEX_ROUTER
0x..., CIRCLE_TREASURY, ISSUER
```

### 5.3 컨텍스트 분류 & 방향성 태깅

`processing/classify_context.py`:

* `category`: {`DIRECT_TRANSFER`, `DEX_SWAP`, `DEFI_OTHER`}

  * 규칙: 풀/라우터 주소 관여 시 `DEX_SWAP`, 그 외 직접 전송은 `DIRECT_TRANSFER`
* `direction`: {`CEX_IN`, `CEX_OUT`, `DEX_IN`, `DEX_OUT`, `P2P`}

  * 예: `to_type=CEX` → `CEX_IN`, `from_type=DEX_POOL` → `DEX_OUT`

### 5.4 일/시간 집계

`processing/aggregate_daily.py`:

* 그룹바이: `DATE(ts), token, category, direction` (+ 옵션: `from_type`, `to_type`)
* 메트릭: `tx_count`, `volume_sum(amount_norm)`, `unique_addr`

출력: `stablecoin_fds.daily_flows`

---

## 6) FDS 연구용 피처(요약)

`processing/feature_specs.md`에 상세 정의. 여기서는 핵심만:

### 6.1 PRS(디페그 노출도) 후보 피처

* DEX 풀 상태(유니스왑/커브): 풀 비중/깊이/가상가격(virtual price), 최근 N분 **유출속도**, 예상 **슬리피지**(고정 주문 크기 기준)
* **오라클-현물 괴리**(가능 시), **DEX-CEX 스프레드**(옵션)
* 시계열 랙(5m/30m) 및 변동성

### 6.2 CRS(동결/검열 위험) 후보 피처

* **동결/블랙리스트 이벤트** 전후 윈도우 내 거래/주소
* **그래프 근접도**(제재/악성 라벨과의 거리), 허브성/전파도
* **allowance 급변**, 신규주소 연쇄, 라우팅 난독화 지표

> 초기 9/13 마감 전에는 **데이터 기반** 마련(표준화/라벨/집계)까지가 목표, 이후 학습·추론은 2주차\~.

---

## 7) 일정(1–2개월 내 완결 목표)

### 9/13 마감 전 (Week 1)

* Day 1: GCP 프로젝트/권한, GCS/BQ 생성, `configs/ethereum_only.yaml` 확정
* Day 2–3: **BigQuery 추출 → GCS Parquet 내보내기**, 표본 검증
* Day 4: `standardize.py` 실행(정규화/스키마 통일), 중복/타임존/결측 체크
* Day 5: `labels_seed.csv` 초안 작성 → `label_merge.py` → `classify_context.py`
* Day 6: `aggregate_daily.py`로 집계 생성
* Day 7: `event_replay.ipynb`로 **USDC 2023-03** 사건 리플레이(스파이크 확인)

### 이후 3–6주

* Week 2–3: **PRS/CRS 피처 구현** + 베이스라인 모델(XGBoost/LGBM) → SHAP 설명
* Week 4: **리플레이 평가**(PR-AUC, Recall\@FPR=1%, 리드타임), 오류 분석
* Week 5: **Pre-Sign API** 목업(지연 p95 측정), 경고 정책 튜닝
* Week 6: 결과 도표/표 정리, 본문 작성(한계/확장: 멀티체인)

---

## 8) 비용·성능 가이드 (Ethereum Only)

* **BigQuery 쿼리**: `$5/TB` — 파티션/필터 최적화 시 **두 자릿수 \$/월**
* **GCS 저장**: `$0.02/GB·월` — Parquet 압축으로 수\~수십 GB 관리
* **성능 팁**

  * BQ 테이블 **파티셔닝**(DATE(ts)), **클러스터링**(`token_address`, `from_address`)
  * 내보내기/로딩은 토큰×월 단위로 나누어 배치 처리

---

## 9) 품질·거버넌스

* **재현성**: 모든 쿼리/스크립트 **버전 고정**(커밋), 실행 로그 남기기
* **라벨 신뢰성**: 핵심 주소(CEX/DEX/발행사)는 **수기 검증** 후 `labels_seed.csv`에 주석
* **커버리지 메타**: 시작/끝 블록, 행 수, 누락 구간 기록(`README.md`에 테이블로 요약)

---

## 10) 마일스톤별 산출물 체크리스트

* [ ] `raw/` Parquet (eth, usdt/usdc/dai, 기간별 파티션) @ GCS
* [ ] `std_token_transfers` (정규화) @ BQ
* [ ] `addr_labels` (라벨 맵) @ BQ
* [ ] `daily_flows` (집계) @ BQ
* [ ] 리플레이 노트북(USDC 2023-03) 실행 스크린샷/PNG
* [ ] `feature_specs.md` 초안(PR S/CRS 피처 테이블)

---

## 부록 A) 실무 명령 예시

### A.1 로컬 → GCS 동기화

```bash
./bin/gcs_sync.sh out/ gs://$GCS_BUCKET_RAW/raw/
```

### A.2 GCS → BigQuery 로드(파티션)

```bash
bq load --source_format=PARQUET \
  --time_partitioning_field ts \
  $BQ_DATASET.std_token_transfers \
  gs://$GCS_BUCKET_PROCESSED/std/chain=eth/token=*/date=*/part-*.parquet
```

---

## 부록 B) 논문 영향도 메모

* **Ethereum 단독**이어도: Uniswap/Curve 중심의 **DEX 유동성/가격 신호**와 **USDT/USDC 동결 이벤트**를 충분히 커버 → \*\*Pre-Sign FDS(PRS+CRS)\*\*의 **핵심 검증**에 차질 없음.
* **한계 서술**: 멀티체인 일반화(Tron/BSC)는 후속 확장으로 명시.

---

**결론**: 본 계획은 \*\*빠른 수집(1주 내)\*\*과 **재현 가능한 파이프라인**에 초점을 둡니다. Ethereum 단독으로도 **충분한 학술 기여**(데이터셋 구축, 피처 설계, 실시간 제약하 성능 검증)가 가능합니다.
