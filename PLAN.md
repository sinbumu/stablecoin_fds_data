좋아! Notion에 바로 올리기 좋은 **페이지 템플릿**이랑, 네 GitHub 저장소(`sinbumu/stablecoin_fds_data`) 기준 **폴더 구조/스크립트 배치안**까지 한 번에 정리해줄게. (목표: 9/13까지 “원시데이터 수집 + 1차 전처리/인덱싱” 준비 완료)

---

# (Notion 템플릿) Stablecoin FDS — 빅데이터 수집·전처리 계획

## 0. 요약

* **대상 토큰**: USDT, USDC, DAI
* **대상 체인**: Ethereum, BSC, Tron *(EVM: ETH/BSC, 비EVM: Tron)*
* **DEX 컨텍스트**: Uniswap(v2/v3), Curve, PancakeSwap(보조)
* **수집 경로**:

  * 과거 대량: **BigQuery**(ETH/Tron) + **Cryo+RPC**(BSC/보완)
  * 실시간/보조: **Etherscan/BscScan/TronScan API**, **The Graph(Subgraph)**
* **저장/분석 인프라**: **GCS(Google Cloud Storage)** + **BigQuery**
* **산출물(9/13 전)**:

  1. 원시 전송 로그(체인×토큰) Parquet/CSV,
  2. 통일 스키마 테이블(정규화),
  3. 주소 라벨링 초안(CEX/DEX/발행사),
  4. 일별 집계(카테고리별 In/Out) 1차본

---

## 1. 범위 & 목표

* **목표**: FDS용 **Pre-Sign(서명 전)** 분석을 위한 **원시데이터 확보 + 1차 전처리/인덱싱**
* **범위**: 2023–현재(최소 1년) — USDC(23.03 디페그), USDT(Curve 3pool 불균형) 포함

---

## 2. 데이터 소스 & 선택 기준

* **BigQuery**: `crypto_ethereum.token_transfers` 등 → ETH/Tron 과거 로그 대량 추출에 최적
* **Cryo CLI + RPC**: 계약별 `eth_getLogs` 대량 ETL(Parquet 저장, ETH/BSC 모두 OK)
* **Explorer/API**: Etherscan/BscScan/TronScan 페이징(백업/부분 보완)
* **Subgraph**: Uniswap/Curve/PancakeSwap GraphQL(스왑/풀 메타데이터 보강)

---

## 3. GCP 인프라 (권장 표준)

* **프로젝트**: `stablecoin-fds` *(예시)*
* **GCS 버킷**: `gs://stablecoin-fds-raw` (원시), `gs://stablecoin-fds-processed` (전처리)

  * 경로 규칙: `raw/chain=<eth|bsc|tron>/token=<usdt|usdc|dai>/date=YYYY-MM-DD/*.parquet`
* **BigQuery 데이터셋**: `stablecoin_fds`

  * 테이블 예:

    * `raw_token_transfers` (파티션: `DATE(block_timestamp)`)
    * `std_token_transfers` (정규화 스키마)
    * `addr_labels` (주소 라벨 맵)
    * `daily_flows` (일별 집계: CEX/DEX/Direct × In/Out)

---

## 4. 수집 파이프라인(실행 플로우)

### 4.1 과거 덤프(우선)

1. **ETH/Tron → BigQuery 쿼리**로 토큰별 전송 로그 추출 → **GCS로 내보내기**
2. **BSC → Cryo+RPC**로 컨트랙트별 `Transfer` 이벤트 파싱 → **GCS 업로드**
3. (옵션) Explorer/API로 **누락 구간** 보완(블록 범위로 페이징)

### 4.2 DEX 컨텍스트 보강(선택)

* Subgraph로 **스왑/풀 정보** 조회 → 풀/라우터 주소 리스트 갱신 → 전송 레코드에 **DEX 태그** 합치기

---

## 5. 스키마(표준화)

### 5.1 원시 → 표준 스키마

* 필드(예):

  ```
  ts (UTC), chain (eth|bsc|tron), tx_hash, log_index,
  from_addr, to_addr, token (usdt|usdc|dai),
  amount_raw, decimals, amount_norm (decimal 통일), block_number
  ```
* **정규화**: `amount_norm = amount_raw / 10^decimals` *(USDC=6, DAI=18 유의)*

### 5.2 컨텍스트 태그

* `from_type`/`to_type`: {CEX, DEX\_POOL, DEX\_ROUTER, USER, ISSUER, TREASURY, PROTOCOL}
* `category`: {DIRECT\_TRANSFER, DEX\_SWAP, DEFI\_OTHER}
* `direction`: {CEX\_IN, CEX\_OUT, DEX\_IN, DEX\_OUT, P2P}

---

## 6. 전처리/인덱싱

* **주소 인덱스**: 주소→(입출금 총액/건수, 최근활동시각, 고유상대수)
* **주소 라벨링**: Etherscan/BscScan/TronScan 라벨 + 수동 큐레이션 → `addr_labels`
* **집계**: 일/시간 단위 `daily_flows`(체인×토큰×카테고리×방향)
* **그래프 준비(확장)**: 주소=노드, 전송=에지(시간/금액 속성) — 차수/근접도 계산 기반

---

## 7. 검증(샘플)

* **타임존/중복/결측** 점검
* **사건 리플레이**: 2023-03 USDC 디페그, USDT 3pool 불균형 — 스파이크/흐름 확인
* **라벨 샘플링**: CEX/DEX 풀/라우터 라벨 정확도 수기 점검

---

## 8. 보안/권한/비용

* **IAM**: 수집용 SA에 GCS(Object Admin) & BQ(Data Editor) 최소 권한
* **비용**: BQ 쿼리시 **파티션 필터** 필수, GCS는 **Parquet**로 압축 저장
* **키/비밀**: `.env`로 분리(Etherscan/BscScan 키, RPC URL 등)

---

## 9. 일정(마일스톤)

* **D-3**: GCS/BQ 세팅, 계약주소/소수점 고정(`contracts.yaml`)
* **D-2**: ETH/Tron 덤프 완료, BSC Cryo 수집 시작
* **D-1**: 스키마 정규화, 주소 라벨 초안, 일별 집계 생성
* **D-day(9/13)**: 검증(사건 리플레이 차트/표) + 산출물 목록화

---

# (Repo 가이드) `sinbumu/stablecoin_fds_data` 구조/스크립트

```
stablecoin_fds_data/
├─ README.md
├─ .env.example                     # API 키/RPC URL 템플릿
├─ configs/
│  ├─ contracts.yaml               # 체인×토큰 컨트랙트주소, decimals
│  ├─ providers.yaml               # RPC/서브그래프/키 설정
│  └─ labels_seed.csv              # 초기 주소 라벨(수동 큐레이션)
├─ ingest/
│  ├─ bigquery/
│  │  ├─ sql/
│  │  │  ├─ eth_usdc_transfers.sql
│  │  │  ├─ eth_usdt_transfers.sql
│  │  │  └─ eth_dai_transfers.sql
│  │  └─ export_to_gcs.sh         # BQ→GCS 내보내기 스크립트
│  ├─ cryo/
│  │  ├─ run_cryo_eth.sh
│  │  ├─ run_cryo_bsc.sh
│  │  └─ README.md
│  └─ tron/
│     ├─ tronscan_pull.py         # TRC-20 페이징 수집(기간 슬라이싱)
│     └─ README.md
├─ processing/
│  ├─ standardize.py              # 스키마 통일/소수점 정규화
│  ├─ label_merge.py              # explorer 라벨 병합
│  ├─ classify_context.py         # DIRECT/DEX, In/Out 태깅
│  └─ aggregate_daily.py          # 일별 집계 테이블 생성
├─ notebooks/
│  ├─ sanity_checks.ipynb         # 데이터 품질 점검
│  └─ event_replay.ipynb          # 사건 리플레이(USDC/USDT)
├─ docker/
│  ├─ Dockerfile
│  └─ docker-compose.yml
└─ bin/
   ├─ gcs_sync.sh                 # 로컬→GCS 동기화
   └─ bq_load.sh                  # GCS→BQ 로드(파티션)
```

### 핵심 파일 설명

* **`configs/contracts.yaml`**

  ```yaml
  ethereum:
    usdc: { address: "0xA0b8...6eB48", decimals: 6 }
    usdt: { address: "0xdAC1...ec7", decimals: 6 }
    dai:  { address: "0x6B17...271d0F", decimals: 18 }
  bsc:
    usdt: { address: "0x55d3...97955", decimals: 18 }
    usdc: { address: "<BSC_USDC>", decimals: 18 }
    dai:  { address: "<BSC_DAI>", decimals: 18 }
  tron:
    usdt: { address: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", decimals: 6 }
  ```

  *(정확 주소는 공식 문서 기준으로 고정. 커밋에 명시)*

* **`ingest/bigquery/sql/eth_usdc_transfers.sql` (예시)**

  ```sql
  SELECT block_timestamp AS ts,
         transaction_hash AS tx_hash,
         from_address AS from_addr,
         to_address   AS to_addr,
         'usdc'       AS token,
         value        AS amount_raw,
         6            AS decimals,
         block_number
  FROM `bigquery-public-data.crypto_ethereum.token_transfers`
  WHERE token_address = '0xA0b86991c6218b36c1d19d4a2e9Eb0cE3606eB48'
    AND block_timestamp >= TIMESTAMP('2023-01-01');
  ```

* **`ingest/cryo/run_cryo_bsc.sh` (예시)**

  ```bash
  #!/usr/bin/env bash
  set -euo pipefail
  export ETH_RPC_URL=${BSC_RPC_URL} # .env에서 로드
  TOKEN_ADDR="0x55d398326f99059fF775485246999027B3197955" # USDT(BSC)
  OUT="out/bsc/usdt"
  mkdir -p "$OUT"
  cryo logs \
    --contract ${TOKEN_ADDR} \
    -b 25000000:latest \
    --requests-per-second 5 \
    --max-concurrent-requests 5 \
    --parquet ${OUT}/bsc_usdt_logs.parquet
  ```

* **`ingest/tron/tronscan_pull.py` (예시 스켈레톤)**

  ```python
  import requests, time, csv, os
  BASE = "https://apilist.tronscanapi.com/api/token_trc20/transfers"
  CONTR = os.environ["TRON_USDT"]  # configs에서 주입
  def pull(start_ts, end_ts, out_csv):
      start, limit, page = start_ts, 200, 0
      with open(out_csv, "w", newline="") as f:
          w = csv.writer(f); w.writerow(["ts","tx_hash","from","to","amount_raw","decimals","block"])
          while True:
              url = f"{BASE}?contract_address={CONTR}&limit={limit}&start={page}&start_timestamp={start}&end_timestamp={end_ts}"
              j = requests.get(url, timeout=30).json()
              rows = j.get("token_transfers", [])
              if not rows: break
              for r in rows:
                  w.writerow([r["block_ts"], r["transaction_id"], r["from_address"], r["to_address"], r["quant"], 6, r["block"]])
              page += limit; time.sleep(0.3)
  ```

* **`processing/standardize.py` (핵심 로직 요지)**

  * 입력: 체인×토큰별 원시 파일 → 공통 스키마로 변환, `amount_norm` 계산, `ts`을 UTC로 통일
  * 출력: `gs://stablecoin-fds-processed/std/chain=.../token=.../date=.../*.parquet`

* **`processing/classify_context.py`**

  * 라벨 소스(`addr_labels`): CEX/DEX\_POOL/DEX\_ROUTER/ISSUER/…
  * 규칙: `from_addr`/`to_addr`에 라벨 매칭 → `category`/`direction` 필드 생성

* **`processing/aggregate_daily.py`**

  * `std_token_transfers` → 그룹바이(`DATE(ts), chain, token, category, direction`) → `daily_flows` 테이블 생성

---

## GCS/BQ 워크플로(명령 예시)

* **로컬 → GCS 업로드**

  ```bash
  gsutil -m rsync -r out/ gs://stablecoin-fds-raw/raw/
  ```
* **GCS → BigQuery 로드(파티션)**

  ```bash
  bq load --source_format=PARQUET \
    --time_partitioning_field ts \
    stablecoin_fds.raw_token_transfers \
    gs://stablecoin-fds-raw/raw/chain=eth/token=usdc/*.parquet
  ```

---

## 체크리스트(9/13 기준)

* [ ] **GCS 버킷/BigQuery 데이터셋 생성**
* [ ] **contracts.yaml**(주소/decimals) 고정 & 커밋
* [ ] **ETH/Tron** 과거 전송 로그 추출 → GCS 적재 → BQ 로드
* [ ] **BSC** Cryo 수집 → GCS 적재 → BQ 로드
* [ ] **standardize.py** 실행(정규화·통일 스키마)
* [ ] **label\_merge.py**(CEX/DEX/발행사 라벨) 적용
* [ ] **classify\_context.py**(Direct/DEX, In/Out 태깅)
* [ ] **aggregate\_daily.py**(일별 집계)
* [ ] **notebooks/event\_replay.ipynb**로 USDC(2023-03) 등 사건 리플레이 확인

---

## 리스크 & 대응

* **API/RPC 레이트리밋** → 블록 범위/시간 분할, 재시도 백오프
* **데이터 용량** → Parquet 압축, 파티션 설계, BQ 파티션 필터 사용
* **주소 라벨 정확도** → 핵심 주소(주요 CEX/DEX/발행사)는 수기 검증 로그 유지
* **Tron 편차(전송량 과다)** → 기간 슬라이싱, 일단 USDT 우선

---

필요하면 위 구조로 **초기 스크립트 골격**(실행 가능한 버전)까지 더 자세히 적어줄게—`.env` 포맷, 도커 실행, BQ 쿼리 템플릿 등.
