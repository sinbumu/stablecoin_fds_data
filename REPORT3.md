# REPORT3 — PLAN2 원시 인제스트 진행 보고 (Chainlink / Uniswap v3 / Freeze)

## 1) 실행 요약
- UDF/블록 헬퍼 등록: 완료(`processing/build_udfs.sql`, `processing/build_block_helpers.sql`)
- 체인링크 AnswerUpdated 원시: 완료(`stablecoin_fds.fact_oracle_prices_raw`) — REPLACED 로그 확인
- Uniswap v3 Swap 원시: 완료(`stablecoin_fds.fact_univ3_swaps_raw`) — 표본 결과 출력 확인
- USDC/USDT 동결/블랙리스트 원시: 완료(`stablecoin_fds.fact_freeze_events`) — 표본 결과 출력 확인

- 체인링크 프록시→집계기 타임라인: 완료(`stablecoin_fds.chainlink_proxy_aggregator_timeline`) — rows: 8
- 체인링크 원시 로그(집계기·다중 topic): 완료(`stablecoin_fds.fact_oracle_prices_raw_all`) — total: 36,769 rows (전체), 5,355 rows (2023-01-01~2025-12-31)
- Uniswap v3 Swap 현황(2023-01-01~2025-12-31): 6,321,550 rows
- Freeze/블랙리스트 현황(2023-01-01~2025-12-31): 2,658 rows

- Uniswap v3 노멀라이즈 뷰: 생성(`stablecoin_fds.view_univ3_swaps_norm`) — amount0/1 부호, sqrtPriceX96, liquidity, tick 디코드 및 price_token1_per_token0 포함
- Uniswap v3 화이트리스트 테이블/뷰: 생성(`stablecoin_fds.univ3_pool_whitelist`, `stablecoin_fds.view_univ3_swaps_norm_stable`)
- Freeze 타임라인/스냅샷: 생성(`stablecoin_fds.view_freeze_timeline`, `stablecoin_fds.view_freeze_snapshot_latest`)
- 체인링크 디코딩: 완료(`stablecoin_fds.fact_oracle_prices`) — rows: 1,984 (min_ts=2020-02-22, max_ts=2025-09-08)
- Uniswap v3 USD 뷰: 생성(`stablecoin_fds.view_univ3_swaps_usd`) — 가까운 시점 체인링크 가격 조인 방식 적용

## 2) 주요 이슈/해결
- ARRAY 파라미터 주입(zsh) 문제 → `--parameter='타입:'"$VAR"` 패턴으로 해결(JSON 배열은 변수로)
- logs.topics 인덱스 에러 → `SAFE_OFFSET()` 적용, subject 없는 이벤트는 NULL 처리
- 주소 대소문자 혼재 → `LOWER(address)`로 통일하여 비교

- 체인링크 AggregatorUpdated 토픽 오인식 → 실제 topic0 `0xed8889f560326eb138920d842192f0eb3dd22b4f139c87a2c57538e05bae1278` 확인 후 타임라인 재생성(8 rows)
- 체인링크 수집 범위 확장 → 프록시가 아닌 집계기 주소 기준으로 기간 필터만 적용해 원시 로그 확보(`fact_oracle_prices_raw_all`)
 - Uniswap v3 BYTES 디코딩 이슈 → 일부 레코드에서 `data`가 빈/짧은 바이트로 UDF BigInt 변환 오류 발생. JS UDF에 빈 바이트 가드(길이<32 → NULL) 적용 완료. 추가적으로 노멀라이즈 뷰 사용 시 NULL 허용.

## 3) 빠른 검증 커맨드
```bash
# 날짜 범위 환경변수
DATE_START=2023-01-01; DATE_END=2025-12-31

# 체인링크 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_oracle_prices_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# 체인링크(all, 집계기·다중 topic) 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n, MIN(block_timestamp_utc) min_ts, MAX(block_timestamp_utc) max_ts \
   FROM \`$BQ_DATASET.fact_oracle_prices_raw_all\` \
   WHERE DATE(block_timestamp_utc) BETWEEN DATE '$DATE_START' AND DATE '$DATE_END'"

# 체인링크 타임라인(프록시→집계기) 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.chainlink_proxy_aggregator_timeline\`"

# Uniswap v3 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_univ3_swaps_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# Uniswap v3 노멀라이즈 샘플
bq query --use_legacy_sql=false \
  "SELECT pool, block_timestamp_utc, amount0, amount1, sqrtPriceX96, price_token1_per_token0 \
     FROM \`$BQ_DATASET.view_univ3_swaps_norm\` ORDER BY block_timestamp_utc DESC LIMIT 5"

# Freeze 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_freeze_events\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# Freeze 최신 스냅샷 상위 5
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$BQ_DATASET.view_freeze_snapshot_latest\` LIMIT 5"
```

## 3-1) 비용 가드(드라이런/바이트 상한) 사용법
대형 쿼리 실행 전 반드시 드라이런으로 스캔 바이트를 추정하고, 상한을 걸어 실행합니다.

```bash
# 환경 변수(예시): 최대 과금 바이트 2GB
export BQ_MAX_BYTES_BILLED=$((2*1024*1024*1024))

# 1) 파이썬 러너에서 드라이런/상한 적용
python3 processing/plan2_inject_and_run.py \
  --date-start 2023-01-01 --date-end 2023-01-31 \
  --dry-run-first --max-bytes-billed $BQ_MAX_BYTES_BILLED \
  --skip-view

# 2) 수동 실행 시 bq_guard.sh 사용
bash bin/bq_guard.sh --sql processing/build_univ3_usd_view.sql \
  --project "$GCP_PROJECT" --max-bytes "$BQ_MAX_BYTES_BILLED" --dry-run-only

# 드라이런 결과가 안전하면 실제 실행
bash bin/bq_guard.sh --sql processing/build_univ3_usd_view.sql \
  --project "$GCP_PROJECT" --max-bytes "$BQ_MAX_BYTES_BILLED"
```

주요 규칙
- require_partition_filter가 설정된 테이블은 반드시 파티션 컬럼으로 범위 제한을 둡니다.
- 스캔 바이트가 상한을 넘으면 실행하지 않고 범위/필터/클러스터 키를 재검토합니다.

## 4) 다음 단계
- 체인링크 디코딩(`processing/build_decoded_prs.sql`) 보강: feed decimals 조인 후 `price_scaled` 계산
- `processing/build_prs_crs_view.sql` 확장: PRS 간이 지표(oracle_deviation_bps 등) 채우기
- 커버리지/품질 점검: 일자별 rowcount/NULL 비율 리포트
 - Uniswap v3 USD 환산 뷰: 토큰 메타/가격 조인으로 USD 값 컬럼 추가
 - Freeze 라벨 확장: 토큰별 정책(USDC Paused/Blacklist, USDT Freeze) 세분화

---

비고: 상세 실행/환경/토픽 값은 `README.md` PLAN2 섹션(10.3~10.6) 참고.
