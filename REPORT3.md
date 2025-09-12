# REPORT3 — PLAN2 원시 인제스트 진행 보고 (Chainlink / Uniswap v3 / Freeze)

## 1) 실행 요약
- UDF/블록 헬퍼 등록: 완료(`processing/build_udfs.sql`, `processing/build_block_helpers.sql`)
- 체인링크 AnswerUpdated 원시: 완료(`stablecoin_fds.fact_oracle_prices_raw`) — REPLACED 로그 확인
- Uniswap v3 Swap 원시: 완료(`stablecoin_fds.fact_univ3_swaps_raw`) — 표본 결과 출력 확인
- USDC/USDT 동결/블랙리스트 원시: 완료(`stablecoin_fds.fact_freeze_events`) — 표본 결과 출력 확인

## 2) 주요 이슈/해결
- ARRAY 파라미터 주입(zsh) 문제 → `--parameter='타입:'"$VAR"` 패턴으로 해결(JSON 배열은 변수로)
- logs.topics 인덱스 에러 → `SAFE_OFFSET()` 적용, subject 없는 이벤트는 NULL 처리
- 주소 대소문자 혼재 → `LOWER(address)`로 통일하여 비교

## 3) 빠른 검증 커맨드
```bash
# 날짜 범위 환경변수
DATE_START=2023-01-01; DATE_END=2025-12-31

# 체인링크 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_oracle_prices_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# Uniswap v3 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_univ3_swaps_raw\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"

# Freeze 카운트
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) n FROM \`$BQ_DATASET.fact_freeze_events\` \
   WHERE block_timestamp_utc BETWEEN TIMESTAMP('$DATE_START') AND TIMESTAMP('$DATE_END')"
```

## 4) 다음 단계
- 체인링크 디코딩(`processing/build_decoded_prs.sql`) 보강: feed decimals 조인 후 `price_scaled` 계산
- `processing/build_prs_crs_view.sql` 확장: PRS 간이 지표(oracle_deviation_bps 등) 채우기
- 커버리지/품질 점검: 일자별 rowcount/NULL 비율 리포트

---

비고: 상세 실행/환경/토픽 값은 `README.md` PLAN2 섹션(10.3~10.6) 참고.
