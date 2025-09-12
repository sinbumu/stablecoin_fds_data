## stablecoin_fds 데이터셋 스키마 개요

본 문서는 BigQuery 데이터셋 `stablecoin_fds` 내 테이블/뷰의 목적과 주요 스키마를 요약합니다. 실제 스키마는 BigQuery에서 `bq show --format=prettyjson`으로 확인하세요.

### 테이블
- `fact_oracle_prices_raw`
  - 목적: 체인링크 AnswerUpdated 원시 로그(프록시 주소 기반) 수집.
  - 주요 컬럼: `feed_address` STRING, `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `tx_hash` STRING, `log_index` INT64, `topic0` STRING, `data` STRING 등.

- `chainlink_proxy_aggregator_timeline`
  - 목적: 프록시→집계기(aggregator) 교체 이력 타임라인.
  - 주요 컬럼: `proxy` STRING, `aggregator` STRING, `from_block` INT64, `to_block_excl` INT64, `from_ts` TIMESTAMP, `to_ts_excl` TIMESTAMP.

- `fact_oracle_prices_raw_all`
  - 목적: 집계기 주소 전체에서 발생한 원시 로그(토픽 제한 없음, 기간 필터) 수집.
  - 주요 컬럼: `feed_address` STRING(=aggregator), `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `tx_hash` STRING, `log_index` INT64, `topic0` STRING, `data` STRING.

- `fact_univ3_swaps_raw`
  - 목적: Uniswap v3 Swap 이벤트 원시 로그.
  - 주요 컬럼: `pool_address` STRING, `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `tx_hash` STRING, `log_index` INT64, `topics` ARRAY<STRING>, `data` STRING.

- `univ3_pool_whitelist`
  - 목적: 분석 대상 풀 화이트리스트 및 메타(토큰, 수수료, 안정성 플래그).
  - 주요 컬럼: `pool` STRING, `token0` STRING, `token1` STRING, `fee_tier` INT64, `is_stable_pool` BOOL.

- `fact_freeze_events`
  - 목적: USDC/USDT 블랙리스트/프리즈 관련 이벤트 원시 로그.
  - 주요 컬럼: `token_contract` STRING, `event_sig` STRING, `subject_address` STRING, `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `tx_hash` STRING, `log_index` INT64.

- `classified_transfers`
  - 목적: 스테이블코인 전송의 분류 결과 테이블(카테고리/방향 라벨 포함).
  - 파티셔닝/클러스터링: DAY(`ts`), 클러스터 `token, from_addr, to_addr`.
  - 주요 컬럼: `ts` TIMESTAMP, `chain` STRING, `tx_hash` STRING, `log_index` INT64, `token` STRING, `token_address` STRING, `from_addr` STRING, `to_addr` STRING, `amount_raw` STRING, `decimals` INT64, `amount_norm` FLOAT64, `block_number` INT64, `from_type` STRING, `to_type` STRING, `category` STRING, `direction` STRING.

- `daily_flows`
  - 목적: 일자·토큰·카테고리·방향별 트랜잭션 수 및 총량 집계.
  - 파티셔닝/클러스터링: DAY(`date`), 클러스터 `token, category, direction`.
  - 주요 컬럼: `date` DATE, `chain` STRING, `token` STRING, `category` STRING, `direction` STRING, `tx_count` INT64, `total_amount` FLOAT64.

- `dim_blocks`
  - 목적: 블록 넘버와 표준화된 시간/날짜 차원 맵핑.
  - 주요 컬럼: `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `block_date` DATE, `block_hour` INT64.

- `eth_stable_transfers_2023p`
  - 목적: 이더리움 네트워크의 스테이블코인 Transfer 원시(2023+ 파티션 소스).
  - 주요 컬럼: `ts` TIMESTAMP, `tx_hash` STRING, `log_index` INT64, `token_address` STRING, `from_addr` STRING, `to_addr` STRING, `amount_raw` STRING, `block_number` INT64.

### 뷰
- `view_univ3_swaps_norm`
  - 목적: v3 Swap 디코딩/정규화. amount 부호, sqrtPriceX96, liquidity, tick 디코딩 및 `price_token1_per_token0` 계산 제공.
  - 주요 컬럼: `pool` STRING, `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `tx_hash` STRING, `log_index` INT64,
    `amount0` BIGNUMERIC, `amount1` BIGNUMERIC, `sqrtPriceX96` BIGNUMERIC, `liquidity` BIGNUMERIC, `tick` BIGNUMERIC,
    `price_token1_per_token0` BIGNUMERIC.

- `view_univ3_swaps_norm_stable`
  - 목적: 화이트리스트된 안정적(stable) 풀로 필터링된 노멀라이즈 스왑 뷰.
  - 조인: `univ3_pool_whitelist`의 `is_stable_pool = TRUE`.

- `view_freeze_events_enriched`
  - 목적: Freeze/Blacklist 이벤트에 on/off 라벨 부여.
  - 주요 컬럼: `token` STRING, `subject` STRING, `block_number` INT64, `block_timestamp_utc` TIMESTAMP, `action` STRING('blacklist_on'|'blacklist_off'|'other').

- `view_freeze_timeline`
  - 목적: 주소별 블랙리스트 상태의 타임라인(유효 블록/시간 범위).
  - 주요 컬럼: `token` STRING, `subject` STRING, `from_block` INT64, `to_block_excl` INT64, `from_ts` TIMESTAMP, `to_ts_excl` TIMESTAMP, `is_blacklisted` BOOL.

- `view_freeze_snapshot_latest`
  - 목적: 토큰-주소별 최신 블랙리스트 상태 스냅샷.
  - 주요 컬럼: `token` STRING, `subject` STRING, `is_blacklisted` BOOL, `last_update_block` INT64.

### UDF (요약)
- `udf_hex_to_bignum(hex STRING) → BIGNUMERIC`: 0x-hex를 정수 문자열로 변환.
- `udf_hex_to_bytes(hex STRING) → BYTES`: 0x-hex를 BYTES로 변환.
- `udf_address_from_topic(topic STRING) → STRING`: topic의 마지막 20바이트를 주소로.
- `udf_uint256_slot(data BYTES, slot INT64) → BIGNUMERIC`: data에서 uint256 슬롯 추출.
- `udf_int256_slot(data BYTES, slot INT64) → BIGNUMERIC`: data에서 int256(2의 보수) 슬롯 추출.
- `udf_univ3_price_ratio(sqrtPriceX96 BIGNUMERIC) → BIGNUMERIC`: 가격비(토큰1/토큰0).

참고: 일부 레코드의 `data` 길이가 32바이트 미만일 수 있어 디코딩 결과는 NULL이 될 수 있습니다. Downstream 계산 시 NULL-safe 연산을 권장합니다.


