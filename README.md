# Stablecoin FDS 데이터 수집/전처리

이 저장소는 USDT/USDC/DAI를 대상으로 ETH/BSC/Tron 네트워크의 전송 로그를 수집·전처리하여 GCS/BigQuery로 적재하기 위한 스크립트/구성을 포함합니다.

- GCS: gs://stablecoin-fds-raw, gs://stablecoin-fds-processed
- BigQuery: dataset stablecoin_fds

자세한 계획은 PLAN.md 참고.
