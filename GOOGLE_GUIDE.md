# Google Cloud 설정 가이드 (Ethereum Only 수집용)

본 문서는 본 저장소를 구동하기 위해 필요한 GCP 설정(프로젝트/버킷/데이터셋/권한)과 인증 방법(로컬/도커/CI)을 정리합니다.

## 1. 요구 사항 요약
- GCP 프로젝트 ID (예: `stablecoin-fds`)
- GCS 버킷 2개
  - 원시: `stablecoin-fds-raw`
  - 전처리: `stablecoin-fds-processed`
- BigQuery 데이터셋: `stablecoin_fds`
- API 활성화: BigQuery API, Cloud Storage API
- 권한(권장 롤)
  - BigQuery: `roles/bigquery.user`, `roles/bigquery.jobUser`, (데이터셋에) `roles/bigquery.dataEditor`
  - GCS: (버킷에) `roles/storage.objectAdmin` 또는 최소 `roles/storage.objectCreator`
- 리전 일치: BigQuery 데이터셋과 GCS 버킷은 같은 location 사용 권장(예: `US`)

## 2. .env 변수 설정
`.env.example`을 복사하여 `.env` 생성 후 값 입력:

```
GCP_PROJECT=stablecoin-fds
GCS_BUCKET_RAW=stablecoin-fds-raw
GCS_BUCKET_PROCESSED=stablecoin-fds-processed
BQ_DATASET=stablecoin_fds
```

- 스크립트에서는 `gs://$GCS_BUCKET_RAW`, `gs://$GCS_BUCKET_PROCESSED` 형태로 사용합니다.

## 3. 리소스 생성 (예시 명령)
- 버킷(리전: US 예시)
```bash
gsutil mb -l US gs://stablecoin-fds-raw
gsutil mb -l US gs://stablecoin-fds-processed
```
- BigQuery 데이터셋
```bash
bq --location=US mk -d stablecoin_fds
```
- API 활성화(최초 1회)
```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com --project=$GCP_PROJECT
```

## 4. 인증(자격증명) 방법
- 결론: 로컬 개발/도커(개인 머신)에서는 별도의 키 파일 없이 `gcloud` 사용자 인증으로 충분. CI/서버에서는 Workload Identity Federation 권장.

### 4.1 로컬(개인 머신)
- 사용자 ADC(응용 프로그램 기본자격증명) 설정:
```bash
gcloud auth login
# BigQuery/Storage 클라이언트용 ADC
gcloud auth application-default login
```
- 이후 `bq`, `gsutil`, `gcloud` 및 파이썬/도커 내부에서도 ADC가 인식됩니다.

### 4.2 도커
- docker-compose에서 `~/.config/gcloud`를 컨테이너에 마운트하여 사용자 인증을 재사용합니다.
- 별도 키 파일 불필요. (대신 팀/CI 환경이라면 Workload Identity 권장)

### 4.3 CI/CD(권장: 키리스)
- GitHub Actions → Google Cloud Workload Identity Federation 구성
  - OIDC 기반으로 서비스 계정에 임시 크레덴셜 발급(키 파일 불필요)
  - 최소 롤만 부여: 위 “권한(권장 롤)” 참조
- 불가피할 경우에만 SA Key JSON 사용(보안 위험). 저장은 CI Secret에.

## 5. 권한(예시)
- 서비스 계정에 다음 롤 부여(프로젝트/버킷/데이터셋 단위 최소권한 원칙):
  - `roles/bigquery.user`
  - `roles/bigquery.jobUser`
  - (데이터셋 `stablecoin_fds`에) `roles/bigquery.dataEditor`
  - (버킷 `stablecoin-fds-raw`, `stablecoin-fds-processed`에) `roles/storage.objectAdmin`

## 6. 실행 체크리스트
1) `.env` 완료(프로젝트/버킷/데이터셋)
2) `gcloud auth application-default login` 완료(로컬/도커)
3) API 활성화 및 권한 세팅
4) 테스트: 단일 쿼리/내보내기
```bash
# 쿼리 → 임시 테이블 생성
echo "SELECT TIMESTAMP('2023-01-01') ts" | \
  bq --project_id=$GCP_PROJECT query --use_legacy_sql=false --destination_table=${BQ_DATASET}_temp.test_ts --replace

# 임시 테이블 → GCS Parquet 내보내기
bq --project_id=$GCP_PROJECT extract --destination_format=PARQUET \
  ${BQ_DATASET}_temp.test_ts gs://$GCS_BUCKET_RAW/tmp/test_ts-*.parquet
```

## 7. 자주 묻는 질문(FAQ)
- Q: “별도 credential key(키 파일)가 꼭 필요합니까?”
  - A: 로컬/도커에서 사용자 인증(`gcloud auth application-default login`)이면 키 파일 없이도 충분합니다. CI/서버는 Workload Identity Federation을 권장합니다. 키 파일은 불가피할 때만.
- Q: “BigQuery → GCS 추출이 실패합니다(Region mismatch).”
  - A: 데이터셋과 버킷 리전을 동일하게 맞추세요(예: 둘 다 US).
- Q: “권한 부족 에러.”
  - A: 위 롤을 점검하고, 특히 버킷에 대한 `storage.object*` 권한과 데이터셋에 대한 `bigquery.dataEditor`를 확인하세요.

---
이 문서대로 설정하면, 본 저장소의 `ingest/bigquery/export_to_gcs.sh`를 바로 실행할 수 있습니다.
