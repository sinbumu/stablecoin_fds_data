FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates git && rm -rf /var/lib/apt/lists/*

# gsutil, bq (gcloud CLI) 설치
RUN curl -sSL https://sdk.cloud.google.com > /tmp/gcloud.sh \
 && bash /tmp/gcloud.sh --disable-prompts --install-dir=/opt \
 && /opt/google-cloud-sdk/bin/gcloud --version

ENV PATH="/opt/google-cloud-sdk/bin:${PATH}"

# python deps
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

CMD ["bash"]
