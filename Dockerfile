ARG AIRFLOW_VERSION="2.2.4"
FROM quay.io/astronomer/ap-airflow:${AIRFLOW_VERSION}

COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi

# Install python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -q -r requirements.txt

USER astro

COPY util ./util
COPY scripts ./scripts

ENV AIRFLOW_CONN_AWS_DEFAULT=s3://
