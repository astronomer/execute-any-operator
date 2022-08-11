ARG AIRFLOW_VERSION="2.2.0"
FROM quay.io/astronomer/ap-airflow:${AIRFLOW_VERSION}

COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi

# Install python packages
COPY . /execute_any_operator
WORKDIR /execute_any_operator
RUN pip install --no-cache-dir -q -r requirements.txt

USER astro

ENV AIRFLOW__CORE__XCOM_BACKEND=execute_any_operator.utils.dict_xcom_backend.DictXComBackend

ENTRYPOINT [ "execute-any-operator" ]
