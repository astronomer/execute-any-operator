ARG BASE_IMAGE=quay.io/astronomer/ap-airflow
ARG IMAGE_TAG="2.3.2"
FROM ${BASE_IMAGE}:${IMAGE_TAG}

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


ENV AIRFLOW__CORE__XCOM_BACKEND=execute_any_operator.utils.dict_xcom_backend.DictXComBackend

RUN mkdir -p /execute_any_operator/secrets
RUN touch /execute_any_operator/secrets/variables.yaml
RUN touch /execute_any_operator/secrets/connections.yaml

ENV AIRFLOW__SECRETS__BACKEND=execute_any_operator.utils.secrets_backend.LocalSecretsBackend
ENV AIRFLOW__SECRETS__BACKEND_KWARGS='{"variables_file_path": "/execute_any_operator/secrets/variables.yaml", "connections_file_path": "/execute_any_operator/secrets/connections.yaml"}'

ENTRYPOINT [ "execute-any-operator" ]
