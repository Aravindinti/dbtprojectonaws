
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11
ARG DBT_CORE_VERSION=1.12.6
ARG DBT_GLUE_VERSION=1.12.4
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "astronomer-cosmos==1.10.2"

RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==${DBT_CORE_VERSION}" \
        "dbt-glue==${DBT_GLUE_VERSION}"

ENV PATH="/home/airflow/dbt-venv/bin:${PATH}"

RUN /usr/local/bin/python -c "from cosmos import DbtTaskGroup; print('Cosmos DbtTaskGroup is available')" \
    && dbt --version
