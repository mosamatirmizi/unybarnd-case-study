# docker/Dockerfile
# Local development image for Airflow + dbt-bigquery + Astronomer Cosmos

# 1) Start from the official Airflow image (version pinned via ARG for easy bumps)
ARG AIRFLOW_VERSION=3.0.6
FROM apache/airflow:${AIRFLOW_VERSION}

# Safer shell in RUN steps
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# 2) Install minimal OS packages for LOCAL DEV only (vim, git).
#    Do this as root, then clean aggressively to keep the image slim.
USER root
RUN set -eux \
  && export DEBIAN_FRONTEND=noninteractive \
  && apt-get update \
  && apt-get install -y --no-install-recommends \
       vim \
       git \
       sudo \
  && echo "root:airflow" | chpasswd \
  && echo "airflow:airflow" | chpasswd \
  && usermod -aG sudo airflow \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 3) Switch back to the 'airflow' user for Python deps (per Airflow docs)
USER airflow

# Optionally avoid pip cache in the layer as well
ENV PIP_NO_CACHE_DIR=1

# 4) Copy requirements first for better layer caching
#    (Place your Python deps in docker/requirements.txt)
COPY requirements.txt /requirements.txt

# 5) Install Python deps for local dev.
#    Best practice: pin apache-airflow to EXACT base version in the same command
#    to prevent the resolver from changing it implicitly.
RUN set -eux \
  && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# 6) (Optional) Create common working directories used by docker-compose volume mounts.
#    Not strictly required, but helpful when running the container without mounts.
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/dbt /opt/airflow/data

# 7) Default working directory (Airflow image already sets this, but explicit is clear)
WORKDIR /opt/airflow
