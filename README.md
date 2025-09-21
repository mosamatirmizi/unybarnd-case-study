# unybarnd-case-study

Case study project for running Apache Airflow with a Postgres backend and custom DAGs.

## Getting Started

- Clone the repository and ensure Docker Desktop (or an equivalent Docker Engine) is running.
- Verify the `airflow/` directory contains your DAGs, plugins, dbt assets, and logs – these folders are mounted into the containers.
- Place the Google Cloud service account key at `secrets/gcp-sa.json` (already referenced by the compose file) and restrict its permissions appropriately.

## Docker Environment

The main entry point for the local Airflow stack is `docker/docker-compose.yml`. The compose file builds a custom Airflow image and wires the services together with shared volumes and secrets.

- `postgres`: Provides the metadata database for Airflow (`postgres:16`). Data is persisted in the named volume `postgres-data` and ports are exposed on `5432` for optional local access.
- `airflow-init`: One-shot container that runs `airflow db migrate` to prepare the metadata database. It is built from `docker/Dockerfile` with the Airflow version argument (`3.0.6`). Other services wait for this step to succeed.
- `airflow-webserver`: Runs the Airflow API/web UI on port `8080`. Health checks probe `/health` to restart the container if it becomes unhealthy.
- `airflow-scheduler`: Executes DAG scheduling loops. Shares the same image, volumes, and secrets as the webserver.

Common configuration for the Airflow services is centralized in two YAML anchors:

- `x-airflow-env`: Defines environment variables (database connection string, executor, concurrency limits, user credentials, and cryptographic keys).
- `x-airflow-vols`: Mounts host directories (`airflow/dags`, `airflow/logs`, `airflow/plugins`, `airflow/dbt`) into `/opt/airflow` so edits on the host are immediately visible inside the containers.

Secrets are handled with Docker secrets. The `gcp_sa` secret surfaces `/run/secrets/gcp_sa` inside Airflow containers, matching `GOOGLE_APPLICATION_CREDENTIALS`.

### Required Environment Variables

Before starting the stack, supply the cryptographic secrets referenced in `x-airflow-env`:

```bash
export AIRFLOW__CORE__FERNET_KEY="$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')"
export AIRFLOW__API__SECRET_KEY="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')"
```

You can place these exports in `docker/.env` (Docker Compose automatically loads it) or set them in your shell prior to running `docker compose`.

### Bringing Up the Stack

```bash
cd docker
# First run only: initialize the metadata database
docker compose run --rm airflow-init

# Start the webserver, scheduler, and postgres together
docker compose up -d postgres airflow-webserver airflow-scheduler

# Tail logs if needed
docker compose logs -f airflow-webserver airflow-scheduler
```

Stop the services with `docker compose down`. Add `-v` if you want to remove the `postgres-data` volume.

Log in to the Airflow web UI at `http://localhost:8080` using the `admin:admin` credentials defined in `x-airflow-env`. Update these defaults for production use.

## Troubleshooting

- Ensure the `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__API__SECRET_KEY` variables are present; missing values will prevent the containers from starting.
- If DAG changes do not appear, confirm they exist under `airflow/dags` and the scheduler container is healthy.
- Use `docker compose ps` to inspect container health checks and restart failed services.
