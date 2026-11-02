# OpenLake AI

**A self-hosted analytics workspace for small teams.** Organize CSV/Parquet data, run read-only SQL with DuckDB, save queries, and turn natural-language questions into reviewable SQL with your own OpenAI or Anthropic Claude key.

> **Release status: v0.2.1 — developer preview.** Suitable for local evaluation and development. Production deployments require security review, end-to-end integration testing, backup validation and capacity testing. See [docs/architecture.md](docs/architecture.md).

## Hybrid analytics

- **DuckDB + optional Spark 3.5.6** (single-node compute, internal HTTP worker)
- **Apache Iceberg 1.9.2** Hadoop catalog with API-triggered or opt-in scheduled sync and binpack compaction
- **Auto engine routing**: DuckDB for small storage footprints; Spark when configured and threshold exceeded
- **Redis SQL results cache** keyed by tenant, dataset metadata and query
- **PostgreSQL-backed scheduling**, authenticated CRUD and execution history
- **Optional AI integration:** bring your own OpenAI or Anthropic API key

Spark and Iceberg must be enabled explicitly; set `SPARK_TOKEN` to a random secret of at least 32 characters. Scheduled jobs, Iceberg maintenance, and cache controls currently expose APIs rather than full UI pages.

See [Spark/Iceberg guide](docs/spark-iceberg.md) for API usage, optional Docker profile, limitations, and production hardening.

## What you get

- Responsive dashboard and workspace UI (Next.js, TypeScript, custom design system)
- Organization-isolated dataset uploads (CSV / Parquet up to 20 MB)
- DuckDB query engine over Parquet with SQL validation and 500-row output limit
- SQL editor, results table, CSV export and saved queries
- OpenAI and Anthropic Claude BYOK AI-to-SQL integration
- Encrypted AI keys, secure session cookie, origin checks and CSRF header
- PostgreSQL metadata and persistent Docker volumes
- OpenAPI documentation at `/docs`, pytest safety tests and GitHub Actions CI

## Quick start (Docker)

Requirements: Docker Engine + Compose v2.

```bash
cp .env.example .env
python -c "import secrets; print(secrets.token_urlsafe(48))" # paste as SECRET_KEY
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" # paste as ENCRYPTION_KEY
# Replace POSTGRES_PASSWORD with a strong random URL-safe password.
docker compose up --build -d
```

Visit **http://localhost:3000**, create a workspace, then upload `examples/sample_sales.csv` with the name `sample_sales`. In SQL Workspace run:

```sql
SELECT category, SUM(revenue) AS revenue FROM sample_sales GROUP BY category ORDER BY revenue DESC;
```

To enable AI: Settings -> choose OpenAI or Anthropic -> enter a supported model identifier and API key -> save. Navigate to AI Copilot and generate SQL for a question, review it in the editor, and click Run.

For a fresh installation, the API is at `http://localhost:8000/docs`.

## Local development

API:
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
# Configure DATABASE_URL, SECRET_KEY, ENCRYPTION_KEY and DATA_DIR through environment variables.
uvicorn app.main:app --reload
```

Frontend:
```bash
cd frontend
npm install
npm run dev
```

## Deployment

The Compose example binds both application ports to `127.0.0.1` deliberately. Put a TLS-terminating reverse proxy in front of them and configure your domain values:

- `FRONTEND_ORIGIN=https://app.example.com`
- `NEXT_PUBLIC_API_URL=https://api.example.com`
- `COOKIE_SECURE=true`
- Choose hosts sharing the same registrable domain for strict session cookies; verify your proxy forwards original host/protocol correctly.
- `NEXT_PUBLIC_API_URL` is compiled into the frontend at **build time**. Rebuild the web image after changing it.
- If your database password contains reserved URL characters, URL-encode it in DATABASE_URL or use a URL-safe generated password.
- Secure backups of PostgreSQL and the dataset volume; store `.env` in a secret manager and do not commit it.

Detailed security notes: [docs/architecture.md](docs/architecture.md).

## Project structure

```text
frontend/      Next.js 15 app, responsive UI
backend/app/   FastAPI, security, AI gateway, datasets, SQL engine
backend/tests/ SQL safety tests
examples/      Sample business dataset and SQL
.github/       Continuous integration
```

## API

The interactive OpenAPI spec at `/docs` covers `/api/auth/*`, `/api/datasets`, `/api/query`, `/api/queries`, `/api/ai/config` and `/api/ai/ask`. Cookie authentication and the `X-Requested-With: openlake-web` header are used for mutations. This interface is currently browser-first; for external programmatic integrations, add revocable scoped personal-access tokens rather than reusing browser sessions.

## Current scope and limitations

The application provides dataset management, SQL analysis, saved queries, optional AI-assisted SQL, Redis query caching, persistent job scheduling, and an optional single-node Spark/Iceberg deployment profile. Automatic engine routing uses a configurable dataset-size threshold; it is not a cost-based optimizer. Iceberg maintenance is opt-in and uses supported rewrite operations.

This release has not been verified under a production load. Do not expose it to untrusted tenants or confidential data without a deployment-specific threat model and a review of authentication, SQL isolation, access controls, networking, secrets, backup/recovery, and dependency vulnerabilities.

## Audit and verification

See [docs/verification.md](docs/verification.md) for source audit, known limitations and executable validation commands. Do not claim production readiness without running the Docker/Spark integration checks.

## Roadmap

- Role-based access control and invitations
- Scoped API tokens for external clients
- Source connectors and ingestion workflows
- Query resource quotas and stronger workload isolation
- Schema migrations, observability, audit logs, and load testing
- Multi-node Spark orchestration and richer cost-aware query planning

## License

Apache License 2.0. See [LICENSE](LICENSE). Contributions are welcome.
