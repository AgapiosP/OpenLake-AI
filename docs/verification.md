# Verification and release limitations (v0.2.1)

## Source audit

Reviewed repository contents against the documented architecture. Source files exist for: Next.js frontend, FastAPI API, DuckDB, optional PySpark/Iceberg worker, PostgreSQL scheduler, Redis caching, AI provider requests, test files, Dockerfiles, CI workflow, sample dataset, and guides.

Changes made during the review:
- Require nonempty 32+ character Spark worker token at startup and at gateway invocation.
- Align dangerous-function denylist in Spark with the main SQL guard for known external readers.
- Correct frontend and API version metadata to v0.2.1.
- Clarify that scheduling and Iceberg maintenance are exposed through REST API; there is no dedicated frontend management UI yet.

## Validation status

- **Passed:** Python modules compile using `compileall` (syntax only).
- **Passed:** ZIP integrity test and required-file presence check.
- **Not run:** Python pytest: installed Python environment has no project backend dependencies.
- **Not run:** TypeScript compile/build: frontend node_modules is absent. Running `tsc` without dependencies produces missing-package errors, not meaningful product diagnostics.
- **Not run:** Docker Compose / Spark / Iceberg end-to-end: Docker Engine is unavailable in the auditing environment.

**This repository is a developer preview, not a production-certified distribution.** The existence of code does not prove runtime correctness, tenancy isolation, or acceptable performance.

## Known functional limitations

- Spark is **single-node local PySpark**, not a multi-node managed cluster.
- Iceberg is a local Hadoop catalog backed by shared Docker volume, not a managed REST catalog; the primary SQL dataset path still queries Parquet sources rather than Iceberg tables.
- Automatic routing is based on total dataset file bytes, not on query plans, estimated scan size, or benchmark-based optimizer decisions.
- Spark and DuckDB SQL/function semantics may differ. Both require further sandbox hardening before untrusted multi-tenant access.
- DuckDB currently loads all Parquet datasets into Arrow memory before querying. The 512 MB connection limit does not cap that preceding Arrow allocation. Enforce process/container memory limits, timeouts and selective scans before production.
- Redis caching uses a TTL and local file stats; cross-node storage changes and exceptional timestamp collisions need more robust versioning.
- Recurring jobs execute SELECT and record status/row count; they do not materialize destination tables, email reports, or expose a dedicated schedule UI.
- Upload file size is capped, but expanded decompression/row/memory limits require additional controls.
- Schema changes use `metadata.create_all`; Alembic migrations are not provided.
- Browser cookie sessions are implemented, but no scoped external API credentials, comprehensive RBAC, invite flow, password reset, audit trail or rate limiting exist.
- AI providers require a user-supplied supported model name and credential; no AI execution is automatic. Billing/cost controls are not comprehensive.
- No verified performance benchmarks or real-world availability/SLA claims.

## Run verification locally before publication

```bash
cp .env.example .env
# Generate and set POSTGRES_PASSWORD, SECRET_KEY and ENCRYPTION_KEY.
docker compose config
docker compose up --build -d
docker compose ps
docker compose logs --tail=100 api web scheduler
curl -f http://localhost:8000/api/health
```

For optional Spark/Iceberg, set `SPARK_URL=http://spark:8010`, set a strong `SPARK_TOKEN` of at least 32 characters, then:

```bash
docker compose --profile spark up --build -d
docker compose --profile spark ps
docker compose --profile spark logs --tail=100 spark
```

Run unit checks in a properly provisioned environment:

```bash
cd backend && pip install -r requirements-dev.txt && pytest -q && cd ..
cd frontend && npm install && npm run lint && npm run build && cd ..
```

Manual end-to-end scenarios: register and login; upload CSV; query in DuckDB; inspect cache hit; configure supported OpenAI/Claude model and test SQL generation; execute Spark SELECT; sync an Iceberg table and compact it; create a short-interval scheduled query via authenticated API and verify run history; check another organization's access is denied; verify no secrets appear in logs or Git history.
