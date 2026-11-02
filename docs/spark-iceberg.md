# Optional Spark, Iceberg, routing, caching, scheduling

## Two deployment modes

Default mode (`docker compose up --build -d`) uses DuckDB, Redis and a PostgreSQL-backed scheduler. Optional Spark mode requires setting `SPARK_TOKEN` (random secret) and `SPARK_URL=http://spark:8010` in `.env`, then running `docker compose --profile spark up --build -d`.

The Spark worker is private to the Compose network. **Never expose port 8010 publicly.** First Spark build fetches the pinned Iceberg runtime JAR from Maven Central and needs network access. Allocate at least 4 GB free memory for this demonstration deployment.

## Query API

`POST /api/query` JSON: `{"sql":"SELECT * FROM sales", "engine":"auto", "use_cache":true}`. The legacy `{"sql": "..."}` request remains supported. Response adds `engine` and `cached` metadata.

Routing checks the sum of compressed local Parquet file sizes across a tenant. Spark is chosen only if configured and file size exceeds `SPARK_MIN_BYTES` (default 256 MiB). This is a **simple threshold heuristic**, not a cost-based optimizer or Spark Adaptive Query Execution replacement. Spark itself has its own optimizer.

## Iceberg maintenance

For a dataset UUID, `POST /api/datasets/{id}/iceberg/sync` creates or replaces an Iceberg table at `lake.t_<tenant_uuid_underscored>.<dataset_name>` from its uploaded Parquet source. Then `POST /api/datasets/{id}/iceberg/compact` invokes `CALL lake.system.rewrite_data_files(...)` for binpack optimization. You can enable automatic daily compaction with `ICEBERG_AUTO_MAINTENANCE=true`. The scheduler tracks source fingerprints and the last maintenance time in PostgreSQL, syncs changed datasets, and invokes Iceberg binpack on the configured interval (`ICEBERG_MAINTENANCE_INTERVAL_MINUTES`, default 1440). This is a time-based policy, not an advanced file-size-aware optimizer. Disabled by default to avoid expensive Spark runs. Iceberg tables are stored on the shared Docker volume under `/app/storage/iceberg`. Dataset querying uses registered Parquet source views, not Iceberg snapshots, in this version.

## Caching

Redis caches bounded result JSON for 300 seconds, keyed by tenant, query, selected engine and dataset path/modification fingerprint. Disabled if `REDIS_URL` is unset; eviction is LRU and the cache is disposable. External modification of data while preserving file metadata can yield stale entries until TTL expires.

## Scheduling

Authenticated `POST /api/schedules` accepts `{ "name":"Sales summary", "sql":"SELECT ...", "engine":"auto", "interval_minutes":60 }`. List with `GET /api/schedules`, retrieve run history with `GET /api/schedules/{id}/runs`, and delete with `DELETE /api/schedules/{id}`. The scheduler polls every 15 seconds, takes a PostgreSQL advisory lock, and records status. It runs bounded SELECT queries only and does **not** export results or schedule Iceberg compaction. Schedule history survives container restarts. Heavy load requires separate job queues and stronger execution limits.

## Production boundaries

Spark mode is a **single-node local Spark worker**, not a scalable Spark cluster, and Iceberg uses a shared-filesystem Hadoop catalog, not an S3/REST catalog. Adding Spark changes resource usage significantly. Tenant sharing is enforced at the API/worker path validation layer, not at operating-system user isolation. There are no resource quotas per user, cancellation, retry backoff, migrations, monitoring or full isolation of untrusted queries. Do not deploy untrusted multi-tenant workloads without hardening and independent penetration testing.
