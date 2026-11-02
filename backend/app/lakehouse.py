"""Tenant-aware engine selection, Redis result cache and Spark gateway."""
import hashlib
import json
from pathlib import Path
import httpx
from fastapi import HTTPException
from sqlalchemy import select
from .analytics import query_data, safe_sql
from .config import get_settings
from .models import Dataset


def route_engine(datasets, engine='auto', spark_enabled=False, threshold=268435456):
    if engine not in ('auto', 'duckdb', 'spark'):
        raise HTTPException(422, 'Unknown execution engine')
    if engine == 'spark' and not spark_enabled:
        raise HTTPException(503, 'Spark is not configured; start the optional spark profile')
    if engine != 'auto':
        return engine
    size = sum(Path(d.storage_path).stat().st_size for d in datasets if Path(d.storage_path).is_file())
    return 'spark' if spark_enabled and size >= threshold else 'duckdb'


def cache_key(org_id, sql, engine, datasets):
    fingerprints = sorted((d.id, d.storage_path, Path(d.storage_path).stat().st_mtime_ns,
                           Path(d.storage_path).stat().st_size) for d in datasets)
    packed = json.dumps([org_id, sql, engine, fingerprints], separators=(',', ':'))
    return 'openlake:query:' + hashlib.sha256(packed.encode()).hexdigest()


def redis_client():
    settings = get_settings()
    if not settings.redis_url:
        return None
    import redis
    return redis.Redis.from_url(settings.redis_url, socket_connect_timeout=1, socket_timeout=1)


def run_lakehouse_query(db, org_id, sql, engine='auto', use_cache=True):
    settings = get_settings()
    datasets = db.scalars(select(Dataset).where(Dataset.organization_id == org_id)).all()
    # Validate against ONLY datasets owned by this tenant before selecting a compute engine.
    safe_sql(sql, {d.name for d in datasets})
    chosen = route_engine(datasets, engine, bool(settings.spark_url), settings.spark_min_bytes)
    cache = None
    key = None
    if use_cache:
        try:
            cache = redis_client()
            if cache is not None:
                key = cache_key(org_id, sql, chosen, datasets)
                hit = cache.get(key)
                if hit:
                    return {**json.loads(hit), 'engine': chosen, 'cached': True}
        except Exception:
            cache = None
    if chosen == 'duckdb':
        result = query_data(db, org_id, sql)
    else:
        if len(settings.spark_token) < 32:
            raise HTTPException(503, 'SPARK_TOKEN is required for Spark requests')
        try:
            # Worker receives ONLY server-controlled paths and normalized identifiers.
            response = httpx.post(settings.spark_url.rstrip('/') + '/execute',
                headers={'X-Worker-Token': settings.spark_token},
                json={'sql': sql, 'datasets': [{'name': d.name, 'path': d.storage_path} for d in datasets],
                      'max_rows': settings.max_query_rows, 'org_id': org_id}, timeout=180)
            response.raise_for_status()
            result = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            raise HTTPException(502, f'Spark execution unavailable: {type(exc).__name__}') from None
    result = {**result, 'engine': chosen, 'cached': False}
    if cache is not None and key:
        try:
            cache.setex(key, settings.cache_ttl_seconds, json.dumps(result, default=str))
        except Exception:
            pass  # Cache failure never blocks analytics.
    return result


def spark_maintenance(org_id, dataset, action='sync'):
    settings = get_settings()
    if not settings.spark_url or len(settings.spark_token) < 32:
        raise HTTPException(503, 'Optional Spark service is not running')
    if action not in {'sync', 'compact'}:
        raise HTTPException(422, 'Unknown maintenance operation')
    try:
        response = httpx.post(settings.spark_url.rstrip('/') + '/maintenance',
            headers={'X-Worker-Token': settings.spark_token},
            json={'org_id':org_id, 'dataset':dataset.name, 'path':dataset.storage_path, 'action':action},
            timeout=300)
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(502, f'Iceberg maintenance failed: {type(exc).__name__}') from None
