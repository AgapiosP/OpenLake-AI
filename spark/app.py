"""Optional single-node Spark + Iceberg worker (not an internet-facing service)."""
import os
import re
from pathlib import Path
from threading import Lock
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from pyspark.sql import SparkSession

TOKEN = os.environ.get('SPARK_TOKEN', '')
if len(TOKEN) < 32:
    raise RuntimeError('SPARK_TOKEN must be set to at least 32 characters')
ROOT = Path(os.environ.get('DATA_DIR', '/app/storage')).resolve()
NAME = re.compile(r'^[a-z][a-z0-9_]{0,89}$')
ORG = re.compile(r'^[0-9a-f-]{36}$')
lock = Lock()  # Spark session temp views are global; serialize requests to prevent cross-tenant leakage.
app = FastAPI(title='OpenLake internal Spark worker', docs_url=None, redoc_url=None)
spark = (SparkSession.builder.appName('OpenLake Spark')
    .master(os.environ.get('SPARK_MASTER', 'local[2]'))
    .config('spark.driver.memory', '1g')
    .config('spark.sql.shuffle.partitions', '4')
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    .config('spark.sql.catalog.lake', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.lake.type', 'hadoop')
    .config('spark.sql.catalog.lake.warehouse', str(ROOT / 'iceberg'))
    .getOrCreate())

class Item(BaseModel):
    name: str
    path: str
class Execution(BaseModel):
    sql: str = Field(max_length=10000)
    datasets: list[Item] = Field(max_length=200)
    org_id: str
    max_rows: int = Field(ge=1, le=5000)
class Maintenance(BaseModel):
    org_id: str
    dataset: str
    path: str
    action: str

def auth(token):
    import hmac
    if not token or not hmac.compare_digest(token, TOKEN):
        raise HTTPException(403, 'Unauthorized')

def valid_path(org_id, path):
    if not ORG.fullmatch(org_id):
        raise HTTPException(422, 'Invalid organization')
    dest = Path(path).resolve()
    if not dest.is_file() or not dest.is_relative_to(ROOT / org_id) or dest.suffix != '.parquet':
        raise HTTPException(422, 'Invalid tenant dataset path')
    return str(dest)

def valid_name(name):
    if not NAME.fullmatch(name):
        raise HTTPException(422, 'Invalid dataset name')
    return name

@app.get('/health')
def health(x_worker_token: str | None = Header(None)):
    auth(x_worker_token)
    return {'status': 'ok'}

@app.post('/execute')
def execute(body: Execution, x_worker_token: str | None = Header(None)):
    auth(x_worker_token)
    names = {valid_name(d.name) for d in body.datasets}
    for d in body.datasets:
        valid_path(body.org_id, d.path)
    try:
        trees = sqlglot.parse(body.sql, read='duckdb')
        if len(trees) != 1 or not isinstance(trees[0], (exp.Select, exp.Union, exp.Subquery)):
            raise ValueError('Only a single SELECT is allowed')
        tree = trees[0]
        forbidden = (exp.Command, exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
                     exp.Alter, exp.Copy, exp.Merge, exp.Set, exp.Use, exp.Attach)
        ctes = {c.alias.lower() for c in tree.find_all(exp.CTE)}
        if any(isinstance(n, forbidden) for n in tree.walk()):
            raise ValueError('Read-only queries only')
        if any(t.db or t.catalog or t.name.lower() not in names | ctes for t in tree.find_all(exp.Table)):
            raise ValueError('Query references an inaccessible dataset')
        banned = {'input_file_name', 'reflect', 'java_method', 'read_csv', 'read_parquet', 'parquet_scan', 'read_json', 'read_text', 'getenv'}
        if any(isinstance(n, exp.Func) and n.sql_name().lower() in banned for n in tree.walk()) or any(isinstance(n, exp.Anonymous) and n.name.lower() in banned for n in tree.walk()):
            raise ValueError('Function not allowed')
    except (sqlglot.ParseError, ValueError) as exc:
        raise HTTPException(422, str(exc)[:160]) from None
    # Spark SQL is evaluated with only the internal allow-listed views.
    with lock:
        for d in body.datasets:
            spark.read.parquet(valid_path(body.org_id, d.path)).createOrReplaceTempView(d.name)
        try:
            result = spark.sql('SELECT * FROM (' + tree.sql(dialect='spark') + f') _bounded LIMIT {body.max_rows}')
            rows = result.collect()
            return {'columns': result.columns, 'rows': [list(map(_json, r)) for r in rows],
                    'count':len(rows), 'truncated_at':body.max_rows}
        except Exception:
            raise HTTPException(422, 'Spark query failed') from None
        finally:
            for d in body.datasets:
                spark.catalog.dropTempView(d.name)

def _json(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)

@app.post('/maintenance')
def maintenance(body: Maintenance, x_worker_token: str | None = Header(None)):
    auth(x_worker_token)
    name = valid_name(body.dataset)
    path = valid_path(body.org_id, body.path)
    if body.action not in ('sync', 'compact'):
        raise HTTPException(422, 'Invalid action')
    namespace = 't_' + body.org_id.replace('-', '_')
    identifier = f'lake.{namespace}.{name}'
    with lock:
        try:
            spark.sql(f'CREATE NAMESPACE IF NOT EXISTS lake.{namespace}')
            if body.action == 'sync':
                # Replace is explicit, never triggered by an analytics SELECT.
                spark.read.parquet(path).writeTo(identifier).using('iceberg').createOrReplace()
                return {'table':identifier, 'action':'sync', 'status':'completed'}
            if not spark.catalog.tableExists(identifier):
                raise HTTPException(409, 'Sync dataset to Iceberg before compacting')
            # Iceberg binpack rewrite merges small data files where worthwhile.
            result = spark.sql(f"CALL lake.system.rewrite_data_files(table => '{namespace}.{name}')").collect()
            return {'table':identifier, 'action':'compact', 'status':'completed',
                    'details':[str(row) for row in result][:10]}
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(422, 'Iceberg operation failed; check Spark service logs') from None
