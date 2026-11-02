import re
import duckdb
import pyarrow.parquet as pq
import sqlglot
from sqlglot import exp
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from .models import Dataset
from .config import get_settings

NAME_RE = re.compile(r'^[a-z][a-z0-9_]{0,89}$')
DISALLOWED = (exp.Command, exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
              exp.Alter, exp.Copy, exp.Merge, exp.Attach, exp.Pragma, exp.Set, exp.Use)
# External table functions and filesystem access are never permitted.
UNSAFE_FUNCTIONS = {'read_csv', 'read_csv_auto', 'read_parquet', 'sqlite_scan', 'postgres_scan',
                    'httpfs', 'query', 'query_table', 'parquet_scan', 'read_json',
                    'read_json_auto', 'glob', 'read_text', 'write_file', 'sqlite_attach',
                    'read_ndjson', 'read_blob', 'getenv', 'current_setting'}

def safe_sql(sql: str, names: set[str]) -> str:
    try:
        trees = sqlglot.parse(sql, read='duckdb')
    except sqlglot.ParseError as exc:
        raise HTTPException(422, f'Invalid SQL: {str(exc)[:160]}') from None
    if len(trees) != 1 or trees[0] is None:
        raise HTTPException(422, 'Exactly one SELECT query is required')
    tree = trees[0]
    if not isinstance(tree, (exp.Select, exp.Union, exp.Subquery)):
        raise HTTPException(422, 'Only SELECT statements are supported')
    for node in tree.walk():
        if isinstance(node, DISALLOWED):
            raise HTTPException(422, 'Read-only queries only')
        if isinstance(node, exp.Func) and node.sql_name().lower() in UNSAFE_FUNCTIONS:
            raise HTTPException(422, 'External functions are unavailable')
        if isinstance(node, exp.Anonymous) and node.name.lower() in UNSAFE_FUNCTIONS:
            raise HTTPException(422, 'External functions are unavailable')
    ctes = {c.alias.lower() for c in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        if table.db or table.catalog or table.name.lower() not in names | ctes:
            raise HTTPException(422, f'Dataset not accessible: {table.name}')
    # An outer LIMIT is applied even when a user supplies their own.
    return f'SELECT * FROM ({tree.sql(dialect="duckdb")}) AS _bounded LIMIT {get_settings().max_query_rows}'

def query_data(db: Session, org_id: str, sql: str):
    datasets = db.scalars(select(Dataset).where(Dataset.organization_id == org_id)).all()
    names = {d.name for d in datasets}
    bounded = safe_sql(sql, names)
    conn = duckdb.connect(':memory:', config={'enable_external_access': 'false', 'threads': '2', 'memory_limit': '512MB'})
    try:
        for dataset in datasets:
            # Read only server-controlled dataset paths, then register in memory.
            table = pq.read_table(dataset.storage_path)
            conn.register(dataset.name, table)
        result = conn.execute(bounded)
        cols = [c[0] for c in result.description]
        rows = [[str(v) if not isinstance(v, (int, float, bool, type(None))) else v for v in row] for row in result.fetchall()]
        return {'columns': cols, 'rows': rows, 'count': len(rows), 'truncated_at': get_settings().max_query_rows}
    except (duckdb.Error, OSError) as exc:
        raise HTTPException(422, f'Query failed: {str(exc)[:240]}') from None
    finally:
        conn.close()
