from pathlib import Path
from types import SimpleNamespace
from fastapi import HTTPException
import pytest
from app.lakehouse import route_engine, cache_key

def test_default_duckdb():
    assert route_engine([], 'auto', False) == 'duckdb'

def test_explicit_spark_requires_service():
    with pytest.raises(HTTPException):
        route_engine([], 'spark', False)

def test_threshold_routing(tmp_path):
    f=tmp_path/'data.parquet'; f.write_bytes(b'0123456789')
    d=SimpleNamespace(id='a', storage_path=str(f))
    assert route_engine([d],'auto',True,threshold=10)=='spark'
    assert route_engine([d],'auto',True,threshold=11)=='duckdb'
    assert cache_key('org1','SELECT 1','duckdb',[d]) != cache_key('org2','SELECT 1','duckdb',[d])
