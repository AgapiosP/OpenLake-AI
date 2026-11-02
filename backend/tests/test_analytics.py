import pytest
from fastapi import HTTPException
from app.analytics import safe_sql

@pytest.mark.parametrize('query',[
    'DROP TABLE sales', 'DELETE FROM sales', 'SELECT * FROM other',
    "SELECT * FROM read_parquet('/etc/passwd')", 'SELECT * FROM sales; SELECT 1',
    'SELECT * FROM information_schema.tables',
])
def test_rejects_unsafe_sql(query):
    with pytest.raises(HTTPException):
        safe_sql(query, {'sales'})

def test_valid_query_is_bounded():
    sql=safe_sql('SELECT category, COUNT(*) FROM sales GROUP BY category',{'sales'})
    assert 'LIMIT' in sql and 'GROUP BY' in sql
