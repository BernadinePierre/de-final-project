import pandas as pd
import pytest
from unittest.mock import MagicMock

import src.warehousing_lambda as warehouse


def test_get_rds_secret_returns_dict(monkeypatch):
    """Check secret is returned from mocked boto3 secrets manager."""
    fake_secret = {
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "username": "u",
        "password": "p",
    }
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {
        "SecretString": '{"host":"localhost","port":5432,"database":"db","username":"u","password":"p"}'
    }

    monkeypatch.setattr(warehouse, "boto3", MagicMock())
    warehouse.boto3.client.return_value = mock_client

    result = warehouse.get_rds_secret()
    assert result == fake_secret


def test_connect_to_warehouse_success(monkeypatch):
    """Check pg8000.connect called with secret values."""
    fake_secret = {
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "username": "u",
        "password": "p",
    }
    monkeypatch.setattr(warehouse, "get_rds_secret", lambda: fake_secret)

    mock_conn = MagicMock()
    monkeypatch.setattr(warehouse.pg8000, "connect", lambda **kwargs: mock_conn)

    conn = warehouse.connect_to_warehouse()
    assert conn == mock_conn


def test_load_parquet_to_warehouse_with_data(monkeypatch):
    """Check parquet file loads to warehouse via awswrangler."""
    fake_df = pd.DataFrame({"id": [1], "name": ["Alice"]})

    monkeypatch.setattr(warehouse.wr.s3, "read_parquet", lambda path: fake_df)
    monkeypatch.setattr(warehouse, "connect_to_warehouse", lambda: MagicMock())
    monkeypatch.setattr(warehouse.wr.postgresql, "to_sql", lambda **kwargs: True)

    # Should not raise
    warehouse.load_parquet_to_warehouse("dim-staff.parquet")


def test_load_parquet_to_warehouse_empty(monkeypatch):
    """Check empty parquet skips load."""
    empty_df = pd.DataFrame()

    monkeypatch.setattr(warehouse.wr.s3, "read_parquet", lambda path: empty_df)
    monkeypatch.setattr(warehouse, "connect_to_warehouse", lambda: MagicMock())
    monkeypatch.setattr(warehouse.wr.postgresql, "to_sql", lambda **kwargs: True)

    result = warehouse.load_parquet_to_warehouse("dim-staff.parquet")
    assert result is None


def test_preview_all_tables_logs(monkeypatch):
    """Check preview queries run for fake table list."""
    fake_tables = pd.DataFrame({"table_name": ["staff"]})
    fake_data = pd.DataFrame({"id": [1], "name": ["Bob"]})

    monkeypatch.setattr(warehouse, "connect_to_warehouse", lambda: MagicMock())
    monkeypatch.setattr(
        warehouse.wr.postgresql,
        "read_sql_query",
        lambda sql, con: fake_tables if "information_schema" in sql else fake_data,
    )
    monkeypatch.setattr(warehouse.wr.s3, "to_csv", lambda df, path, index: True)

    # Should run without errors
    warehouse.preview_all_tables()


def test_lambda_handler_with_s3_event(monkeypatch):
    """Check lambda handler processes S3 event records."""
    fake_df = pd.DataFrame({"id": [1]})

    monkeypatch.setattr(warehouse.wr.s3, "read_parquet", lambda path: fake_df)
    monkeypatch.setattr(warehouse, "connect_to_warehouse", lambda: MagicMock())
    monkeypatch.setattr(warehouse.wr.postgresql, "to_sql", lambda **kwargs: True)
    # ✅ patch preview_all_tables so it doesn't try real DB calls
    monkeypatch.setattr(warehouse, "preview_all_tables", lambda *a, **k: True)

    event = {"Records": [{"s3": {"object": {"key": "dim-staff.parquet"}}}]}
    result = warehouse.lambda_handler(event, None)

    assert result["statusCode"] == 200


def test_lambda_handler_manual_trigger(monkeypatch):
    """Check manual trigger scans bucket and loads parquet."""
    fake_df = pd.DataFrame({"id": [1]})

    monkeypatch.setattr(warehouse.wr.s3, "read_parquet", lambda path: fake_df)
    monkeypatch.setattr(warehouse, "connect_to_warehouse", lambda: MagicMock())
    monkeypatch.setattr(warehouse.wr.postgresql, "to_sql", lambda **kwargs: True)
    monkeypatch.setattr(warehouse, "preview_all_tables", lambda *a, **k: True)

    mock_s3_client = MagicMock()
    mock_s3_client.list_objects_v2.return_value = {"Contents": [{"Key": "dim-staff.parquet"}]}
    monkeypatch.setattr(warehouse.boto3, "client", lambda service: mock_s3_client)

    result = warehouse.lambda_handler({}, None)
    assert result["statusCode"] == 200
