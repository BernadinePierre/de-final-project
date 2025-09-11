import io
import json
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
import src.ingestion_lambda as ingestion
import src.process_lambda as process
import src.warehousing_lambda as warehouse


def test_get_secret_returns_dict(monkeypatch):
    """Check secret parsing works with fake Secrets Manager response."""

    fake_response = {"SecretString": '"crigglestone":{"host":"localhost","port":5432,"database":"db","user":"u","password":"p"}'}
    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = fake_response

    with patch("boto3.client", return_value=mock_client):
        secret = ingestion.get_secret()
        assert isinstance(secret, dict)
        assert "host" in secret


def test_connect_to_original_database_success(monkeypatch):
    """Check pg8000.connect is called with proper params."""
    fake_secret = {
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "u",
        "password": "p",
    }

    monkeypatch.setattr(ingestion, "get_secret", lambda: fake_secret)

    mock_conn = MagicMock()
    with patch("pg8000.connect", return_value=mock_conn) as mock_connect:
        conn = ingestion.connect_to_original_database()
        assert conn == mock_conn
        mock_connect.assert_called_once()


def test_check_original_update_returns_latest(monkeypatch):
    """Check latest date string is returned."""
    mock_conn = MagicMock()
    fake_df = pd.DataFrame({"last_updated": ["2025-09-11 12:00:00"]})

    monkeypatch.setattr(
        ingestion.wr.postgresql,
        "read_sql_query",
        lambda sql, con: fake_df,
    )

    date = ingestion.check_original_update("staff", mock_conn)
    assert date == "2025-09-11 12:00:00"


def test_get_original_updates_fetches_dataframe(monkeypatch):
    """Check dataframe returned for updated rows."""
    mock_conn = MagicMock()
    fake_df = pd.DataFrame({"staff_id": [1], "first_name": ["Alice"]})

    monkeypatch.setattr(
        ingestion.wr.postgresql,
        "read_sql_query",
        lambda sql, con: fake_df,
    )

    df = ingestion.get_original_updates("staff", mock_conn, "2025-09-10 00:00:00")
    assert not df.empty
    assert "first_name" in df.columns


def test_put_in_s3_calls_wr_s3(monkeypatch):
    """Check that wr.s3.to_csv is called."""
    fake_df = pd.DataFrame({"id": [1]})
    called = {}

    def fake_to_csv(df, path, index):
        called["was_called"] = True
        called["path"] = path

    monkeypatch.setattr(ingestion.wr.s3, "to_csv", fake_to_csv)

    ingestion.put_in_s3("staff", fake_df, "2025-09-11")
    assert called["was_called"]
    assert "staff" in called["path"]


def test_get_updates_table_creates_file(monkeypatch):
    """Check update_tracking.json is created if missing."""

    mock_client = MagicMock()
    # head_object raises 404 first
    error = ingestion.ClientError(
        {"Error": {"Code": "404"}}, "HeadObject"
    )
    mock_client.head_object.side_effect = error
    mock_client.get_object.return_value = {
        "Body": io.BytesIO(json.dumps(ingestion.DATA_UPDATES).encode("utf-8"))
    }

    updates = ingestion.get_updates_table(mock_client)
    assert isinstance(updates, dict)
    mock_client.put_object.assert_called_once()
