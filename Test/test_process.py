import io
import pandas as pd
import pytest
from unittest.mock import MagicMock

import src.process_lambda as process


class FakeS3Client:
    """Minimal fake S3 client for process lambda tests."""
    def __init__(self, csv_data=None):
        self.csv_data = csv_data or "id,name\n1,Alice\n2,Bob"
        self.objects = {}

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.csv_data.encode("utf-8"))}

    def list_objects(self, Bucket, Prefix):
        return {"Contents": [{"Key": "staff/file1.csv"}]}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body
        return True

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise process.ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}


def test_fetch_file_from_ingest_reads_csv():
    client = FakeS3Client("staff_id,first_name\n1,Alice")
    df = process.fetch_file_from_ingest(client, "staff/file1.csv")
    assert not df.empty
    assert list(df.columns) == ["staff_id", "first_name"]


def test_get_keys_for_table_returns_keys():
    client = FakeS3Client()
    keys = process.get_keys_for_table(client, "staff")
    assert isinstance(keys, list)
    assert keys[0].endswith(".csv")


def test_put_in_processed_stores_parquet(tmp_path):
    df = pd.DataFrame({"id": [1], "name": ["Alice"]})
    client = FakeS3Client()
    process.put_in_processed(client, "staff", df)
    assert "staff.parquet" in client.objects


def test_make_dim_currency(monkeypatch):
    """Check currency dimension is built correctly."""
    fake_df = pd.DataFrame({"currency_id": [1, 2], "currency_code": ["GBP", "USD"]})
    monkeypatch.setattr(process, "get_from_ingest", lambda client, table: fake_df)

    client = FakeS3Client()
    dim = process.make_dim_currency(client)
    assert "currency_code" in dim.columns
    assert len(dim) == 2


def test_lambda_handler_with_currency(monkeypatch):
    """Check lambda handler runs and puts processed parquet."""

    # Fake S3 client
    s3_client = FakeS3Client("currency_id,currency_code\n1,GBP")

    # Fake Lambda client with an invoke method
    lambda_client = MagicMock()
    lambda_client.invoke.return_value = {"StatusCode": 202}

    # Patch get_from_ingest to return a small dataframe
    monkeypatch.setattr(process, "get_from_ingest", lambda c, t: pd.DataFrame({
        "currency_id": [1],
        "currency_code": ["GBP"]
    }))

    # Patch put_in_processed to skip writing
    monkeypatch.setattr(process, "put_in_processed", lambda c, t, d: True)

    # Patch boto3.client to return the correct fake depending on service
    def fake_boto3_client(service_name, *args, **kwargs):
        if service_name == "s3":
            return s3_client
        elif service_name == "lambda":
            return lambda_client
        raise ValueError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(process, "boto3", MagicMock())
    process.boto3.client.side_effect = fake_boto3_client

    event = {"updates": ["currency"]}
    result = process.lambda_handler(event, None)

    # Just check it runs without errors
    assert result is None
    lambda_client.invoke.assert_called_once()
