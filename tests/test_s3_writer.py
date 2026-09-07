import json
import os
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from ingestion.s3_writer import S3Writer, generate_s3_key


class TestS3Writer(unittest.TestCase):
    def setUp(self):
        # Unset S3_ENDPOINT_URL so boto3 talks to Moto instead of real localhost:9000
        self.env_patcher = patch.dict(os.environ, {"S3_ENDPOINT_URL": ""})
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    def test_generate_s3_key_utc(self):
        dt = datetime(2026, 8, 15, 14, 30, 0, tzinfo=UTC)
        key = generate_s3_key("coins/markets", dt)
        expected = "raw/coins/markets/date=2026-08-15/fetched_at=2026-08-15T14-30-00Z.json"
        self.assertEqual(key, expected)

    def test_generate_s3_key_strip_slashes(self):
        dt = datetime(2026, 8, 15, 14, 30, 0, tzinfo=UTC)
        key = generate_s3_key("/coins/bitcoin/market_chart/", dt)
        expected = (
            "raw/coins/bitcoin/market_chart/date=2026-08-15/fetched_at=2026-08-15T14-30-00Z.json"
        )
        self.assertEqual(key, expected)

    @mock_aws
    def test_s3_writer_init_and_upload_with_moto(self):
        bucket_name = "test-bucket"
        region_name = "ap-southeast-1"

        # Tạo bucket trên Moto S3 giả lập
        s3_client = boto3.client("s3", region_name=region_name)
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region_name},
        )

        writer = S3Writer(bucket_name=bucket_name, region_name=region_name)
        dt = datetime(2026, 8, 15, 10, 0, 0, tzinfo=UTC)
        raw_data = [{"id": "bitcoin", "price": 60000}]

        key = writer.upload_raw_json("coins/markets", raw_data, dt)
        expected_key = "raw/coins/markets/date=2026-08-15/fetched_at=2026-08-15T10-00-00Z.json"
        self.assertEqual(key, expected_key)

        # Kiểm tra nội dung thực sự được ghi vào Moto S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = json.loads(response["Body"].read().decode("utf-8"))
        self.assertEqual(content, raw_data)
        self.assertEqual(response["ContentType"], "application/json")

        # Kiểm tra hàm read_raw_json
        read_back = writer.read_raw_json(key)
        self.assertEqual(read_back, raw_data)

    @mock_aws
    def test_ensure_bucket_exists_creates_bucket(self):
        bucket_name = "auto-created-bucket"
        region_name = "ap-southeast-1"
        writer = S3Writer(bucket_name=bucket_name, region_name=region_name)
        writer.ensure_bucket_exists()

        s3_client = boto3.client("s3", region_name=region_name)
        response = s3_client.head_bucket(Bucket=bucket_name)
        self.assertEqual(response["ResponseMetadata"]["HTTPStatusCode"], 200)

    @mock_aws
    def test_upload_raw_json_non_existent_bucket_raises_client_error(self):
        writer = S3Writer(bucket_name="non-existent-bucket", region_name="ap-southeast-1")
        dt = datetime(2026, 8, 15, 10, 0, 0, tzinfo=UTC)

        with self.assertRaises(ClientError):
            writer.upload_raw_json("coins/markets", {"test": "data"}, dt)

    def test_s3_writer_missing_bucket_raises_value_error(self):
        with patch.dict(os.environ, {"S3_BUCKET_NAME": ""}):
            with self.assertRaises(ValueError):
                S3Writer(bucket_name="")


if __name__ == "__main__":
    unittest.main()
