import json
import unittest
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from ingestion.s3_writer import S3Writer, generate_s3_key


class TestS3Writer(unittest.TestCase):

    def test_generate_s3_key_utc(self):
        dt = datetime(2026, 8, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = generate_s3_key("coins/markets", dt)
        expected = "raw/coins/markets/date=2026-08-15/fetched_at=2026-08-15T14-30-00Z.json"
        self.assertEqual(key, expected)

    def test_generate_s3_key_strip_slashes(self):
        dt = datetime(2026, 8, 15, 14, 30, 0, tzinfo=timezone.utc)
        key = generate_s3_key("/coins/bitcoin/market_chart/", dt)
        expected = "raw/coins/bitcoin/market_chart/date=2026-08-15/fetched_at=2026-08-15T14-30-00Z.json"
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
        dt = datetime(2026, 8, 15, 10, 0, 0, tzinfo=timezone.utc)
        raw_data = [{"id": "bitcoin", "price": 60000}]

        key = writer.upload_raw_json("coins/markets", raw_data, dt)
        expected_key = "raw/coins/markets/date=2026-08-15/fetched_at=2026-08-15T10-00-00Z.json"
        self.assertEqual(key, expected_key)

        # Kiểm tra nội dung thực sự được ghi vào Moto S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = json.loads(response["Body"].read().decode("utf-8"))
        self.assertEqual(content, raw_data)
        self.assertEqual(response["ContentType"], "application/json")

    @mock_aws
    def test_upload_raw_json_non_existent_bucket_raises_client_error(self):
        writer = S3Writer(bucket_name="non-existent-bucket", region_name="ap-southeast-1")
        dt = datetime(2026, 8, 15, 10, 0, 0, tzinfo=timezone.utc)

        with self.assertRaises(ClientError):
            writer.upload_raw_json("coins/markets", {"test": "data"}, dt)

    def test_s3_writer_missing_bucket_raises_value_error(self):
        with self.assertRaises(ValueError):
            S3Writer(bucket_name="")


if __name__ == "__main__":
    unittest.main()
