import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

load_dotenv()
logger = logging.getLogger(__name__)

RETRYABLE_S3_ERROR_CODES = {
    "SlowDown",
    "RequestTimeout",
    "RequestTimeTooSkewed",
    "InternalError",
    "ServiceUnavailable",
    "Throttling",
}


def is_transient_s3_error(exception: Exception) -> bool:
    if isinstance(exception, ClientError):
        code = exception.response.get("Error", {}).get("Code", "")
        return code in RETRYABLE_S3_ERROR_CODES
    if isinstance(exception, BotoCoreError):
        # Lỗi tầng network/connection (timeout, connection reset...) — luôn
        # an toàn để retry vì bản chất là sự cố tạm thời, không phải lỗi logic.
        return True
    return False


def generate_s3_key(prefix: str, fetched_at: datetime) -> str:
    """Tạo S3 key chuẩn định dạng partition Data Lake.
    
    Ví dụ: raw/coins/markets/date=2026-08-15/fetched_at=2026-08-15T14-15-00Z.json
    """
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    else:
        fetched_at = fetched_at.astimezone(timezone.utc)

    clean_prefix = prefix.strip("/")
    date_str = fetched_at.strftime("%Y-%m-%d")
    timestamp_str = fetched_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    return f"raw/{clean_prefix}/date={date_str}/fetched_at={timestamp_str}.json"


class S3Writer:
    def __init__(
        self,
        bucket_name: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
    ):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "")
        if not self.bucket_name:
            raise ValueError("bucket_name is required or must be set in S3_BUCKET_NAME env var")

        region = region_name or os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION") or os.getenv("REGION_NAME") or "ap-southeast-1"

        client_kwargs: dict[str, Any] = {"region_name": region}
        if aws_access_key_id and aws_secret_access_key:
            client_kwargs["aws_access_key_id"] = aws_access_key_id
            client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token

        endpoint = endpoint_url or os.getenv("S3_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            client_kwargs["endpoint_url"] = endpoint
            # MinIO local (không có DNS wildcard cho virtual-hosted-style)
            # thường cần path-style addressing để connect được đúng bucket.
            client_kwargs["config"] = Config(s3={"addressing_style": "path"})

        self.s3_client = boto3.client("s3", **client_kwargs)

    def ensure_bucket_exists(self) -> None:
        """Kiểm tra và tự động tạo bucket trên MinIO/LocalStack nếu chưa tồn tại."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchBucket", "NotFound"):
                logger.info(f"Bucket '{self.bucket_name}' not found. Creating it...")
                try:
                    self.s3_client.create_bucket(Bucket=self.bucket_name)
                except ClientError as create_err:
                    # Đôi khi S3 đòi LocationConstraint nếu không phải us-east-1
                    region = self.s3_client.meta.region_name
                    if region and region != "us-east-1":
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={"LocationConstraint": region},
                        )
                    else:
                        raise create_err

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception(is_transient_s3_error),
        reraise=True,
    )
    def _put_object_with_retry(self, key: str, json_bytes: bytes) -> None:
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=json_bytes,
            ContentType="application/json",
        )

    def upload_raw_json(
        self,
        endpoint: str,
        raw_data: list | dict | Any,
        fetched_at: datetime,
        indent: int | None = None,
    ) -> str:
        """Upload raw JSON payload lên S3 Bucket.

        Lưu ý idempotency: key được sinh từ `fetched_at` (xem generate_s3_key).
        Caller (DAG) PHẢI truyền fetched_at cố định theo logical_date/
        data_interval_start của Airflow, KHÔNG dùng datetime.now() — nếu
        không, mỗi lần task retry sẽ tạo ra 1 file raw khác nhau cho cùng
        1 lần chạy logic, gây trùng lặp rác trên S3.

        Returns:
            str: S3 key của file vừa upload.

        Raises:
            ClientError | BotoCoreError: Khi upload thất bại sau khi đã retry.
        """
        key = generate_s3_key(endpoint, fetched_at)
        json_bytes = json.dumps(raw_data, indent=indent, ensure_ascii=False).encode("utf-8")
        try:
            self._put_object_with_retry(key, json_bytes)
            logger.info(f"Successfully uploaded raw JSON to s3://{self.bucket_name}/{key}")
            return key
        except Exception as e:
            logger.error(f"Failed to upload raw JSON to s3://{self.bucket_name}/{key}: {e}")
            raise