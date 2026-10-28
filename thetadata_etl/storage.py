from __future__ import annotations

from thetadata_etl.config import Settings
from thetadata_etl.datasets import BUCKET


class GarageStore:
    def __init__(self, client: object, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"404", "NoSuchKey", "NotFound"} or status == 404:
                return False
            raise
        return True

    def put_csv(self, key: str, body: bytes) -> None:
        self.put_bytes(key, body, "text/csv; charset=utf-8")

    def put_bytes(self, key: str, body: bytes, content_type: str) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    def get_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        body = response["Body"].read()
        if isinstance(body, str):
            return body.encode("utf-8")
        return body

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs: dict[str, object] = {"Bucket": self.bucket, "Prefix": prefix}
            if token is not None:
                kwargs["ContinuationToken"] = token
            page = self.client.list_objects_v2(**kwargs)
            for item in page.get("Contents", []):
                keys.append(item["Key"])
            if not page.get("IsTruncated"):
                return keys
            token = page.get("NextContinuationToken")
            if not token:
                return keys


def garage_store(settings: Settings) -> GarageStore:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "s3",
        endpoint_url=settings.garage_s3_endpoint,
        aws_access_key_id=settings.garage_access_key_id,
        aws_secret_access_key=settings.garage_secret_access_key,
        region_name=settings.garage_s3_region,
        config=Config(
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )
    return GarageStore(client, BUCKET)
