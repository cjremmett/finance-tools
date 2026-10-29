from __future__ import annotations

import re

from marketparquet_etl.config import Settings
from marketparquet_etl.models import (
    BUCKET,
    KEY_PREFIX,
    STOCKS,
    legacy_stock_key,
    object_key,
    require_asset,
)

_PREFIXED_KEY = re.compile(
    rf"^{KEY_PREFIX}/(stocks|etfs)-(\d{{4}}-\d{{2}}-\d{{2}})\.csv$"
)
_LEGACY_KEY = re.compile(rf"^{KEY_PREFIX}/(\d{{4}}-\d{{2}}-\d{{2}})\.csv$")
_MISSING_BUCKET = {"404", "NoSuchBucket", "NotFound"}
_OWNED_BUCKET = {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}


class GarageStore:
    def __init__(self, client: object, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self.client.head_bucket(Bucket=self.bucket)
            return
        except ClientError as error:
            if not _is_missing_bucket(error):
                raise
        try:
            self.client.create_bucket(Bucket=self.bucket)
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code not in _OWNED_BUCKET:
                raise

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
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="text/csv; charset=utf-8",
        )

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


def stored_session_dates(store: GarageStore, asset: str = STOCKS) -> list[str]:
    """Dates whose CSV is already in Garage.

    Stock days written before the prefix used ``YYYY-MM-DD.csv``. Those keys
    still count as the stock file. ETF days only match ``etfs-YYYY-MM-DD.csv``.
    """
    spec = require_asset(asset)
    dates: list[str] = []
    for key in store.list_keys(f"{KEY_PREFIX}/"):
        prefixed = _PREFIXED_KEY.fullmatch(key)
        if prefixed is not None and prefixed.group(1) == spec.name:
            dates.append(prefixed.group(2))
            continue
        if spec.name == STOCKS:
            legacy = _LEGACY_KEY.fullmatch(key)
            if legacy is not None:
                dates.append(legacy.group(1))
    return sorted(set(dates))


def existing_csv_key(store: GarageStore, asset: str, day: str) -> str | None:
    canonical = object_key(asset, day)
    if store.exists(canonical):
        return canonical
    if require_asset(asset).name == STOCKS:
        legacy = legacy_stock_key(day)
        if store.exists(legacy):
            return legacy
    return None


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


def _is_missing_bucket(error: object) -> bool:
    response = getattr(error, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code in _MISSING_BUCKET or status == 404
