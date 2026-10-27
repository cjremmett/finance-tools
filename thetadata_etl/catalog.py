from __future__ import annotations

import threading

from thetadata_etl.config import Settings
from thetadata_etl.datasets import (
    MIN_SYMBOL_ROWS,
    WAREHOUSE,
    object_key,
    qualified_table,
    require_catalog_uri,
    require_dataset,
    require_fetch_date,
)
from thetadata_etl.models import AppendResult

# The DuckDB server already has the Garage key in its environment. Keeping the
# secret in that environment means the SQL sent over Quack does not carry it.
SECRET_SQL = """
CREATE OR REPLACE SECRET garage_securities (
    TYPE s3,
    KEY_ID getenv('GARAGE_ACCESS_KEY_ID'),
    SECRET getenv('GARAGE_SECRET_ACCESS_KEY'),
    ENDPOINT regexp_replace(getenv('GARAGE_S3_ENDPOINT'), '^https?://', ''),
    REGION getenv('GARAGE_S3_REGION'),
    URL_STYLE 'path',
    USE_SSL false,
    SCOPE 's3://securities-data'
)
""".strip()

SCHEMA_SQL = f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE}.securities"

_install_lock = threading.Lock()
_quack_installed = False


def attach_sql(catalog_uri: str) -> str:
    uri = require_catalog_uri(catalog_uri)
    return f"""
ATTACH IF NOT EXISTS '{WAREHOUSE}' AS {WAREHOUSE} (
    TYPE iceberg,
    URI '{uri}',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
)
""".strip()


def create_table_sql(dataset: str) -> str:
    table = qualified_table(dataset)
    return f"CREATE TABLE IF NOT EXISTS {table} (symbol VARCHAR, date DATE)"


def count_sql(dataset: str, fetch_date: str) -> str:
    table = qualified_table(dataset)
    day = require_fetch_date(fetch_date)
    return f"SELECT count(*) FROM {table} WHERE date = DATE '{day}'"


def insert_sql(dataset: str, fetch_date: str) -> str:
    table = qualified_table(dataset)
    key = object_key(dataset, fetch_date)
    return f"""
INSERT INTO {table}
SELECT symbol, date
FROM read_csv(
    's3://securities-data/{key}',
    header = true,
    columns = {{'symbol': 'VARCHAR', 'date': 'DATE'}}
)
""".strip()


class QuackCatalog:
    def __init__(self, uri: str, token: str) -> None:
        self.uri = uri
        self.token = token
        self._connection = None

    def connection(self):
        global _quack_installed
        if self._connection is None:
            import duckdb

            connection = duckdb.connect()
            with _install_lock:
                if not _quack_installed:
                    connection.execute("INSTALL quack;")
                    _quack_installed = True
            connection.execute("LOAD quack;")
            self._connection = connection
        return self._connection

    def execute(self, sql: str) -> list[tuple]:
        return self.connection().execute(
            "FROM quack_query(?, ?, token := ?, disable_ssl := true)",
            [self.uri, sql, self.token],
        ).fetchall()

    def scalar(self, sql: str) -> int:
        rows = self.execute(sql)
        if len(rows) != 1 or len(rows[0]) != 1:
            raise RuntimeError(f"expected one scalar from DuckDB, got {rows!r}")
        return int(rows[0][0])

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None


def ensure_catalog(client: QuackCatalog, catalog_uri: str) -> None:
    client.execute(SECRET_SQL)
    client.execute(attach_sql(catalog_uri))
    client.execute(SCHEMA_SQL)


def append_csv(
    client: QuackCatalog,
    catalog_uri: str,
    dataset: str,
    fetch_date: str,
) -> tuple[bool, int]:
    require_dataset(dataset)
    require_fetch_date(fetch_date)
    ensure_catalog(client, catalog_uri)
    client.execute(create_table_sql(dataset))
    existing = client.scalar(count_sql(dataset, fetch_date))
    if existing > 0:
        if existing < MIN_SYMBOL_ROWS:
            raise RuntimeError(
                f"Iceberg table {dataset} already has {existing} rows for {fetch_date}; "
                f"expected at least {MIN_SYMBOL_ROWS}"
            )
        return False, existing
    client.execute(insert_sql(dataset, fetch_date))
    loaded = client.scalar(count_sql(dataset, fetch_date))
    if loaded < MIN_SYMBOL_ROWS:
        raise RuntimeError(
            f"Iceberg append for {dataset} on {fetch_date} loaded {loaded} rows; "
            f"expected at least {MIN_SYMBOL_ROWS}"
        )
    return True, loaded


def ensure_securities_catalog(settings: Settings) -> None:
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        ensure_catalog(client, settings.iceberg_catalog_uri)
    finally:
        client.close()


def append_symbol_csv(
    settings: Settings, dataset: str, fetch_date: str
) -> AppendResult:
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        appended, row_count = append_csv(
            client, settings.iceberg_catalog_uri, dataset, fetch_date
        )
    finally:
        client.close()
    return AppendResult(appended=appended, row_count=row_count)
