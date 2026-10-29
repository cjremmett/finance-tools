from __future__ import annotations

import threading
from datetime import date, datetime

from marketparquet_etl.config import require_catalog_uri
from marketparquet_etl.models import (
    COLUMNS,
    CSV_TYPES,
    QUALIFIED_TABLE,
    STOCKS,
    WAREHOUSE,
    object_key,
    require_asset,
    require_csv_key,
    require_session_date,
)

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


def create_table_sql() -> str:
    columns = ",\n    ".join(f"{name} {CSV_TYPES[name]}" for name in COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {QUALIFIED_TABLE} (\n    {columns}\n)"
    )


def count_sql(day: str, asset: str = STOCKS) -> str:
    spec = require_asset(asset)
    session = require_session_date(day)
    return (
        f"SELECT count(*) FROM {QUALIFIED_TABLE} "
        f"WHERE date = DATE '{session}' AND asset_type = '{spec.label}'"
    )


def loaded_dates_sql(asset: str = STOCKS) -> str:
    spec = require_asset(asset)
    return (
        f"SELECT date FROM {QUALIFIED_TABLE} "
        f"WHERE asset_type = '{spec.label}' "
        f"GROUP BY date HAVING count(*) >= {spec.min_rows} "
        f"ORDER BY date"
    )


def insert_sql(day: str, asset: str = STOCKS, key: str | None = None) -> str:
    spec = require_asset(asset)
    session = require_session_date(day)
    chosen = require_csv_key(
        spec.name, object_key(spec.name, session) if key is None else key
    )
    columns = ", ".join(COLUMNS)
    mapping = ", ".join(f"'{name}': '{CSV_TYPES[name]}'" for name in COLUMNS)
    return f"""
INSERT INTO {QUALIFIED_TABLE} ({columns})
SELECT {columns}
FROM read_csv(
    's3://securities-data/{chosen}',
    header = true,
    columns = {{{mapping}}}
)
WHERE date = DATE '{session}' AND asset_type = '{spec.label}'
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
    client.execute(create_table_sql())


def loaded_dates(client: QuackCatalog, asset: str = STOCKS) -> list[str]:
    dates: list[str] = []
    for (value,) in client.execute(loaded_dates_sql(asset)):
        dates.append(_iso_date(value))
    return sorted(set(dates))


def append_day(
    client: QuackCatalog, day: str, asset: str = STOCKS, key: str | None = None
) -> tuple[bool, int]:
    spec = require_asset(asset)
    session = require_session_date(day)
    existing = client.scalar(count_sql(session, spec.name))
    if existing >= spec.min_rows:
        return False, existing
    if existing > 0:
        raise RuntimeError(
            f"Iceberg table stock_data already has {existing} {spec.label} rows "
            f"for {session}; expected at least {spec.min_rows}"
        )
    client.execute(insert_sql(session, spec.name, key))
    loaded = client.scalar(count_sql(session, spec.name))
    if loaded < spec.min_rows:
        raise RuntimeError(
            f"Iceberg append for stock_data {spec.label} on {session} loaded "
            f"{loaded} rows; expected at least {spec.min_rows}"
        )
    return True, loaded


def _iso_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).split("T", 1)[0].split(" ", 1)[0]
    return require_session_date(text)
