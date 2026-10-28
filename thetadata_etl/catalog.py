from __future__ import annotations

import threading

from thetadata_etl.config import Settings
from thetadata_etl.datasets import (
    MIN_SYMBOL_ROWS,
    OPTION_DATA_TABLE,
    OPTION_VOLUME_TABLE,
    WAREHOUSE,
    object_key,
    qualified_table,
    require_catalog_uri,
    require_dataset,
    require_fetch_date,
    require_option_part_key,
    require_option_symbol,
)
from thetadata_etl.models import AppendResult
from thetadata_etl.options import OPTION_COLUMNS

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


_OPTION_TABLES = frozenset({OPTION_DATA_TABLE, OPTION_VOLUME_TABLE})
_OPTION_CSV_TYPES = {
    "symbol": "VARCHAR",
    "expiration": "DATE",
    "strike": "DOUBLE",
    "right": "VARCHAR",
    "created": "TIMESTAMP",
    "last_trade": "TIMESTAMP",
    "open": "DOUBLE",
    "high": "DOUBLE",
    "low": "DOUBLE",
    "close": "DOUBLE",
    "volume": "BIGINT",
    "count": "BIGINT",
    "bid_size": "BIGINT",
    "bid_exchange": "INTEGER",
    "bid": "DOUBLE",
    "bid_condition": "INTEGER",
    "ask_size": "BIGINT",
    "ask_exchange": "INTEGER",
    "ask": "DOUBLE",
    "ask_condition": "INTEGER",
    "open_interest": "BIGINT",
    "date": "DATE",
}


def _sql_name(name: str) -> str:
    if not name.isascii() or not name.isidentifier():
        raise ValueError(f"invalid SQL name: {name}")
    return f'"{name}"'


def create_option_data_sql() -> str:
    # DuckDB reserves RIGHT, so every option column is quoted.
    columns = ",\n    ".join(
        f"{_sql_name(name)} {_OPTION_CSV_TYPES[name]}" for name in OPTION_COLUMNS
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {OPTION_DATA_TABLE} (\n    {columns}\n)"
    )


def create_option_volume_sql() -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {OPTION_VOLUME_TABLE} (\n"
        f"    {_sql_name('symbol')} VARCHAR,\n"
        f"    {_sql_name('date')} DATE,\n"
        f"    {_sql_name('total_volume')} BIGINT\n)"
    )


def option_count_sql(trade_date: str) -> str:
    day = require_fetch_date(trade_date)
    return f"SELECT count(*) FROM {OPTION_DATA_TABLE} WHERE date = DATE '{day}'"


def symbols_present_sql(table: str, trade_date: str, symbols: list[str]) -> str:
    relation = _require_option_table(table)
    day = require_fetch_date(trade_date)
    if not symbols:
        raise ValueError("symbols_present_sql requires at least one symbol")
    quoted = ", ".join(f"'{require_option_symbol(symbol)}'" for symbol in symbols)
    return (
        f"SELECT DISTINCT symbol FROM {relation} "
        f"WHERE date = DATE '{day}' AND symbol IN ({quoted})"
    )


def latest_volume_sql(trade_date: str) -> str:
    day = require_fetch_date(trade_date)
    return f"""
SELECT symbol, total_volume
FROM (
    SELECT symbol, total_volume,
           row_number() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM {OPTION_VOLUME_TABLE}
    WHERE date < DATE '{day}'
) AS latest
WHERE rn = 1
""".strip()


def option_insert_sql(part_key: str, skip_symbols: list[str]) -> str:
    key = require_option_part_key(part_key)
    columns = ", ".join(_sql_name(name) for name in OPTION_COLUMNS)
    return f"""
INSERT INTO {OPTION_DATA_TABLE} ({columns})
SELECT {columns}
FROM read_csv(
    's3://securities-data/{key}',
    header = true,
    columns = {_option_csv_columns()}
){_skip_clause(skip_symbols)}
""".strip()


def volume_insert_sql(part_key: str, skip_symbols: list[str]) -> str:
    key = require_option_part_key(part_key)
    symbol = _sql_name("symbol")
    day = _sql_name("date")
    volume = _sql_name("volume")
    total = _sql_name("total_volume")
    return f"""
INSERT INTO {OPTION_VOLUME_TABLE} ({symbol}, {day}, {total})
SELECT {symbol}, {day}, CAST(sum({volume}) AS BIGINT) AS {total}
FROM read_csv(
    's3://securities-data/{key}',
    header = true,
    columns = {_option_csv_columns()}
){_skip_clause(skip_symbols)}
GROUP BY {symbol}, {day}
""".strip()


def _require_option_table(table: str) -> str:
    if table not in _OPTION_TABLES:
        raise ValueError(f"unknown option table: {table}")
    return table


def _option_csv_columns() -> str:
    body = ", ".join(
        f"'{name}': '{_OPTION_CSV_TYPES[name]}'" for name in OPTION_COLUMNS
    )
    return "{" + body + "}"


def _skip_clause(symbols: list[str]) -> str:
    if not symbols:
        return ""
    quoted = ", ".join(f"'{require_option_symbol(symbol)}'" for symbol in symbols)
    return f"\nWHERE symbol NOT IN ({quoted})"


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


def ensure_option_tables(client: QuackCatalog, catalog_uri: str) -> None:
    ensure_catalog(client, catalog_uri)
    client.execute(create_option_data_sql())
    client.execute(create_option_volume_sql())


def append_option_part(
    client: QuackCatalog,
    catalog_uri: str,
    trade_date: str,
    part_key: str,
    symbols: list[str],
) -> int:
    day = require_fetch_date(trade_date)
    key = require_option_part_key(part_key)
    validated = [require_option_symbol(symbol) for symbol in symbols]
    ensure_option_tables(client, catalog_uri)
    before = client.scalar(option_count_sql(day))
    if validated:
        present = _symbol_set(
            client.execute(symbols_present_sql(OPTION_DATA_TABLE, day, validated))
        )
        if any(symbol not in present for symbol in validated):
            skip = [symbol for symbol in validated if symbol in present]
            client.execute(option_insert_sql(key, skip))
        summarized = _symbol_set(
            client.execute(symbols_present_sql(OPTION_VOLUME_TABLE, day, validated))
        )
        if any(symbol not in summarized for symbol in validated):
            volume_skip = [symbol for symbol in validated if symbol in summarized]
            client.execute(volume_insert_sql(key, volume_skip))
    after = client.scalar(option_count_sql(day))
    return after - before


def append_option_csv(
    settings: Settings,
    trade_date: str,
    part_key: str,
    symbols: list[str],
) -> int:
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        return append_option_part(
            client,
            settings.iceberg_catalog_uri,
            trade_date,
            part_key,
            symbols,
        )
    finally:
        client.close()


def latest_option_volumes(settings: Settings, trade_date: str) -> dict[str, int]:
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        ensure_option_tables(client, settings.iceberg_catalog_uri)
        rows = client.execute(latest_volume_sql(trade_date))
    finally:
        client.close()
    volumes: dict[str, int] = {}
    for row in rows:
        symbol = require_option_symbol(str(row[0]))
        raw = row[1]
        volumes[symbol] = 0 if raw is None else int(raw)
    return volumes


def option_session_row_count(settings: Settings, trade_date: str) -> int:
    client = QuackCatalog(settings.quack_uri, settings.quack_token)
    try:
        ensure_option_tables(client, settings.iceberg_catalog_uri)
        return client.scalar(option_count_sql(trade_date))
    finally:
        client.close()


def _symbol_set(rows: list[tuple]) -> set[str]:
    return {str(row[0]) for row in rows}
