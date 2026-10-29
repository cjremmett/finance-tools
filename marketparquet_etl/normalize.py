from __future__ import annotations

import io
from datetime import date

import polars as pl

from marketparquet_etl.models import COLUMNS, MIN_ROWS, require_session_date

_ASSET_LABELS = {"Stock", "ETF"}

_PRICE_COLUMNS = ("open", "high", "low", "close", "volume")


def parquet_to_csv(
    payload: bytes,
    session_date: str,
    *,
    min_rows: int = MIN_ROWS,
    asset_label: str = "Stock",
) -> tuple[bytes, int]:
    if asset_label not in _ASSET_LABELS:
        raise ValueError(f"unsupported asset label: {asset_label}")
    if min_rows < 1:
        raise ValueError("minimum row count must be positive")
    day = require_session_date(session_date)
    frame = pl.read_parquet(io.BytesIO(payload))
    missing = [name for name in ("symbol", "asset_type", *_PRICE_COLUMNS) if name not in frame.columns]
    if missing:
        raise ValueError(
            f"session {day} is missing columns: {', '.join(missing)}"
        )
    dates = _trading_dates(frame)
    out = frame.with_columns(dates.alias("date")).select(list(COLUMNS))
    out = out.with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("symbol").cast(pl.String),
        pl.col("asset_type").cast(pl.String),
        *[pl.col(name).cast(pl.Float64) for name in _PRICE_COLUMNS],
    )
    nulls = [name for name in COLUMNS if out[name].null_count()]
    if nulls:
        raise ValueError(f"session {day} has null values in {', '.join(nulls)}")
    if out["symbol"].n_unique() != out.height:
        raise ValueError(f"session {day} has duplicate symbols")
    expected = date.fromisoformat(day)
    if out.filter(pl.col("date") != expected).height:
        raise ValueError(f"session {day} contains another trading date")
    labels = set(out["asset_type"].unique().to_list())
    if labels != {asset_label}:
        raise ValueError(
            f"session {day} has asset_type {sorted(labels)}; expected {asset_label}"
        )
    if out.height < min_rows:
        raise ValueError(
            f"session {day} has {out.height} rows; expected at least {min_rows}"
        )
    text = out.write_csv(float_precision=12)
    return text.encode("utf-8"), out.height


def _trading_dates(frame: pl.DataFrame) -> pl.Series:
    if "date" in frame.columns and frame["date"].null_count() < frame.height:
        return frame["date"].cast(pl.Date)
    if "timestamp" in frame.columns:
        return frame["timestamp"].dt.date()
    raise ValueError("parquet file has neither date nor timestamp")
