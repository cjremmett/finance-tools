from __future__ import annotations

import logging
from datetime import date

import polars as pl

from thetadata_etl.datasets import FETCH_METHODS, MIN_SYMBOL_ROWS, require_dataset


def fetch_symbol_frame(dataset: str, api_key: str) -> pl.DataFrame:
    # ThetaClient logs the auth response, including the session id, at INFO.
    logging.getLogger("thetadata").setLevel(logging.ERROR)
    from thetadata import ThetaClient

    client = ThetaClient(api_key=api_key, dataframe_type="polars")
    method_name = FETCH_METHODS[require_dataset(dataset)]
    frame = getattr(client, method_name)()
    if not isinstance(frame, pl.DataFrame):
        raise RuntimeError(
            f"ThetaData {method_name} returned {type(frame).__name__}, expected a DataFrame"
        )
    if "symbol" not in frame.columns:
        raise RuntimeError(
            f"ThetaData {method_name} response has no symbol column: {frame.columns}"
        )
    return frame


def build_symbol_csv(
    frame: pl.DataFrame,
    fetch_date: date,
    *,
    minimum_rows: int = MIN_SYMBOL_ROWS,
) -> tuple[bytes, int]:
    if "symbol" not in frame.columns:
        raise RuntimeError(f"symbol frame has no symbol column: {frame.columns}")
    dated = frame.select(pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"))
    dated = (
        dated.filter(
            pl.col("symbol").is_not_null() & (pl.col("symbol").str.len_chars() > 0)
        )
        .with_columns(pl.lit(fetch_date).alias("date"))
        .select("symbol", "date")
    )
    if dated.height < minimum_rows:
        raise RuntimeError(
            f"ThetaData returned {dated.height} symbols; expected at least {minimum_rows}"
        )
    csv_text = dated.write_csv()
    if not isinstance(csv_text, str):
        raise RuntimeError("polars write_csv did not return CSV text")
    return csv_text.encode("utf-8"), dated.height
