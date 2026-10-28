from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl

from thetadata_etl.datasets import (
    EASTERN,
    OPTION_DATA_WORKFLOW,
    OPTION_OI_PROBE_SYMBOL,
    require_option_symbol,
)

OPTION_COLUMNS = (
    "symbol",
    "expiration",
    "strike",
    "right",
    "created",
    "last_trade",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "count",
    "bid_size",
    "bid_exchange",
    "bid",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask",
    "ask_condition",
    "open_interest",
    "date",
)
REQUIRED_EOD = (
    "symbol",
    "expiration",
    "strike",
    "right",
    "open",
    "high",
    "low",
    "close",
    "volume",
)
_OPTION_SCHEMA = {
    "symbol": pl.Utf8,
    "expiration": pl.Date,
    "strike": pl.Float64,
    "right": pl.Utf8,
    "created": pl.Datetime("us"),
    "last_trade": pl.Datetime("us"),
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "count": pl.Int64,
    "bid_size": pl.Int64,
    "bid_exchange": pl.Int64,
    "bid": pl.Float64,
    "bid_condition": pl.Int64,
    "ask_size": pl.Int64,
    "ask_exchange": pl.Int64,
    "ask": pl.Float64,
    "ask_condition": pl.Int64,
    "open_interest": pl.Int64,
    "date": pl.Date,
}


def empty_option_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_OPTION_SCHEMA)


def intersect_symbols(stocks: pl.DataFrame, options: pl.DataFrame) -> list[str]:
    left = _symbol_column(stocks, "stocks")
    right = _symbol_column(options, "option underlyings")
    shared = left.join(right, on="symbol", how="inner").unique(subset=["symbol"])
    symbols = shared.sort("symbol")["symbol"].to_list()
    if not symbols:
        raise RuntimeError("stock and option underlying snapshots have no symbols in common")
    return [require_option_symbol(symbol) for symbol in symbols]


def order_underlyings(symbols: list[str], volumes: dict[str, int]) -> list[str]:
    return sorted(symbols, key=lambda symbol: (-int(volumes.get(symbol, 0)), symbol))


def open_interest_probe_symbol(volumes: dict[str, int]) -> str:
    """Symbol used to decide whether today's open-interest print is published."""
    if not volumes:
        return OPTION_OI_PROBE_SYMBOL
    return min(volumes, key=lambda symbol: (-int(volumes[symbol]), symbol))


def chunk_symbols(symbols: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("batch size must be >= 1")
    return [symbols[index : index + size] for index in range(0, len(symbols), size)]


def build_option_frame(
    end_of_day: pl.DataFrame | None,
    open_interest: pl.DataFrame | None,
    trade_date: date,
    requested_symbol: str,
) -> pl.DataFrame:
    if end_of_day is None or end_of_day.height == 0:
        return empty_option_frame()
    missing = [column for column in REQUIRED_EOD if column not in end_of_day.columns]
    if missing:
        raise RuntimeError(
            f"ThetaData EOD response for {requested_symbol} is missing {missing}: "
            f"{end_of_day.columns}"
        )
    priced = end_of_day.with_columns(
        pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
        _date_expr(end_of_day, "expiration"),
        pl.col("right").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("right"),
        pl.col("strike").cast(pl.Float64).round(4).alias("_strike_key"),
    )
    found = priced["symbol"].unique().to_list()
    if found != [requested_symbol]:
        raise RuntimeError(
            f"ThetaData EOD response for {requested_symbol} contains symbols {found}"
        )
    priced = priced.unique(
        subset=["symbol", "expiration", "_strike_key", "right"], keep="last"
    )
    interest = _kept_open_interest(open_interest, trade_date)
    if interest.height == 0:
        joined = priced.with_columns(pl.lit(0).cast(pl.Int64).alias("open_interest"))
    else:
        interest = (
            interest.sort("timestamp")
            .unique(
                subset=["symbol", "expiration", "_strike_key", "right"],
                keep="last",
            )
            .select(
                ["symbol", "expiration", "_strike_key", "right", "open_interest"]
            )
        )
        joined = priced.join(
            interest,
            on=["symbol", "expiration", "_strike_key", "right"],
            how="left",
        ).with_columns(pl.col("open_interest").fill_null(0).cast(pl.Int64))
    return joined.select(
        pl.col("symbol"),
        pl.col("expiration"),
        pl.col("strike").cast(pl.Float64),
        pl.col("right"),
        _utc_naive(end_of_day, "created"),
        _utc_naive(end_of_day, "last_trade"),
        _float_column(end_of_day, "open"),
        _float_column(end_of_day, "high"),
        _float_column(end_of_day, "low"),
        _float_column(end_of_day, "close"),
        pl.col("volume").cast(pl.Int64, strict=False).fill_null(0),
        _int_column(end_of_day, "count"),
        _int_column(end_of_day, "bid_size"),
        _int_column(end_of_day, "bid_exchange"),
        _float_column(end_of_day, "bid"),
        _int_column(end_of_day, "bid_condition"),
        _int_column(end_of_day, "ask_size"),
        _int_column(end_of_day, "ask_exchange"),
        _float_column(end_of_day, "ask"),
        _int_column(end_of_day, "ask_condition"),
        pl.col("open_interest"),
        pl.lit(trade_date).cast(pl.Date).alias("date"),
    )


def load_option_chain(
    client: object,
    gate: object,
    symbol: str,
    trade_date: date,
    oi_end_date: date,
    heartbeat: Callable[[], None] | None = None,
    as_of: date | None = None,
    job_name: str = OPTION_DATA_WORKFLOW,
) -> pl.DataFrame:
    from thetadata.errors import NoDataFoundError

    checked = require_option_symbol(symbol)
    with gate.lease(job_name, heartbeat=heartbeat):
        try:
            end_of_day = client.option_history_eod(
                trade_date, trade_date, checked, "*"
            )
        except NoDataFoundError:
            return empty_option_frame()
    if not isinstance(end_of_day, pl.DataFrame):
        raise RuntimeError(
            f"ThetaData EOD response for {checked} returned "
            f"{type(end_of_day).__name__}"
        )
    if end_of_day.height == 0:
        return empty_option_frame()
    with gate.lease(job_name, heartbeat=heartbeat):
        try:
            open_interest = request_open_interest(
                client, checked, oi_end_date, as_of=as_of
            )
        except NoDataFoundError:
            open_interest = None
    if open_interest is not None and not isinstance(open_interest, pl.DataFrame):
        raise RuntimeError(
            f"ThetaData open interest response for {checked} returned "
            f"{type(open_interest).__name__}"
        )
    return build_option_frame(end_of_day, open_interest, trade_date, checked)


def request_open_interest(
    client: object,
    symbol: str,
    oi_end_date: date,
    as_of: date | None = None,
) -> object:
    """Return the open-interest frame for one underlying.

    Wildcard history rejects the current calendar day, so a publication
    morning uses the current-day snapshot. That snapshot is the prior
    session until OPRA's next ~06:30 ET print.
    """
    checked = require_option_symbol(symbol)
    publication_day = as_of or datetime.now(EASTERN).date()
    if oi_end_date >= publication_day:
        return client.option_snapshot_open_interest(
            checked, "*", current_day_only=True
        )
    return client.option_history_open_interest(checked, "*", date=oi_end_date)


def open_interest_covers_session(
    frame: pl.DataFrame | None, trade_date: date
) -> bool:
    """True when the frame has a print published after the trade date."""
    return _kept_open_interest(frame, trade_date).height > 0


def frame_to_csv(frame: pl.DataFrame) -> bytes:
    text = frame.select(OPTION_COLUMNS).write_csv()
    if not isinstance(text, str):
        raise RuntimeError("polars write_csv did not return CSV text")
    return text.encode("utf-8")


def concat_checkpoint_csvs(bodies: list[bytes]) -> bytes:
    if not bodies:
        return frame_to_csv(empty_option_frame())
    chunks = [bodies[0] if bodies[0].endswith(b"\n") else bodies[0] + b"\n"]
    for body in bodies[1:]:
        newline = body.find(b"\n")
        if newline < 0:
            continue
        rest = body[newline + 1 :]
        if not rest:
            continue
        if not rest.endswith(b"\n"):
            rest += b"\n"
        chunks.append(rest)
    return b"".join(chunks)


def _symbol_column(frame: pl.DataFrame, label: str) -> pl.DataFrame:
    if "symbol" not in frame.columns:
        raise RuntimeError(f"{label} snapshot has no symbol column: {frame.columns}")
    return (
        frame.select(pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"))
        .filter(pl.col("symbol").is_not_null() & (pl.col("symbol").str.len_chars() > 0))
    )


def _date_expr(frame: pl.DataFrame, column: str) -> pl.Expr:
    dtype = frame.schema[column]
    if dtype == pl.String or dtype == pl.Utf8:
        return pl.col(column).str.to_date(strict=False).alias(column)
    return pl.col(column).cast(pl.Date, strict=False).alias(column)


def _float_column(frame: pl.DataFrame, column: str) -> pl.Expr:
    if column not in frame.columns:
        return pl.lit(None).cast(pl.Float64).alias(column)
    return pl.col(column).cast(pl.Float64, strict=False).alias(column)


def _int_column(frame: pl.DataFrame, column: str) -> pl.Expr:
    if column not in frame.columns:
        return pl.lit(None).cast(pl.Int64).alias(column)
    return pl.col(column).cast(pl.Int64, strict=False).alias(column)


def _utc_naive(frame: pl.DataFrame, column: str) -> pl.Expr:
    if column not in frame.columns:
        return pl.lit(None).cast(pl.Datetime("us")).alias(column)
    dtype = frame.schema[column]
    if isinstance(dtype, pl.Datetime) and dtype.time_zone:
        return (
            pl.col(column)
            .dt.convert_time_zone("UTC")
            .dt.replace_time_zone(None)
            .cast(pl.Datetime("us"))
            .alias(column)
        )
    if isinstance(dtype, pl.Datetime):
        return pl.col(column).cast(pl.Datetime("us")).alias(column)
    return pl.lit(None).cast(pl.Datetime("us")).alias(column)


def _kept_open_interest(
    frame: pl.DataFrame | None, trade_date: date
) -> pl.DataFrame:
    if (
        frame is None
        or frame.height == 0
        or "timestamp" not in frame.columns
        or "open_interest" not in frame.columns
    ):
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "expiration": pl.Date,
                "right": pl.Utf8,
                "_strike_key": pl.Float64,
                "timestamp": pl.Datetime("us", EASTERN.key),
                "open_interest": pl.Int64,
            }
        )
    dtype = frame.schema["timestamp"]
    if not isinstance(dtype, pl.Datetime):
        return _kept_open_interest(None, trade_date)
    timestamp = pl.col("timestamp")
    if dtype.time_zone:
        session_date = timestamp.dt.convert_time_zone(EASTERN.key).dt.date()
    else:
        session_date = (
            timestamp.dt.replace_time_zone("UTC")
            .dt.convert_time_zone(EASTERN.key)
            .dt.date()
        )
    kept = frame.filter(session_date > trade_date)
    if kept.height == 0:
        return _kept_open_interest(None, trade_date)
    return kept.with_columns(
        pl.col("symbol").cast(pl.Utf8).str.strip_chars().alias("symbol"),
        _date_expr(kept, "expiration"),
        pl.col("right").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("right"),
        pl.col("strike").cast(pl.Float64).round(4).alias("_strike_key"),
        pl.col("open_interest").cast(pl.Int64, strict=False).alias("open_interest"),
    )


@dataclass
class RangeSplit:
    frames: dict[date, pl.DataFrame]
    empty_days: list[date]
    unresolved: list[list[date]]


def is_transient(error: BaseException) -> bool:
    """True when the call should be retried without counting as a bad symbol."""
    name = type(error).__name__
    if name in {
        "ConnectionError",
        "TimeoutError",
        "ConnectionResetError",
        "OSError",
        "BrokenPipeError",
    }:
        return True
    module = type(error).__module__
    if module.startswith(("redis", "botocore", "urllib3")):
        return True
    if name != "RpcError":
        return False
    code = getattr(error, "code", None)
    status = code() if callable(code) else code
    status_name = getattr(status, "name", "")
    return status_name in {
        "UNAVAILABLE",
        "DEADLINE_EXCEEDED",
        "RESOURCE_EXHAUSTED",
        "ABORTED",
    }


def split_backfill_eod(
    sessions: list[date], frame: pl.DataFrame
) -> RangeSplit | None:
    """Split a multi-day EOD frame into settled sessions and an unresolved tail.

    Holes strictly between the first and last returned session are empty
    chains. Sessions before the first row, and the tail after the last row,
    stay unresolved so a truncated response is not stored as empty days.
    """
    if (
        not sessions
        or frame.height == 0
        or "created" not in frame.columns
    ):
        return None
    session_expr = _session_date_expr(frame, "created")
    if session_expr is None:
        return None
    labeled = frame.with_columns(session_expr.alias("_session"))
    requested = set(sessions)
    labeled = labeled.filter(
        pl.col("_session").is_not_null() & pl.col("_session").is_in(list(requested))
    )
    if labeled.height == 0:
        return None
    present = set(labeled["_session"].unique().to_list())
    first = min(present)
    last = max(present)
    frames: dict[date, pl.DataFrame] = {}
    empty_days: list[date] = []
    leading: list[date] = []
    tail: list[date] = []
    for day in sessions:
        if day < first:
            leading.append(day)
            continue
        if day > last:
            tail.append(day)
            continue
        if day in present:
            frames[day] = labeled.filter(pl.col("_session") == day).drop("_session")
        else:
            empty_days.append(day)
    unresolved = [run for run in (leading, tail) if run]
    return RangeSplit(frames=frames, empty_days=empty_days, unresolved=unresolved)


def open_interest_for_publication(
    frame: pl.DataFrame | None, publication: date
) -> pl.DataFrame | None:
    if (
        frame is None
        or frame.height == 0
        or "timestamp" not in frame.columns
    ):
        return None
    session_expr = _session_date_expr(frame, "timestamp")
    if session_expr is None:
        return None
    kept = frame.with_columns(session_expr.alias("_published")).filter(
        pl.col("_published") == publication
    )
    if kept.height == 0:
        return None
    return kept.drop("_published")


class _DataFailures:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.count = 0


def load_backfill_sessions(
    client: object,
    gate: object,
    symbol: str,
    sessions: list[date],
    publication: dict[date, date],
    *,
    job_name: str,
    heartbeat: Callable[[], None] | None = None,
    as_of: date | None = None,
    data_attempts: int = 5,
) -> dict[date, pl.DataFrame]:
    """Load every session in `sessions`, splitting ranges that come back short."""
    from thetadata.errors import NoDataFoundError
    from temporalio.exceptions import ApplicationError

    from thetadata_etl.datasets import BACKFILL_SYMBOL_FAILED

    failures = _DataFailures(data_attempts)
    loaded: dict[date, pl.DataFrame] = {}

    def note(error: BaseException) -> None:
        if is_transient(error):
            raise error
        failures.count += 1
        if failures.count >= failures.limit:
            raise ApplicationError(
                f"backfill failed for {symbol}: {error}",
                type=BACKFILL_SYMBOL_FAILED,
                non_retryable=True,
            ) from error

    def load(chunk: list[date]) -> None:
        if not chunk:
            return
        if len(chunk) == 1:
            day = chunk[0]
            while day not in loaded:
                try:
                    loaded[day] = load_option_chain(
                        client,
                        gate,
                        symbol,
                        day,
                        publication[day],
                        heartbeat,
                        as_of=as_of,
                        job_name=job_name,
                    )
                except ApplicationError:
                    raise
                except Exception as error:
                    note(error)
            return
        checked = require_option_symbol(symbol)
        try:
            with gate.lease(job_name, heartbeat=heartbeat):
                end_of_day = client.option_history_eod(
                    chunk[0], chunk[-1], checked, "*"
                )
        except NoDataFoundError:
            for day in chunk:
                loaded[day] = empty_option_frame()
            return
        except ApplicationError:
            raise
        except Exception as error:
            if is_transient(error):
                raise
            midpoint = len(chunk) // 2
            load(chunk[:midpoint])
            load(chunk[midpoint:])
            return
        if not isinstance(end_of_day, pl.DataFrame) or end_of_day.height == 0:
            midpoint = len(chunk) // 2
            load(chunk[:midpoint])
            load(chunk[midpoint:])
            return
        split = split_backfill_eod(chunk, end_of_day)
        if split is None:
            midpoint = len(chunk) // 2
            load(chunk[:midpoint])
            load(chunk[midpoint:])
            return
        try:
            interest = _range_open_interest(
                client,
                gate,
                checked,
                split.frames,
                publication,
                job_name=job_name,
                heartbeat=heartbeat,
                as_of=as_of,
            )
            for day, day_frame in split.frames.items():
                loaded[day] = build_option_frame(
                    day_frame,
                    open_interest_for_publication(interest, publication[day]),
                    day,
                    checked,
                )
        except ApplicationError:
            raise
        except Exception as error:
            note(error)
            load(chunk)
            return
        for day in split.empty_days:
            loaded[day] = empty_option_frame()
        for run in split.unresolved:
            load(run)

    load(sessions)
    missing = [day for day in sessions if day not in loaded]
    if missing:
        raise RuntimeError(
            f"backfill for {symbol} left {missing[0].isoformat()} unloaded"
        )
    return loaded


def _range_open_interest(
    client: object,
    gate: object,
    symbol: str,
    frames: dict[date, pl.DataFrame],
    publication: dict[date, date],
    *,
    job_name: str,
    heartbeat: Callable[[], None] | None,
    as_of: date | None,
) -> pl.DataFrame | None:
    from thetadata.errors import NoDataFoundError

    if not frames:
        return None
    publications = sorted({publication[day] for day in frames})
    history_as_of = as_of or (publications[-1] + timedelta(days=1))
    try:
        with gate.lease(job_name, heartbeat=heartbeat):
            if len(publications) == 1:
                fetched = request_open_interest(
                    client, symbol, publications[0], as_of=history_as_of
                )
            else:
                fetched = client.option_history_open_interest(
                    symbol,
                    "*",
                    start_date=publications[0],
                    end_date=publications[-1],
                )
    except NoDataFoundError:
        return None
    except Exception as error:
        if is_transient(error) or len(publications) == 1:
            raise
        pieces: list[pl.DataFrame] = []
        for published in publications:
            try:
                with gate.lease(job_name, heartbeat=heartbeat):
                    piece = request_open_interest(
                        client, symbol, published, as_of=history_as_of
                    )
            except NoDataFoundError:
                continue
            if isinstance(piece, pl.DataFrame) and piece.height > 0:
                pieces.append(piece)
        if not pieces:
            return None
        return pl.concat(pieces, how="diagonal_relaxed")
    if not isinstance(fetched, pl.DataFrame):
        raise RuntimeError(
            f"ThetaData open interest response for {symbol} returned "
            f"{type(fetched).__name__}"
        )
    if fetched.height == 0:
        return None
    return fetched


def _session_date_expr(frame: pl.DataFrame, column: str) -> pl.Expr | None:
    if column not in frame.columns:
        return None
    dtype = frame.schema[column]
    col = pl.col(column)
    if isinstance(dtype, pl.Datetime) and dtype.time_zone:
        return col.dt.convert_time_zone(EASTERN.key).dt.date()
    if isinstance(dtype, pl.Datetime):
        return col.dt.replace_time_zone(EASTERN.key).dt.date()
    if dtype == pl.String or dtype == pl.Utf8:
        return (
            col.str.to_datetime(strict=False)
            .dt.replace_time_zone(EASTERN.key)
            .dt.date()
        )
    return None
