from __future__ import annotations

from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from interest_rates_scraper.database import (
    Base,
    RateObservationRow,
    get_engine,
    persist_scrape,
)
from interest_rates_scraper.models import (
    PersistRequest,
    RateObservation,
    ScrapeResult,
    SourceFailure,
)


def _observation(rate: str, key: str = "account") -> RateObservation:
    return RateObservation(
        source="test_source",
        product_key=key,
        product_name=key.title(),
        product_type="apy",
        rate_percent=rate,
        source_url="https://example.test/rates",
        source_effective_date="2026-07-20",
    )


def _request(
    run_id: str,
    observations: list[RateObservation],
    failures: list[SourceFailure] | None = None,
) -> PersistRequest:
    return PersistRequest(
        workflow_id=f"workflow-{run_id}",
        workflow_run_id=run_id,
        observed_at=f"2026-07-{20 + int(run_id[-1])}T12:00:00+00:00",
        results=[
            ScrapeResult(source="test_source", observations=observations)
        ] if observations else [],
        failures=failures or [],
    )


def test_persistence_records_every_check_and_detects_changes(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'rates.db'}"
    get_engine.cache_clear()
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)

    first = persist_scrape(_request("run1", [_observation("3.4")]), database_url)
    assert first.baseline is True
    assert [change.change_type for change in first.changes] == ["added"]

    second = persist_scrape(_request("run2", [_observation("3.4")]), database_url)
    assert second.baseline is False
    assert second.changes == []

    third = persist_scrape(
        _request("run3", [_observation("3.5"), _observation("4.0", "cd")]),
        database_url,
    )
    assert {change.change_type for change in third.changes} == {"changed", "added"}

    fourth = persist_scrape(_request("run4", [_observation("3.5")]), database_url)
    assert [(change.product_key, change.change_type) for change in fourth.changes] == [
        ("cd", "removed")
    ]

    with Session(engine) as session:
        assert len(session.scalars(select(RateObservationRow)).all()) == 5


def test_persistence_is_idempotent_by_workflow_run_id(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'idempotent.db'}"
    get_engine.cache_clear()
    Base.metadata.create_all(get_engine(database_url))
    request = _request("run1", [_observation("3.4")])

    first = persist_scrape(request, database_url)
    repeated = persist_scrape(request, database_url)

    assert repeated == first
    with Session(get_engine(database_url)) as session:
        assert len(session.scalars(select(RateObservationRow)).all()) == 1
