from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    MetaData,
    select,
    text,
    update,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from interest_rates_scraper.config import Settings
from interest_rates_scraper.models import (
    PersistRequest,
    PersistResult,
    RateChange,
    RateObservation,
)

DB_SCHEMA = "interest_rates"

class Base(DeclarativeBase):
    metadata = MetaData(schema=DB_SCHEMA)


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    workflow_run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(255), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    baseline: Mapped[bool] = mapped_column(nullable=False, default=False)
    change_payload: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list
    )
    notification_status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="not_required"
    )


class SourceRun(Base):
    __tablename__ = "source_runs"
    __table_args__ = (
        UniqueConstraint("workflow_run_id", "source", name="uq_source_run"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    workflow_run_id: Mapped[str] = mapped_column(
        ForeignKey("scrape_runs.workflow_run_id", ondelete="CASCADE"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error: Mapped[str | None] = mapped_column(Text)
    source_effective_date: Mapped[date | None] = mapped_column(Date)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source", "product_key", name="uq_product_source_key"),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    product_key: Mapped[str] = mapped_column(String(255), nullable=False)
    product_name: Mapped[str] = mapped_column(String(255), nullable=False)
    product_type: Mapped[str] = mapped_column(String(128), nullable=False)
    term_months: Mapped[int | None] = mapped_column(Integer)
    product_metadata: Mapped[dict[str, str]] = mapped_column(
        "metadata", JSON, nullable=False, default=dict
    )


class RateObservationRow(Base):
    __tablename__ = "rate_observations"
    __table_args__ = (
        UniqueConstraint(
            "workflow_run_id", "product_id", name="uq_observation_run_product"
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    workflow_run_id: Mapped[str] = mapped_column(
        ForeignKey("scrape_runs.workflow_run_id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        ForeignKey("products.id", ondelete="RESTRICT"), nullable=False
    )
    rate_percent: Mapped[Decimal] = mapped_column(Numeric(9, 5), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_effective_date: Mapped[date | None] = mapped_column(Date)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    observation_metadata: Mapped[dict[str, str]] = mapped_column(
        "metadata", JSON, nullable=False, default=dict
    )


@lru_cache(maxsize=4)
def get_engine(database_url: str):
    engine = create_engine(database_url, pool_pre_ping=True)
    if engine.dialect.name == "sqlite":
        engine = engine.execution_options(schema_translate_map={DB_SCHEMA: None})
    return engine


def _as_datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return result if result.tzinfo else result.replace(tzinfo=timezone.utc)


def _as_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _rate_string(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _serialize_change(change: RateChange) -> dict[str, Any]:
    return {
        "source": change.source,
        "product_key": change.product_key,
        "product_name": change.product_name,
        "change_type": change.change_type,
        "old_rate_percent": change.old_rate_percent,
        "new_rate_percent": change.new_rate_percent,
    }


def _deserialize_change(value: dict[str, Any]) -> RateChange:
    return RateChange(**value)


def _previous_snapshot(
    session: Session, source: str, current_run_id: str
) -> dict[str, tuple[Product, Decimal]]:
    previous_run_id = session.execute(
        select(SourceRun.workflow_run_id)
        .where(
            SourceRun.source == source,
            SourceRun.status == "success",
            SourceRun.workflow_run_id != current_run_id,
        )
        .order_by(SourceRun.observed_at.desc(), SourceRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if previous_run_id is None:
        return {}

    rows = session.execute(
        select(Product, RateObservationRow.rate_percent)
        .join(RateObservationRow, RateObservationRow.product_id == Product.id)
        .where(RateObservationRow.workflow_run_id == previous_run_id)
    ).all()
    return {product.product_key: (product, rate) for product, rate in rows}


def _get_or_create_product(
    session: Session, observation: RateObservation
) -> Product:
    product = session.execute(
        select(Product).where(
            Product.source == observation.source,
            Product.product_key == observation.product_key,
        )
    ).scalar_one_or_none()
    if product is None:
        product = Product(
            source=observation.source,
            product_key=observation.product_key,
            product_name=observation.product_name,
            product_type=observation.product_type,
            term_months=observation.term_months,
            product_metadata=observation.metadata,
        )
        session.add(product)
        session.flush()
    else:
        product.product_name = observation.product_name
        product.product_type = observation.product_type
        product.term_months = observation.term_months
        product.product_metadata = observation.metadata
    return product


def persist_scrape(request: PersistRequest, database_url: str | None = None) -> PersistResult:
    database_url = database_url or Settings.from_env().database_url
    observed_at = _as_datetime(request.observed_at)
    engine = get_engine(database_url)

    with Session(engine) as session, session.begin():
        if engine.dialect.name == "postgresql":
            session.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtext('finance_tools_interest_rate_persistence'))"
                )
            )

        existing = session.get(ScrapeRun, request.workflow_run_id)
        if existing is not None:
            return PersistResult(
                changes=[
                    _deserialize_change(value) for value in existing.change_payload
                ],
                baseline=existing.baseline,
            )

        run = ScrapeRun(
            workflow_run_id=request.workflow_run_id,
            workflow_id=request.workflow_id,
            observed_at=observed_at,
        )
        session.add(run)
        session.flush()

        changes: list[RateChange] = []
        any_previous_snapshot = False

        for result in sorted(request.results, key=lambda item: item.source):
            previous = _previous_snapshot(
                session, result.source, request.workflow_run_id
            )
            any_previous_snapshot = any_previous_snapshot or bool(previous)
            current = {item.product_key: item for item in result.observations}

            session.add(
                SourceRun(
                    workflow_run_id=request.workflow_run_id,
                    source=result.source,
                    status="success",
                    source_effective_date=_as_date(result.source_effective_date),
                    observed_at=observed_at,
                )
            )
            for product_key, observation in sorted(current.items()):
                product = _get_or_create_product(session, observation)
                new_rate = Decimal(observation.rate_percent)
                old = previous.get(product_key)
                if old is None:
                    changes.append(
                        RateChange(
                            source=result.source,
                            product_key=product_key,
                            product_name=observation.product_name,
                            change_type="added",
                            old_rate_percent=None,
                            new_rate_percent=observation.rate_percent,
                        )
                    )
                elif old[1] != new_rate:
                    changes.append(
                        RateChange(
                            source=result.source,
                            product_key=product_key,
                            product_name=observation.product_name,
                            change_type="changed",
                            old_rate_percent=_rate_string(old[1]),
                            new_rate_percent=observation.rate_percent,
                        )
                    )
                session.add(
                    RateObservationRow(
                        workflow_run_id=request.workflow_run_id,
                        product_id=product.id,
                        rate_percent=new_rate,
                        observed_at=observed_at,
                        source_effective_date=_as_date(
                            observation.source_effective_date
                        ),
                        source_url=observation.source_url,
                        observation_metadata=observation.metadata,
                    )
                )

            for product_key in sorted(previous.keys() - current.keys()):
                previous_product, previous_rate = previous[product_key]
                changes.append(
                    RateChange(
                        source=result.source,
                        product_key=product_key,
                        product_name=previous_product.product_name,
                        change_type="removed",
                        old_rate_percent=_rate_string(previous_rate),
                        new_rate_percent=None,
                    )
                )

        for failure in sorted(request.failures, key=lambda item: item.source):
            session.add(
                SourceRun(
                    workflow_run_id=request.workflow_run_id,
                    source=failure.source,
                    status="failed",
                    error=failure.error[:4000],
                    observed_at=observed_at,
                )
            )

        run.baseline = not any_previous_snapshot
        run.change_payload = [_serialize_change(change) for change in changes]
        run.notification_status = (
            "pending" if changes or request.failures else "not_required"
        )
        return PersistResult(changes=changes, baseline=run.baseline)


def mark_notification_sent(
    workflow_run_id: str, database_url: str | None = None
) -> None:
    database_url = database_url or Settings.from_env().database_url
    with Session(get_engine(database_url)) as session, session.begin():
        session.execute(
            update(ScrapeRun)
            .where(ScrapeRun.workflow_run_id == workflow_run_id)
            .values(notification_status="sent")
        )
