from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from threading import Event, Thread
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from core.models.chargeback import ChargebackRow, CostType
from core.models.pipeline import PipelineState
from core.models.topic_attribution import TopicAttributionRow
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

_ECO = "eco"
_TENANT = "tenant"
_DAY = date(2026, 2, 1)
_START = datetime(2026, 2, 1, tzinfo=UTC)
_END = datetime(2026, 2, 2, tzinfo=UTC)
_COMPARISON_DAY = date(2026, 2, 2)
_COMPARISON_START = datetime(2026, 2, 2, tzinfo=UTC)
_COMPARISON_END = datetime(2026, 2, 3, tzinfo=UTC)


def _chargeback(amount: str, *, timestamp: datetime = _START) -> ChargebackRow:
    return ChargebackRow(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        timestamp=timestamp,
        resource_id="resource-1",
        product_category="kafka",
        product_type="KAFKA_BASE",
        identity_id="alice",
        cost_type=CostType.USAGE,
        amount=Decimal(amount),
    )


def _state(*, usable: bool, tracking_date: date = _DAY) -> PipelineState:
    return PipelineState(
        ecosystem=_ECO,
        tenant_id=_TENANT,
        tracking_date=tracking_date,
        chargeback_calculated=usable,
        calculation_id="calculation" if usable else None,
        calculation_completed_at=datetime(
            tracking_date.year,
            tracking_date.month,
            tracking_date.day,
            tzinfo=UTC,
        )
        if usable
        else None,
        topic_overlay_gathered=usable,
        topic_attribution_calculated=usable,
    )


def _topic(
    timestamp: datetime,
    *,
    tenant_id: str = _TENANT,
    amount: str = "1",
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=_ECO,
        tenant_id=tenant_id,
        timestamp=timestamp,
        env_id="env-1",
        cluster_resource_id="cluster-a",
        topic_name="orders",
        product_category="kafka",
        product_type="KAFKA_BASE",
        attribution_method="bytes_ratio",
        amount=Decimal(amount),
    )


def _seed(backend: SQLModelBackend) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.upsert(_chargeback("1"))
        uow.chargebacks.upsert(_chargeback("10", timestamp=_COMPARISON_START))
        uow.topic_attributions.upsert_batch(
            [
                _topic(_START, amount="100"),
                _topic(_COMPARISON_START, amount="110"),
            ]
        )
        uow.pipeline_state.upsert(_state(usable=True))
        uow.pipeline_state.upsert(_state(usable=True, tracking_date=_COMPARISON_DAY))
        uow.commit()


def _assert_snapshot_keeps_one_generation(backend: SQLModelBackend) -> None:
    from core.storage.interface import ConsistentReadStorageBackend

    assert isinstance(backend, ConsistentReadStorageBackend)
    initial_read_pinned = Event()
    writer_committed = Event()
    writer_failures: list[BaseException] = []

    def write_new_generation() -> None:
        try:
            assert initial_read_pinned.wait(timeout=10)
            with backend.create_unit_of_work() as writer:
                writer.chargebacks.upsert(_chargeback("2"))
                writer.chargebacks.upsert(_chargeback("20", timestamp=_COMPARISON_START))
                writer.topic_attributions.upsert_batch(
                    [
                        _topic(_START, amount="200"),
                        _topic(_COMPARISON_START, amount="220"),
                        _topic(datetime(2026, 2, 1, 12, tzinfo=UTC), amount="30"),
                    ]
                )
                writer.pipeline_state.upsert(_state(usable=False))
                writer.pipeline_state.upsert(_state(usable=False, tracking_date=_COMPARISON_DAY))
                writer.commit()
        except BaseException as exc:
            writer_failures.append(exc)
        finally:
            writer_committed.set()

    writer_thread = Thread(target=write_new_generation)
    writer_thread.start()
    try:
        with backend.create_consistent_read_unit_of_work() as reader:
            before_states = reader.pipeline_state.find_by_range(_ECO, _TENANT, _DAY, date(2026, 2, 3))
            initial_read_pinned.set()
            assert writer_committed.wait(timeout=10)
            assert writer_failures == []
            before_rows = list(reader.chargebacks.iter_by_filters(_ECO, _TENANT, _START, _END))
            before_comparison_rows = list(
                reader.chargebacks.iter_by_filters(_ECO, _TENANT, _COMPARISON_START, _COMPARISON_END)
            )
            before_topic_rows = list(reader.topic_attributions.iter_by_filters(_ECO, _TENANT, _START, _END))
            before_topic_comparison_rows = list(
                reader.topic_attributions.iter_by_filters(
                    _ECO,
                    _TENANT,
                    _COMPARISON_START,
                    _COMPARISON_END,
                )
            )
            before_topic_timestamps = reader.topic_attributions.get_distinct_timestamps_in_range(
                _ECO,
                _TENANT,
                _START,
                _END,
            )
            assert [state.has_usable_calculation for state in before_states] == [True, True]
            assert [row.amount for row in before_rows] == [Decimal("1")]
            assert [row.amount for row in before_comparison_rows] == [Decimal("10")]
            assert [row.amount for row in before_topic_rows] == [Decimal("100")]
            assert [row.amount for row in before_topic_comparison_rows] == [Decimal("110")]
            assert before_topic_timestamps == {_START}
            with pytest.raises(SQLAlchemyError, match=r"read.?only"):
                reader.chargebacks.upsert(_chargeback("3"))
                assert reader._session is not None
                reader._session.flush()
            with pytest.raises(RuntimeError, match="read-only"):
                reader.commit()
    finally:
        writer_thread.join(timeout=10)

    assert not writer_thread.is_alive()
    assert writer_failures == []
    with backend.create_consistent_read_unit_of_work() as after_reader:
        after_states = after_reader.pipeline_state.find_by_range(_ECO, _TENANT, _DAY, date(2026, 2, 3))
        after_rows = list(after_reader.chargebacks.iter_by_filters(_ECO, _TENANT, _START, _END))
        after_comparison_rows = list(
            after_reader.chargebacks.iter_by_filters(_ECO, _TENANT, _COMPARISON_START, _COMPARISON_END)
        )
        after_topic_rows = list(after_reader.topic_attributions.iter_by_filters(_ECO, _TENANT, _START, _END))
        after_topic_comparison_rows = list(
            after_reader.topic_attributions.iter_by_filters(
                _ECO,
                _TENANT,
                _COMPARISON_START,
                _COMPARISON_END,
            )
        )
        after_topic_timestamps = after_reader.topic_attributions.get_distinct_timestamps_in_range(
            _ECO,
            _TENANT,
            _START,
            _END,
        )
    assert [state.has_usable_calculation for state in after_states] == [False, False]
    assert [row.amount for row in after_rows] == [Decimal("2")]
    assert [row.amount for row in after_comparison_rows] == [Decimal("20")]
    assert {row.timestamp: row.amount for row in after_topic_rows} == {
        _START: Decimal("200"),
        datetime(2026, 2, 1, 12, tzinfo=UTC): Decimal("30"),
    }
    assert [row.amount for row in after_topic_comparison_rows] == [Decimal("220")]
    assert after_topic_timestamps == {
        _START,
        datetime(2026, 2, 1, 12, tzinfo=UTC),
    }


def test_sqlite_wal_consistent_read_keeps_coverage_and_fact_streams_on_one_snapshot(tmp_path: Path) -> None:
    database_url = f"sqlite:///{tmp_path / 'snapshot.db'}"
    backend = SQLModelBackend(database_url, CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        _seed(backend)
        _assert_snapshot_keeps_one_generation(backend)
    finally:
        engine.dispose()
        backend.dispose()


def test_consistent_read_closes_after_lazy_stream_failure_and_allows_a_fresh_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from core.storage.backends.sqlmodel.repositories import TopicAttributionRepository
    from core.storage.interface import ConsistentReadStorageBackend

    database_url = f"sqlite:///{tmp_path / 'failure.db'}"
    backend = SQLModelBackend(database_url, CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    _seed(backend)
    try:
        assert isinstance(backend, ConsistentReadStorageBackend)
        original_iter = TopicAttributionRepository.iter_by_filters

        def failing_iter(
            self: TopicAttributionRepository,
            *args: Any,
            **kwargs: Any,
        ) -> Iterator[TopicAttributionRow]:
            yield from original_iter(self, *args, **kwargs)
            raise ValueError("stream failure")

        monkeypatch.setattr(TopicAttributionRepository, "iter_by_filters", failing_iter)
        failed_reader = backend.create_consistent_read_unit_of_work()
        with pytest.raises(ValueError, match="stream failure"), failed_reader as reader:
            list(reader.topic_attributions.iter_by_filters(_ECO, _TENANT, _START, _END))
        assert failed_reader._session is None

        monkeypatch.undo()

        with backend.create_consistent_read_unit_of_work() as fresh_reader:
            rows = list(fresh_reader.topic_attributions.iter_by_filters(_ECO, _TENANT, _START, _END))
        assert [row.amount for row in rows] == [Decimal("100")]
    finally:
        backend.dispose()


def test_topic_timestamp_availability_is_tenant_scoped_and_range_bounded(tmp_path: Path) -> None:
    from core.storage.interface import ConsistentReadStorageBackend

    database_url = f"sqlite:///{tmp_path / 'availability.db'}"
    backend = SQLModelBackend(database_url, CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    try:
        with backend.create_unit_of_work() as writer:
            writer.topic_attributions.upsert_batch(
                [
                    _topic(datetime(2026, 2, 1, tzinfo=UTC)),
                    _topic(datetime(2026, 2, 1, 23, 59, tzinfo=UTC)),
                    _topic(datetime(2026, 2, 2, tzinfo=UTC)),
                    _topic(datetime(2026, 2, 1, tzinfo=UTC), tenant_id="other-tenant"),
                ]
            )
            writer.commit()

        assert isinstance(backend, ConsistentReadStorageBackend)
        with backend.create_consistent_read_unit_of_work() as reader:
            timestamps = reader.topic_attributions.get_distinct_timestamps_in_range(
                _ECO,
                _TENANT,
                _START,
                _END,
            )
        assert timestamps == {
            datetime(2026, 2, 1, tzinfo=UTC),
            datetime(2026, 2, 1, 23, 59, tzinfo=UTC),
        }
    finally:
        backend.dispose()


_POSTGRES_URL = os.environ.get("TEST_POSTGRESQL_URL")


@pytest.fixture
def postgresql_schema_url() -> Iterator[str]:
    if _POSTGRES_URL is None:
        pytest.skip("TEST_POSTGRESQL_URL is not configured")
    schema = f"consistent_read_{uuid4().hex}"
    admin_engine = create_engine(_POSTGRES_URL)
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    schema_url = (
        make_url(_POSTGRES_URL)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    try:
        yield schema_url
    finally:
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        admin_engine.dispose()


def test_postgresql_repeatable_read_is_read_only_before_first_repository_select(
    postgresql_schema_url: str,
) -> None:
    from core.storage.interface import ConsistentReadStorageBackend

    backend = SQLModelBackend(postgresql_schema_url, CoreStorageModule(), use_migrations=False)
    backend.create_tables()
    try:
        _seed(backend)
        assert isinstance(backend, ConsistentReadStorageBackend)
        with backend.create_consistent_read_unit_of_work() as reader:
            assert reader._session is not None
            read_only = reader._session.execute(text("SHOW transaction_read_only")).scalar_one()
            assert read_only == "on"
            assert reader.pipeline_state.find_by_range(_ECO, _TENANT, _DAY, date(2026, 2, 2))
        _assert_snapshot_keeps_one_generation(backend)
    finally:
        backend.dispose()
