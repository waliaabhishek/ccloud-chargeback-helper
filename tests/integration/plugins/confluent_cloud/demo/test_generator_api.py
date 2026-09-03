"""Real-storage and production-API coverage for the Clean Confluent demo."""

from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING, Any, NoReturn

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete

from core.api.app import create_app
from core.config.loader import load_config
from core.metrics.prometheus import PrometheusMetricsSource
from core.models.resource import CoreResource, ResourceStatus
from core.preview.evidence import PreviewEvidenceScope
from core.storage.backends.sqlmodel.base_tables import ResourceTable
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.registry import create_storage_backend
from demo.generator import GenerationResult, generate_or_reuse_clean_demo
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.demo.scenario import CleanDemoScenario, build_clean_demo_scenario
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository
from plugins.self_managed_kafka.demo.scenario import (
    CleanSelfManagedKafkaScenario,
    build_clean_self_managed_kafka_scenario,
)
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

if TYPE_CHECKING:
    from pytest import MonkeyPatch

    from core.models.billing import CoreBillingLineItem
    from core.models.chargeback import ChargebackRow
    from core.models.entity_tag import EntityTag
    from core.models.topic_attribution import TopicAttributionRow
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem


ECOSYSTEM = "confluent_cloud"
TENANT_NAME = "clean-confluent"
TENANT_ID = "northstar-confluent"
SELF_MANAGED_ECOSYSTEM = "self_managed_kafka"
SELF_MANAGED_TENANT_NAME = "clean-self-managed"
SELF_MANAGED_TENANT_ID = "northstar-self-managed"
ANCHOR_DATE = date(2026, 9, 2)
FUTURE_ANCHOR_DATE = date(2026, 10, 15)


def _generation_results(
    ccloud: GenerationResult,
    self_managed: GenerationResult,
) -> dict[str, GenerationResult]:
    return {TENANT_NAME: ccloud, SELF_MANAGED_TENANT_NAME: self_managed}


def _write_config(tmp_path: Path, ccloud_db_path: Path, self_managed_db_path: Path) -> Path:
    """Write the two-tenant API-only configuration consumed by production startup."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
api:
  host: "127.0.0.1"
  port: 8080
features:
  enable_periodic_refresh: false
preview:
  artifact_root: "{tmp_path / "focus-artifacts"}"
  max_workers: 1
tenants:
  {TENANT_NAME}:
    ecosystem: {ECOSYSTEM}
    tenant_id: {TENANT_ID}
    lookback_days: 200
    cutoff_days: 5
    storage:
      backend: sqlmodel
      connection_string: "sqlite:///{ccloud_db_path}"
    focus_preview:
      commercial_profile: direct_payg
      billing_currency: USD
      effective_start_date: 2000-01-01
      effective_end_date: 9999-12-31
    plugin_settings:
      ccloud_api:
        key: "demo-placeholder-key"
        secret: "demo-placeholder-secret"  # pragma: allowlist secret
      metrics:
        type: prometheus
        url: "http://prometheus.invalid:9090"
      topic_attribution:
        enabled: true
  {SELF_MANAGED_TENANT_NAME}:
    ecosystem: {SELF_MANAGED_ECOSYSTEM}
    tenant_id: {SELF_MANAGED_TENANT_ID}
    lookback_days: 200
    cutoff_days: 5
    storage:
      backend: sqlmodel
      connection_string: "sqlite:///{self_managed_db_path}"
    plugin_settings:
      cluster_id: northstar-logistics-kafka
      metrics_identifier: northstar-logistics-kafka
      broker_count: 3
      cost_model:
        compute_hourly_rate: 1.25
        storage_per_gib_hourly: 0.01
        network_ingress_per_gib: 0.02
        network_egress_per_gib: 0.04
      metrics:
        type: prometheus
        url: "http://prometheus.invalid:9090"
      topic_attribution:
        enabled: true
        compute_policy: shared_even_v1
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def _create_real_backend(config_path: Path, tenant_name: str = TENANT_NAME) -> SQLModelBackend:
    """Create a normal plugin-owned storage backend for direct persisted assertions."""
    settings = load_config(config_path)
    tenant_config = settings.tenants[tenant_name]
    storage_module = CCloudStorageModule() if tenant_config.ecosystem == ECOSYSTEM else SelfManagedKafkaStorageModule()
    backend = create_storage_backend(
        tenant_config.storage,
        storage_module=storage_module,
        use_migrations=False,
        focus_preview_enabled=tenant_config.focus_preview_enabled,
    )
    assert isinstance(backend, SQLModelBackend)
    backend.create_tables()
    return backend


def _tag_fields(tag: EntityTag) -> tuple[str, str, str, str, str, str]:
    """Return stable logical EntityTag fields, excluding database-assigned values."""
    return (
        tag.tenant_id,
        tag.entity_type,
        tag.entity_id,
        tag.tag_key,
        tag.tag_value,
        tag.created_by,
    )


def _billing_fields(line: CCloudBillingLineItem) -> tuple[object, ...]:
    return (
        line.timestamp,
        line.env_id,
        line.resource_id,
        line.product_category,
        line.product_type,
        line.quantity,
        line.unit_price,
        line.total_cost,
    )


def _chargeback_fields(row: ChargebackRow) -> tuple[object, ...]:
    return (
        row.timestamp,
        row.resource_id,
        row.identity_id,
        row.product_category,
        row.product_type,
        row.cost_type,
        row.amount,
        row.allocation_method,
        row.allocation_detail,
        row.metadata["env_id"],
    )


def _self_managed_billing_fields(line: CoreBillingLineItem) -> tuple[object, ...]:
    return (
        line.ecosystem,
        line.tenant_id,
        line.timestamp,
        line.resource_id,
        line.product_category,
        line.product_type,
        line.quantity,
        line.unit_price,
        line.total_cost,
        line.currency,
        line.granularity,
    )


def _self_managed_chargeback_fields(row: ChargebackRow) -> tuple[object, ...]:
    return (
        row.ecosystem,
        row.tenant_id,
        row.timestamp,
        row.resource_id,
        row.product_category,
        row.product_type,
        row.identity_id,
        row.cost_type,
        row.amount,
        row.allocation_method,
        row.allocation_detail,
        tuple(sorted(row.metadata.items())),
    )


def _topic_attribution_fields(row: TopicAttributionRow) -> tuple[object, ...]:
    return (
        row.ecosystem,
        row.tenant_id,
        row.timestamp,
        row.env_id,
        row.cluster_resource_id,
        row.topic_name,
        row.product_category,
        row.product_type,
        row.attribution_method,
        row.amount,
    )


def _snapshot_ccloud_preview_state(
    backend: SQLModelBackend,
    scenario: CleanDemoScenario,
) -> dict[str, object]:
    scope = PreviewEvidenceScope(
        ECOSYSTEM,
        scenario.tenant_id,
        datetime.combine(scenario.start_date, datetime.min.time(), tzinfo=UTC),
        datetime.combine(scenario.anchor_date + timedelta(days=1), datetime.min.time(), tzinfo=UTC),
    )
    refresh_token = f"clean-demo:{scenario.start_date.isoformat()}:{scenario.anchor_date.isoformat()}"
    calculation_ids = tuple(state.calculation_id for state in scenario.pipeline_states if state.calculation_id)
    with backend.create_preview_generation_read_unit_of_work() as uow:
        return {
            "source_attempt": uow.source_readiness.get_by_token(
                ECOSYSTEM,
                scenario.tenant_id,
                refresh_token,
            ),
            "source_readiness": tuple(
                uow.source_readiness.list_covering(
                    ECOSYSTEM,
                    scenario.tenant_id,
                    scope.start,
                    scope.end,
                )
            ),
            "native_sources": tuple(uow.cost_evidence.iter_preview_sources(scope)),
            "allocation_lineage": tuple(uow.allocation_evidence.iter_preview_allocation_runs(scope, calculation_ids)),
            "organization_authority": uow.organization_authority.get_latest(ECOSYSTEM, scenario.tenant_id),
        }


def _snapshot_persisted_state(
    backend: SQLModelBackend,
    preview_scenario: CleanDemoScenario | None = None,
) -> dict[str, object]:
    """Capture logical repository contents for no-write and fail-closed checks."""
    with backend.create_read_only_unit_of_work() as uow:
        tags, tag_total = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        billing, billing_total = uow.billing.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        chargebacks, chargeback_total = uow.chargebacks.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        topic_attributions, attribution_total = uow.topic_attributions.find_by_filters(
            ECOSYSTEM,
            TENANT_ID,
            limit=10_000,
        )
        states = uow.pipeline_state.find_by_range(ECOSYSTEM, TENANT_ID, date.min, date.max)
        latest_run = uow.pipeline_runs.get_latest_run(TENANT_NAME)
        return {
            "resource_counts": uow.resources.count_by_type(ECOSYSTEM, TENANT_ID),
            "identity_counts": uow.identities.count_by_type(ECOSYSTEM, TENANT_ID),
            "tags": tuple(sorted(_tag_fields(tag) for tag in tags)),
            "tag_total": tag_total,
            "billing": tuple(sorted(_billing_fields(line) for line in billing)),
            "billing_total": billing_total,
            "chargebacks": tuple(sorted(_chargeback_fields(row) for row in chargebacks)),
            "chargeback_total": chargeback_total,
            "topic_attributions": tuple(sorted(_topic_attribution_fields(row) for row in topic_attributions)),
            "topic_attribution_total": attribution_total,
            "pipeline_states": tuple(
                sorted(
                    (
                        state.tracking_date,
                        state.billing_gathered,
                        state.resources_gathered,
                        state.chargeback_calculated,
                        state.calculation_id,
                        state.calculation_completed_at,
                    )
                    for state in states
                )
            ),
            "pipeline_run": (
                None
                if latest_run is None
                else (
                    latest_run.id,
                    latest_run.tenant_name,
                    latest_run.started_at,
                    latest_run.ended_at,
                    latest_run.status,
                    latest_run.stage,
                    latest_run.current_date,
                    latest_run.dates_gathered,
                    latest_run.dates_calculated,
                    latest_run.rows_written,
                    latest_run.error_message,
                )
            ),
            "preview_evidence": (
                None if preview_scenario is None else _snapshot_ccloud_preview_state(backend, preview_scenario)
            ),
        }


def _snapshot_self_managed_state(backend: SQLModelBackend) -> dict[str, object]:
    """Capture logical self-managed repository contents for no-write reuse checks."""
    with backend.create_read_only_unit_of_work() as uow:
        tags, tag_total = uow.tags.find_tags_for_tenant(SELF_MANAGED_TENANT_ID, limit=10_000)
        billing, billing_total = uow.billing.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        chargebacks, chargeback_total = uow.chargebacks.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        topic_attributions, attribution_total = uow.topic_attributions.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        states = uow.pipeline_state.find_by_range(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            date.min,
            date.max,
        )
        latest_run = uow.pipeline_runs.get_latest_run(SELF_MANAGED_TENANT_NAME)
        return {
            "resource_counts": uow.resources.count_by_type(SELF_MANAGED_ECOSYSTEM, SELF_MANAGED_TENANT_ID),
            "identity_counts": uow.identities.count_by_type(SELF_MANAGED_ECOSYSTEM, SELF_MANAGED_TENANT_ID),
            "tags": tuple(sorted(_tag_fields(tag) for tag in tags)),
            "tag_total": tag_total,
            "billing": tuple(sorted(_self_managed_billing_fields(line) for line in billing)),
            "billing_total": billing_total,
            "chargebacks": tuple(sorted(_self_managed_chargeback_fields(row) for row in chargebacks)),
            "chargeback_total": chargeback_total,
            "topic_attributions": tuple(sorted(_topic_attribution_fields(row) for row in topic_attributions)),
            "topic_attribution_total": attribution_total,
            "pipeline_states": tuple(
                sorted(
                    (
                        state.tracking_date,
                        state.billing_gathered,
                        state.resources_gathered,
                        state.chargeback_calculated,
                        state.calculation_id,
                        state.calculation_completed_at,
                        state.calculation_run_id,
                        state.topic_overlay_gathered,
                        state.topic_attribution_calculated,
                    )
                    for state in states
                )
            ),
            "pipeline_run": (
                None
                if latest_run is None
                else (
                    latest_run.id,
                    latest_run.tenant_name,
                    latest_run.started_at,
                    latest_run.ended_at,
                    latest_run.status,
                    latest_run.stage,
                    latest_run.current_date,
                    latest_run.dates_gathered,
                    latest_run.dates_calculated,
                    latest_run.rows_written,
                    latest_run.error_message,
                )
            ),
        }


def _assert_empty(backend: SQLModelBackend) -> None:
    """Assert that no tenant-owned Clean data survived a rolled-back transaction."""
    snapshot = _snapshot_persisted_state(backend)
    assert snapshot["resource_counts"] == {}
    assert snapshot["identity_counts"] == {}
    assert snapshot["tag_total"] == 0
    assert snapshot["billing_total"] == 0
    assert snapshot["chargeback_total"] == 0
    assert snapshot["topic_attribution_total"] == 0
    assert snapshot["pipeline_states"] == ()
    assert snapshot["pipeline_run"] is None


def _assert_persisted_scenario(backend: SQLModelBackend, scenario: CleanDemoScenario) -> None:
    """Check the complete scenario through the real CCloud repositories and tag UoW."""
    with backend.create_read_only_unit_of_work() as uow:
        for resource in scenario.resources:
            assert uow.resources.get(ECOSYSTEM, TENANT_ID, resource.resource_id) == resource
        for identity in scenario.identities:
            assert uow.identities.get(ECOSYSTEM, TENANT_ID, identity.identity_id) == identity

        actual_tags, tag_total = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        assert tag_total == len(scenario.entity_tags)
        assert {_tag_fields(tag) for tag in actual_tags} == {_tag_fields(tag) for tag in scenario.entity_tags}

        actual_billing, billing_total = uow.billing.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        assert billing_total == len(scenario.billing_lines)
        assert {_billing_fields(line) for line in actual_billing} == {
            _billing_fields(line) for line in scenario.billing_lines
        }

        actual_chargebacks, chargeback_total = uow.chargebacks.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        assert chargeback_total == len(scenario.chargebacks)
        assert {_chargeback_fields(row) for row in actual_chargebacks} == {
            _chargeback_fields(row) for row in scenario.chargebacks
        }

        actual_topic_attributions, attribution_total = uow.topic_attributions.find_by_filters(
            ECOSYSTEM,
            TENANT_ID,
            limit=10_000,
        )
        assert attribution_total == len(scenario.topic_attributions)
        assert {_topic_attribution_fields(row) for row in actual_topic_attributions} == {
            _topic_attribution_fields(row) for row in scenario.topic_attributions
        }

        actual_states = uow.pipeline_state.find_by_range(
            ECOSYSTEM,
            TENANT_ID,
            scenario.start_date,
            scenario.anchor_date + timedelta(days=1),
        )
        latest_run = uow.pipeline_runs.get_latest_run(TENANT_NAME)
        assert latest_run is not None
        assert latest_run.status == "completed"
        assert latest_run.id is not None
        assert latest_run.id > 0
        assert latest_run.tenant_name == TENANT_NAME
        assert latest_run.started_at == datetime.combine(scenario.start_date, datetime.min.time(), tzinfo=UTC)
        assert latest_run.ended_at == datetime.combine(
            scenario.anchor_date,
            datetime.max.time().replace(microsecond=0),
            tzinfo=UTC,
        )
        assert latest_run.stage is None
        assert latest_run.current_date is None
        assert latest_run.dates_gathered == len(scenario.pipeline_states)
        assert latest_run.dates_calculated == len(scenario.pipeline_states)
        assert latest_run.rows_written == len(scenario.chargebacks)
        assert latest_run.error_message is None
        assert [replace(state, calculation_run_id=None) for state in actual_states] == list(scenario.pipeline_states)
        assert {state.calculation_run_id for state in actual_states} == {latest_run.id}

        actual_dates = uow.chargebacks.get_distinct_dates(ECOSYSTEM, TENANT_ID)
        assert actual_dates[0] == scenario.start_date
        assert actual_dates[-1] == scenario.anchor_date

    billing_totals: dict[tuple[object, ...], Decimal] = {}
    for billing in scenario.billing_lines:
        key = (
            billing.timestamp,
            billing.env_id,
            billing.resource_id,
            billing.product_category,
            billing.product_type,
        )
        billing_totals[key] = billing.total_cost

    allocated_totals: dict[tuple[object, ...], Decimal] = {}
    for chargeback in scenario.chargebacks:
        key = (
            chargeback.timestamp,
            chargeback.metadata["env_id"],
            chargeback.resource_id,
            chargeback.product_category,
            chargeback.product_type,
        )
        allocated_totals[key] = allocated_totals.get(key, Decimal("0")) + chargeback.amount
    assert allocated_totals == billing_totals


def _assert_persisted_self_managed_scenario(backend: SQLModelBackend, scenario: CleanSelfManagedKafkaScenario) -> None:
    """Check the complete self-managed scenario through its real plugin storage module."""
    with backend.create_read_only_unit_of_work() as uow:
        for resource in scenario.resources:
            assert uow.resources.get(SELF_MANAGED_ECOSYSTEM, SELF_MANAGED_TENANT_ID, resource.resource_id) == resource
        for identity in scenario.identities:
            assert uow.identities.get(SELF_MANAGED_ECOSYSTEM, SELF_MANAGED_TENANT_ID, identity.identity_id) == identity

        actual_tags, tag_total = uow.tags.find_tags_for_tenant(SELF_MANAGED_TENANT_ID, limit=10_000)
        assert tag_total == len(scenario.entity_tags)
        assert {_tag_fields(tag) for tag in actual_tags} == {_tag_fields(tag) for tag in scenario.entity_tags}

        actual_billing, billing_total = uow.billing.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        assert billing_total == len(scenario.billing_lines)
        assert {_self_managed_billing_fields(line) for line in actual_billing} == {
            _self_managed_billing_fields(line) for line in scenario.billing_lines
        }

        actual_chargebacks, chargeback_total = uow.chargebacks.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        assert chargeback_total == len(scenario.chargebacks)
        assert {_self_managed_chargeback_fields(row) for row in actual_chargebacks} == {
            _self_managed_chargeback_fields(row) for row in scenario.chargebacks
        }

        actual_topic_attributions, attribution_total = uow.topic_attributions.find_by_filters(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            limit=10_000,
        )
        assert attribution_total == len(scenario.topic_attributions)
        assert {_topic_attribution_fields(row) for row in actual_topic_attributions} == {
            _topic_attribution_fields(row) for row in scenario.topic_attributions
        }

        actual_states = uow.pipeline_state.find_by_range(
            SELF_MANAGED_ECOSYSTEM,
            SELF_MANAGED_TENANT_ID,
            scenario.start_date,
            scenario.anchor_date + timedelta(days=1),
        )
        latest_run = uow.pipeline_runs.get_latest_run(SELF_MANAGED_TENANT_NAME)
        assert latest_run is not None
        assert latest_run.status == "completed"
        assert latest_run.id is not None
        assert latest_run.id > 0
        assert latest_run.tenant_name == SELF_MANAGED_TENANT_NAME
        assert latest_run.started_at == datetime.combine(scenario.start_date, datetime.min.time(), tzinfo=UTC)
        assert latest_run.ended_at == datetime.combine(
            scenario.anchor_date,
            datetime.max.time().replace(microsecond=0),
            tzinfo=UTC,
        )
        assert latest_run.stage is None
        assert latest_run.current_date is None
        assert latest_run.dates_gathered == len(scenario.pipeline_states)
        assert latest_run.dates_calculated == len(scenario.pipeline_states)
        assert latest_run.rows_written == len(scenario.chargebacks)
        assert latest_run.error_message is None
        assert [replace(state, calculation_run_id=None) for state in actual_states] == list(scenario.pipeline_states)
        assert {state.calculation_run_id for state in actual_states} == {latest_run.id}


@pytest.mark.parametrize("schema_only", [False, True], ids=["new-database", "schema-only-database"])
def test_generator_persists_the_complete_clean_scenario_for_empty_database_states(
    tmp_path: Path,
    schema_only: bool,
) -> None:
    """Empty and schema-only CCloud databases generate one complete tagged scenario."""
    config_path = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "self-managed.db")
    if schema_only:
        backend = _create_real_backend(config_path)
        backend.dispose()

    result = generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE)

    assert result == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    backend = _create_real_backend(config_path)
    try:
        _assert_persisted_scenario(
            backend,
            build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE),
        )
    finally:
        backend.dispose()

    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        _assert_persisted_self_managed_scenario(
            self_managed_backend,
            build_clean_self_managed_kafka_scenario(tenant_id=SELF_MANAGED_TENANT_ID, anchor_date=ANCHOR_DATE),
        )
    finally:
        self_managed_backend.dispose()


@pytest.mark.parametrize("requested_anchor", [ANCHOR_DATE, ANCHOR_DATE + timedelta(days=1)])
def test_generator_reuses_a_complete_dataset_from_its_persisted_chargeback_anchor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    requested_anchor: date,
) -> None:
    """Same-day and next-day launches validate and reuse the maximum persisted fact date."""
    config_path = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "self-managed.db")
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )
    expected_ccloud_scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)

    backend = _create_real_backend(config_path)
    try:
        before = _snapshot_persisted_state(backend, expected_ccloud_scenario)
    finally:
        backend.dispose()
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        self_managed_backend.dispose()

    observed_date_queries: list[list[date]] = []
    original_get_distinct_dates = CCloudChargebackRepository.get_distinct_dates

    def record_distinct_dates(
        self: CCloudChargebackRepository,
        ecosystem: str,
        tenant_id: str,
    ) -> list[date]:
        dates = original_get_distinct_dates(self, ecosystem, tenant_id)
        observed_date_queries.append(dates)
        return dates

    monkeypatch.setattr(CCloudChargebackRepository, "get_distinct_dates", record_distinct_dates)
    result = generate_or_reuse_clean_demo(config_path=config_path, anchor_date=requested_anchor)

    assert result == _generation_results(GenerationResult.REUSED, GenerationResult.REUSED)
    assert observed_date_queries
    assert observed_date_queries[-1][-1] == ANCHOR_DATE

    backend = _create_real_backend(config_path)
    try:
        after = _snapshot_persisted_state(backend, expected_ccloud_scenario)
        assert after == before
        _assert_persisted_scenario(
            backend,
            expected_ccloud_scenario,
        )
    finally:
        backend.dispose()
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
        _assert_persisted_self_managed_scenario(
            self_managed_backend,
            build_clean_self_managed_kafka_scenario(tenant_id=SELF_MANAGED_TENANT_ID, anchor_date=ANCHOR_DATE),
        )
    finally:
        self_managed_backend.dispose()


def test_generator_reuses_the_complete_confluent_database_and_generates_an_empty_self_managed_database(
    tmp_path: Path,
) -> None:
    initial_config = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "first-self-managed.db")
    assert generate_or_reuse_clean_demo(config_path=initial_config, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )
    expected_ccloud_scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)
    ccloud_backend = _create_real_backend(initial_config)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend, expected_ccloud_scenario)
    finally:
        ccloud_backend.dispose()

    mixed_config = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "second-self-managed.db")
    result = generate_or_reuse_clean_demo(config_path=mixed_config, anchor_date=ANCHOR_DATE + timedelta(days=1))

    assert result == _generation_results(GenerationResult.REUSED, GenerationResult.GENERATED)
    ccloud_backend = _create_real_backend(mixed_config)
    try:
        assert _snapshot_persisted_state(ccloud_backend, expected_ccloud_scenario) == ccloud_before
    finally:
        ccloud_backend.dispose()
    self_managed_backend = _create_real_backend(mixed_config, SELF_MANAGED_TENANT_NAME)
    try:
        _assert_persisted_self_managed_scenario(
            self_managed_backend,
            build_clean_self_managed_kafka_scenario(
                tenant_id=SELF_MANAGED_TENANT_ID,
                anchor_date=ANCHOR_DATE + timedelta(days=1),
            ),
        )
    finally:
        self_managed_backend.dispose()


@pytest.mark.parametrize(
    "corruption",
    ["missing-resource", "unexpected-resource-type", "deleted-resource", "deleted-identity"],
)
def test_generator_rejects_missing_unexpected_or_deleted_persisted_topology_without_repairing_it(
    tmp_path: Path,
    corruption: str,
) -> None:
    config_path = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "self-managed.db")
    scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )
    backend = _create_real_backend(config_path)
    try:
        with backend.create_unit_of_work() as uow:
            if corruption == "missing-resource":
                resource = scenario.resources[0]
                uow.resources._session.execute(  # type: ignore[attr-defined]
                    delete(ResourceTable).where(
                        ResourceTable.ecosystem == ECOSYSTEM,
                        ResourceTable.tenant_id == TENANT_ID,
                        ResourceTable.resource_id == resource.resource_id,
                    )
                )
            elif corruption == "unexpected-resource-type":
                resource = scenario.resources[0]
                uow.resources.upsert(
                    replace(
                        resource,
                        resource_id="northstar-unexpected-resource",
                        resource_type="unexpected",
                        parent_id=None,
                    )
                )
            elif corruption == "deleted-resource":
                uow.resources.mark_deleted(
                    ECOSYSTEM,
                    TENANT_ID,
                    scenario.resources[0].resource_id,
                    datetime.combine(ANCHOR_DATE, datetime.min.time(), tzinfo=UTC),
                )
            else:
                uow.identities.mark_deleted(
                    ECOSYSTEM,
                    TENANT_ID,
                    scenario.identities[0].identity_id,
                    datetime.combine(ANCHOR_DATE, datetime.min.time(), tzinfo=UTC),
                )
            uow.commit()
        before = _snapshot_persisted_state(backend)
    finally:
        backend.dispose()

    with pytest.raises(ValueError, match="persisted Clean (resources|identities) do not match the expected topology"):
        generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE + timedelta(days=1))

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend) == before
    finally:
        backend.dispose()


@pytest.mark.parametrize(
    "corruption",
    ["missing-resource", "unexpected-resource-type", "deleted-resource", "deleted-identity"],
)
def test_generator_rejects_self_managed_topology_corruption_without_repairing_it(
    tmp_path: Path,
    corruption: str,
) -> None:
    config_path = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "self-managed.db")
    scenario = build_clean_self_managed_kafka_scenario(
        tenant_id=SELF_MANAGED_TENANT_ID,
        anchor_date=ANCHOR_DATE,
    )
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )
    backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        with backend.create_unit_of_work() as uow:
            if corruption == "missing-resource":
                resource = scenario.resources[0]
                uow.resources._session.execute(  # type: ignore[attr-defined]
                    delete(ResourceTable).where(
                        ResourceTable.ecosystem == SELF_MANAGED_ECOSYSTEM,
                        ResourceTable.tenant_id == SELF_MANAGED_TENANT_ID,
                        ResourceTable.resource_id == resource.resource_id,
                    )
                )
            elif corruption == "unexpected-resource-type":
                resource = scenario.resources[0]
                uow.resources.upsert(
                    replace(
                        resource,
                        resource_id="northstar-unexpected-resource",
                        resource_type="unexpected",
                        parent_id=None,
                    )
                )
            elif corruption == "deleted-resource":
                uow.resources.mark_deleted(
                    SELF_MANAGED_ECOSYSTEM,
                    SELF_MANAGED_TENANT_ID,
                    scenario.resources[0].resource_id,
                    datetime.combine(ANCHOR_DATE, datetime.min.time(), tzinfo=UTC),
                )
            else:
                uow.identities.mark_deleted(
                    SELF_MANAGED_ECOSYSTEM,
                    SELF_MANAGED_TENANT_ID,
                    scenario.identities[0].identity_id,
                    datetime.combine(ANCHOR_DATE, datetime.min.time(), tzinfo=UTC),
                )
            uow.commit()
        before = _snapshot_self_managed_state(backend)
    finally:
        backend.dispose()

    with pytest.raises(ValueError, match="persisted Clean (resources|identities) do not match the expected topology"):
        generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE + timedelta(days=1))

    backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_self_managed_state(backend) == before
    finally:
        backend.dispose()


@pytest.mark.parametrize("partial_state", ["resource", "tag"], ids=["resource-only", "tag-only"])
def test_generator_rejects_nonempty_partial_state_without_repairing_it(
    tmp_path: Path,
    partial_state: str,
) -> None:
    """Representative partial persisted state fails closed and remains unchanged."""
    config_path = _write_config(tmp_path, tmp_path / "partial-confluent.db", tmp_path / "partial-self-managed.db")
    backend = _create_real_backend(config_path)
    try:
        with backend.create_unit_of_work() as uow:
            if partial_state == "resource":
                uow.resources.upsert(
                    CoreResource(
                        ecosystem=ECOSYSTEM,
                        tenant_id=TENANT_ID,
                        resource_id="partial-resource",
                        resource_type="environment",
                        display_name="Partial",
                        status=ResourceStatus.ACTIVE,
                        created_at=datetime(2026, 3, 3, tzinfo=UTC),
                    )
                )
            else:
                uow.tags.add_tag(
                    TENANT_ID,
                    "resource",
                    "missing-resource",
                    "team",
                    "Platform",
                    "demo-generator",
                )
            uow.commit()
        before = _snapshot_persisted_state(backend)
    finally:
        backend.dispose()

    with pytest.raises(ValueError) as error:
        generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE)
    assert str(error.value) == "nonempty Clean state has no persisted chargeback dates"

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend) == before
    finally:
        backend.dispose()


def test_generator_rolls_back_a_repository_failure_and_a_retry_generates(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A pre-commit repository failure leaves schema-only state retryable."""
    config_path = _write_config(tmp_path, tmp_path / "rollback-confluent.db", tmp_path / "rollback-self-managed.db")

    def fail_tag_write(*_args: object, **_kwargs: object) -> NoReturn:
        raise OSError("simulated entity-tag persistence failure")

    with monkeypatch.context() as failing_patch:
        failing_patch.setattr(SQLModelEntityTagRepository, "add_tag", fail_tag_write)
        with pytest.raises(OSError, match="simulated entity-tag persistence failure"):
            generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE)

    backend = _create_real_backend(config_path)
    try:
        _assert_empty(backend)
    finally:
        backend.dispose()

    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )


def test_generator_does_not_report_success_when_preview_evidence_persistence_fails_and_rejects_incomplete_reuse(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    config_path = _write_config(tmp_path, tmp_path / "confluent.db", tmp_path / "self-managed.db")

    def fail_preview_evidence(
        self: CCloudNativeSourceEvidenceCapture,
        *_args: object,
        **_kwargs: object,
    ) -> NoReturn:
        raise OSError("simulated preview-evidence persistence failure")

    with monkeypatch.context() as failing_patch:
        failing_patch.setattr(CCloudNativeSourceEvidenceCapture, "persist", fail_preview_evidence)
        with pytest.raises(OSError, match="simulated preview-evidence persistence failure"):
            generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE)

    with pytest.raises(ValueError, match="preview evidence"):
        generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE + timedelta(days=1))


def _decimal_from_response(value: object) -> Decimal:
    return Decimal(str(value))


def _tag_values_by_entity(scenario: CleanDemoScenario) -> dict[tuple[str, str], str]:
    return {(tag.entity_type, tag.entity_id): tag.tag_value for tag in scenario.entity_tags}


def _assert_parent_edges_are_resolved(graph: dict[str, object]) -> None:
    nodes = graph["nodes"]
    edges = graph["edges"]
    assert isinstance(nodes, list)
    assert isinstance(edges, list)
    node_ids = {node["id"] for node in nodes}
    for edge in edges:
        if edge["relationship_type"] == "parent":
            assert edge["source"] in node_ids
            assert edge["target"] in node_ids


def _node(graph: dict[str, object], entity_id: str) -> dict[str, object]:
    nodes = graph["nodes"]
    assert isinstance(nodes, list)
    return next(node for node in nodes if node["id"] == entity_id)


def _parent_children(graph: dict[str, object], parent_id: str) -> set[str]:
    edges = graph["edges"]
    assert isinstance(edges, list)
    return {edge["target"] for edge in edges if edge["relationship_type"] == "parent" and edge["source"] == parent_id}


def test_generated_clean_data_is_available_through_the_production_api_without_provider_requests(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Production startup serves both generated tenants without provider or metrics acquisition."""
    config_path = _write_config(tmp_path, tmp_path / "api-confluent.db", tmp_path / "api-self-managed.db")
    scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)
    self_managed_scenario = build_clean_self_managed_kafka_scenario(
        tenant_id=SELF_MANAGED_TENANT_ID,
        anchor_date=ANCHOR_DATE,
    )
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )

    provider_requests: list[tuple[str, str]] = []

    def fail_provider_request(
        self: CCloudConnection,
        method: str,
        url: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        provider_requests.append((method, url))
        raise AssertionError(f"unexpected provider request: {method} {url}")

    monkeypatch.setattr(CCloudConnection, "_request", fail_provider_request)
    metrics_requests: list[object] = []

    def fail_metrics_query(self: PrometheusMetricsSource, *_args: object, **_kwargs: object) -> NoReturn:
        metrics_requests.append(self)
        raise AssertionError("unexpected Prometheus query")

    monkeypatch.setattr(PrometheusMetricsSource, "query", fail_metrics_query)
    import core.api.routes.readiness as readiness_routes

    monkeypatch.setattr(readiness_routes, "_readiness_cache", None)
    settings = load_config(config_path)
    period_params = {
        "start_date": scenario.start_date.isoformat(),
        "end_date": scenario.anchor_date.isoformat(),
    }
    api_prefix = f"/api/v1/tenants/{TENANT_NAME}"

    with TestClient(create_app(settings, mode="api")) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        readiness = client.get("/api/v1/readiness")
        assert readiness.status_code == 200
        assert readiness.json()["status"] == "ready"
        assert readiness.json()["mode"] == "api"

        tenants = client.get("/api/v1/tenants")
        assert tenants.status_code == 200
        tenant_summaries = tenants.json()["tenants"]
        assert [(item["tenant_name"], item["tenant_id"]) for item in tenant_summaries] == [
            (TENANT_NAME, TENANT_ID),
            (SELF_MANAGED_TENANT_NAME, SELF_MANAGED_TENANT_ID),
        ]
        assert [item["dates_calculated"] for item in tenant_summaries] == [
            len(scenario.pipeline_states),
            len(self_managed_scenario.pipeline_states),
        ]
        assert [item["last_calculated_date"] for item in tenant_summaries] == [
            scenario.anchor_date.isoformat(),
            self_managed_scenario.anchor_date.isoformat(),
        ]

        for tenant_name, tenant_scenario in (
            (TENANT_NAME, scenario),
            (SELF_MANAGED_TENANT_NAME, self_managed_scenario),
        ):
            inventory_prefix = f"/api/v1/tenants/{tenant_name}"
            resources = client.get(f"{inventory_prefix}/resources", params={"page_size": 1_000})
            assert resources.status_code == 200
            assert resources.json()["total"] == len(tenant_scenario.resources)
            assert Counter(item["resource_type"] for item in resources.json()["items"]) == Counter(
                resource.resource_type for resource in tenant_scenario.resources
            )

            identities = client.get(f"{inventory_prefix}/identities", params={"page_size": 1_000})
            assert identities.status_code == 200
            assert identities.json()["total"] == len(tenant_scenario.identities)
            assert Counter(item["identity_type"] for item in identities.json()["items"]) == Counter(
                identity.identity_type for identity in tenant_scenario.identities
            )

        dates = client.get(f"{api_prefix}/chargebacks/dates")
        assert dates.status_code == 200
        assert dates.json()["dates"][0] == scenario.start_date.isoformat()
        assert dates.json()["dates"][-1] == scenario.anchor_date.isoformat()

        aggregations: dict[str, dict[str, object]] = {}
        for group_by in ("identity_id", "product_type", "resource_id", "environment_id"):
            response = client.get(
                f"{api_prefix}/chargebacks/aggregate",
                params={**period_params, "group_by": group_by, "time_bucket": "day"},
            )
            assert response.status_code == 200
            data = response.json()
            assert data["buckets"]
            aggregations[group_by] = data

        total_response = aggregations["identity_id"]
        expected_total = sum((line.total_cost for line in scenario.billing_lines), Decimal("0"))
        assert _decimal_from_response(total_response["total_amount"]) == expected_total

        tagged = client.get(
            f"{api_prefix}/chargebacks/aggregate",
            params=[*period_params.items(), ("group_by", "tag:team"), ("time_bucket", "day")],
        )
        assert tagged.status_code == 200
        tagged_data = tagged.json()
        expected_teams = {tag.tag_value for tag in scenario.entity_tags}
        actual_teams = {bucket["dimensions"]["tag:team"] for bucket in tagged_data["buckets"]}
        assert actual_teams == expected_teams
        assert _decimal_from_response(tagged_data["total_amount"]) == expected_total

        root_response = client.get(f"{api_prefix}/graph", params=period_params)
        assert root_response.status_code == 200
        root_graph = root_response.json()
        root = _node(root_graph, TENANT_ID)
        assert root["resource_type"] == "tenant"
        assert root["tags"] == {}
        environment_nodes = [node for node in root_graph["nodes"] if node["resource_type"] == "environment"]
        assert len(environment_nodes) == 4
        assert _decimal_from_response(root["cost"]) == sum(
            (_decimal_from_response(node["cost"]) for node in environment_nodes), Decimal("0")
        )
        assert _decimal_from_response(root["cost"]) == expected_total
        _assert_parent_edges_are_resolved(root_graph)

        tag_values = _tag_values_by_entity(scenario)
        tagged_environment = next(
            resource.resource_id
            for resource in scenario.resources
            if resource.resource_type == "environment" and ("resource", resource.resource_id) in tag_values
        )
        environment = _node(root_graph, tagged_environment)
        assert environment["tags"] == {"team": tag_values[("resource", tagged_environment)]}

        tagged_resource = next(
            resource.resource_id
            for resource in scenario.resources
            if resource.resource_type != "environment" and ("resource", resource.resource_id) in tag_values
        )
        focused_resource = client.get(
            f"{api_prefix}/graph",
            params={
                **period_params,
                "focus": tagged_resource,
                "depth": 3,
                "at": f"{scenario.anchor_date.isoformat()}T00:00:00+00:00",
            },
        )
        assert focused_resource.status_code == 200
        focused_resource_graph = focused_resource.json()
        assert _node(focused_resource_graph, tagged_resource)["tags"] == {
            "team": tag_values[("resource", tagged_resource)]
        }
        _assert_parent_edges_are_resolved(focused_resource_graph)

        focused_environment = client.get(
            f"{api_prefix}/graph",
            params={
                **period_params,
                "focus": "env-commerce",
                "depth": 3,
                "at": f"{scenario.anchor_date.isoformat()}T00:00:00+00:00",
            },
        )
        assert focused_environment.status_code == 200
        focused_environment_graph = focused_environment.json()
        assert _node(focused_environment_graph, "env-commerce")["resource_type"] == "environment"
        assert {
            "lkc-commerce",
            "lsrc-commerce",
            "lksql-commerce",
        } <= _parent_children(focused_environment_graph, "env-commerce")
        _assert_parent_edges_are_resolved(focused_environment_graph)

        focused_cluster = client.get(
            f"{api_prefix}/graph",
            params={
                **period_params,
                "focus": "lkc-commerce",
                "depth": 3,
                "at": f"{scenario.anchor_date.isoformat()}T00:00:00+00:00",
            },
        )
        assert focused_cluster.status_code == 200
        focused_cluster_graph = focused_cluster.json()
        assert _node(focused_cluster_graph, "lkc-commerce")["resource_type"] == "kafka_cluster"
        assert {
            "lkc-commerce:topic:orders.created.v1",
            "lkc-commerce:topic:payments.authorized.v1",
            "lkc-commerce:topic:customer.profile.v1",
            "lcc-commerce-orders",
        } <= _parent_children(focused_cluster_graph, "lkc-commerce")
        _assert_parent_edges_are_resolved(focused_cluster_graph)

        tagged_identity = next(
            identity.identity_id for identity in scenario.identities if ("identity", identity.identity_id) in tag_values
        )
        focused_identity = client.get(
            f"{api_prefix}/graph",
            params={
                **period_params,
                "focus": tagged_identity,
                "at": f"{scenario.anchor_date.isoformat()}T00:00:00+00:00",
            },
        )
        assert focused_identity.status_code == 200
        assert _node(focused_identity.json(), tagged_identity)["tags"] == {
            "team": tag_values[("identity", tagged_identity)]
        }

        for tenant_name, tenant_id, tenant_scenario in (
            (TENANT_NAME, TENANT_ID, scenario),
            (SELF_MANAGED_TENANT_NAME, SELF_MANAGED_TENANT_ID, self_managed_scenario),
        ):
            tenant_prefix = f"/api/v1/tenants/{tenant_name}"
            tenant_period = {
                "start_date": tenant_scenario.start_date.isoformat(),
                "end_date": tenant_scenario.anchor_date.isoformat(),
            }
            tag_list = client.get(f"{tenant_prefix}/tags", params={"page_size": 1_000})
            assert tag_list.status_code == 200
            assert tag_list.json()["total"] == len(tenant_scenario.entity_tags)
            assert tag_list.json()["items"]
            tag_keys = client.get(f"{tenant_prefix}/tags/keys")
            assert tag_keys.status_code == 200
            assert tag_keys.json()["keys"] == ["team"]
            tag_values_response = client.get(f"{tenant_prefix}/tags/keys/team/values")
            assert tag_values_response.status_code == 200
            assert set(tag_values_response.json()["values"]) == {tag.tag_value for tag in tenant_scenario.entity_tags}

            resource_id = tenant_scenario.resources[0].resource_id
            tag_url = f"{tenant_prefix}/entities/resource/{resource_id}/tags/demo-note"
            created = client.post(
                tag_url.removesuffix("/demo-note"),
                json={"tag_key": "demo-note", "tag_value": "created", "created_by": "integration-test"},
            )
            assert created.status_code == 201
            assert created.json()["tag_value"] == "created"
            updated = client.put(tag_url, json={"tag_value": "updated"})
            assert updated.status_code == 200
            assert updated.json()["tag_value"] == "updated"
            fetched = client.get(tag_url.removesuffix("/demo-note"))
            assert fetched.status_code == 200
            assert {tag["tag_key"]: tag["tag_value"] for tag in fetched.json()}["demo-note"] == "updated"
            deleted = client.delete(tag_url)
            assert deleted.status_code == 204
            assert "demo-note" not in {tag["tag_key"] for tag in client.get(tag_url.removesuffix("/demo-note")).json()}

            invalid_body = client.post(tag_url.removesuffix("/demo-note"), json={})
            assert invalid_body.status_code == 422
            assert isinstance(invalid_body.json()["detail"], list)
            invalid_entity_type = client.post(
                f"{tenant_prefix}/entities/not-an-entity/{resource_id}/tags",
                json={"tag_key": "demo-note", "tag_value": "created", "created_by": "integration-test"},
            )
            assert invalid_entity_type.status_code == 422
            assert invalid_entity_type.json() == {"detail": "entity_type must be one of ['identity', 'resource']"}
            missing_entity = client.post(
                f"{tenant_prefix}/entities/resource/missing-resource/tags",
                json={"tag_key": "demo-note", "tag_value": "created", "created_by": "integration-test"},
            )
            assert missing_entity.status_code == 404
            assert missing_entity.json() == {"detail": "Resource missing-resource not found"}

            attribution_list = client.get(f"{tenant_prefix}/topic-attributions", params=tenant_period)
            assert attribution_list.status_code == 200
            assert attribution_list.json()["total"] == len(tenant_scenario.topic_attributions)
            assert attribution_list.json()["items"]
            attribution_aggregate = client.get(
                f"{tenant_prefix}/topic-attributions/aggregate",
                params={**tenant_period, "group_by": "topic_name", "time_bucket": "day"},
            )
            assert attribution_aggregate.status_code == 200
            assert attribution_aggregate.json()["total_rows"] == len(tenant_scenario.topic_attributions)
            assert attribution_aggregate.json()["buckets"]
            attribution_dates = client.get(f"{tenant_prefix}/topic-attributions/dates")
            assert attribution_dates.status_code == 200
            assert attribution_dates.json()["dates"][0] == tenant_scenario.start_date.isoformat()
            assert attribution_dates.json()["dates"][-1] == tenant_scenario.anchor_date.isoformat()
            attribution_export = client.post(f"{tenant_prefix}/topic-attributions/export", params=tenant_period)
            assert attribution_export.status_code == 200
            assert attribution_export.text.count("\n") > 1

            pipeline = client.get(f"{tenant_prefix}/pipeline/status")
            assert pipeline.status_code == 200
            assert pipeline.json()["is_running"] is False
            assert pipeline.json()["last_result"]["dates_calculated"] == len(tenant_scenario.pipeline_states)
            states = client.get(
                f"{tenant_prefix}/status",
                params={
                    "start_date": tenant_scenario.start_date.isoformat(),
                    "end_date": (tenant_scenario.anchor_date + timedelta(days=1)).isoformat(),
                },
            )
            assert states.status_code == 200
            assert len(states.json()["states"]) == len(tenant_scenario.pipeline_states)
            assert all(state["topic_overlay_gathered"] for state in states.json()["states"])
            assert all(state["topic_attribution_calculated"] for state in states.json()["states"])

            chargeback_export = client.post(
                f"{tenant_prefix}/export",
                json={**tenant_period, "columns": ["tenant_id", "timestamp", "resource_id", "amount"]},
            )
            assert chargeback_export.status_code == 200
            assert chargeback_export.text.startswith("tenant_id,timestamp,resource_id,amount")
            assert tenant_id in chargeback_export.text
            invalid_export = client.post(
                f"{tenant_prefix}/export",
                json={**tenant_period, "columns": ["not-a-column"]},
            )
            assert invalid_export.status_code == 400
            assert invalid_export.json() == {"detail": "Invalid columns: ['not-a-column']"}

        profile = client.get(f"{api_prefix}/focus-preview/profile")
        assert profile.status_code == 200
        assert profile.json()["target_focus_version"] == "1.4"
        unsupported_focus = client.get(f"/api/v1/tenants/{SELF_MANAGED_TENANT_NAME}/focus-preview/profile")
        assert unsupported_focus.status_code == 400
        assert unsupported_focus.json() == {
            "detail": "FOCUS Mapping Preview currently supports only Confluent Cloud tenants"
        }
        submitted_preview = client.post(
            f"{api_prefix}/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": scenario.start_date.isoformat(),
                "end_date": (scenario.start_date + timedelta(days=1)).isoformat(),
                "column_profile": "full",
            },
        )
        assert submitted_preview.status_code == 202
        request_id = submitted_preview.json()["request_id"]
        deadline = monotonic() + 5
        preview_status: dict[str, object] = submitted_preview.json()
        while monotonic() < deadline:
            response = client.get(f"{api_prefix}/focus-preview/requests/{request_id}")
            assert response.status_code == 200
            preview_status = response.json()
            if preview_status["status"] in {"ready", "failed"}:
                break
            sleep(0.01)
        assert preview_status["status"] == "ready"
        package = preview_status["package"]
        assert isinstance(package, dict)
        manifest = client.get(package["manifest"]["download_url"])
        data_file = client.get(package["files"][0]["download_url"])
        archive = client.get(package["download_all_url"])
        assert manifest.status_code == 200
        assert data_file.status_code == 200
        assert data_file.content.startswith(b"AllocatedMethodId,")
        assert archive.status_code == 200
        assert archive.content.startswith(b"PK")

    assert provider_requests == []
    assert metrics_requests == []


def test_future_anchor_keeps_focus_preview_available_without_provider_requests(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A future rolling demo anchor remains eligible for a real Preview request."""
    config_path = _write_config(tmp_path, tmp_path / "future-confluent.db", tmp_path / "future-self-managed.db")
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=FUTURE_ANCHOR_DATE) == _generation_results(
        GenerationResult.GENERATED,
        GenerationResult.GENERATED,
    )

    provider_requests: list[tuple[str, str]] = []

    def fail_provider_request(
        self: CCloudConnection,
        method: str,
        url: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        provider_requests.append((method, url))
        raise AssertionError(f"unexpected provider request: {method} {url}")

    monkeypatch.setattr(CCloudConnection, "_request", fail_provider_request)
    metrics_requests: list[object] = []

    def fail_metrics_query(self: PrometheusMetricsSource, *_args: object, **_kwargs: object) -> NoReturn:
        metrics_requests.append(self)
        raise AssertionError("unexpected Prometheus query")

    monkeypatch.setattr(PrometheusMetricsSource, "query", fail_metrics_query)
    import core.api.routes.readiness as readiness_routes

    monkeypatch.setattr(readiness_routes, "_readiness_cache", None)
    settings = load_config(config_path)
    api_prefix = f"/api/v1/tenants/{TENANT_NAME}"

    with TestClient(create_app(settings, mode="api")) as client:
        profile = client.get(f"{api_prefix}/focus-preview/profile")
        assert profile.status_code == 200
        assert profile.json()["target_focus_version"] == "1.4"

        submitted = client.post(
            f"{api_prefix}/focus-preview/requests",
            json={
                "grain": "daily",
                "start_date": "2026-10-02",
                "end_date": "2026-10-03",
                "column_profile": "full",
            },
        )
        assert submitted.status_code == 202
        request_id = submitted.json()["request_id"]
        deadline = monotonic() + 5
        preview_status: dict[str, object] = submitted.json()
        while monotonic() < deadline:
            response = client.get(f"{api_prefix}/focus-preview/requests/{request_id}")
            assert response.status_code == 200
            preview_status = response.json()
            if preview_status["status"] in {"ready", "failed"}:
                break
            sleep(0.01)
        assert preview_status["status"] == "ready"

    assert provider_requests == []
    assert metrics_requests == []
