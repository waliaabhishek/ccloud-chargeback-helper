"""Real-storage and production-API coverage for the Clean Confluent demo."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING, Any, NoReturn, cast

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, inspect

from core.api.app import create_app
from core.config.loader import load_config
from core.metrics.prometheus import PrometheusMetricsSource
from core.models.resource import CoreResource, ResourceStatus
from core.preview.evidence import PreviewEvidenceScope
from core.storage.backends.sqlmodel.base_tables import ResourceTable
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository, TopicAttributionRepository
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.registry import create_storage_backend
from demo.generator import DemoProfile, GenerationResult, generate_or_reuse_clean_demo, generate_or_reuse_demo
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.demo.scenario import (
    CleanDemoScenario,
    ConfluentDemoScenario,
    build_clean_demo_scenario,
    build_showcase_demo_scenario,
)
from plugins.confluent_cloud.source_capture import CCloudNativeSourceEvidenceCapture
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.confluent_cloud.storage.repositories import CCloudBillingRepository, CCloudChargebackRepository
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
    from core.storage.interface import EntityTagRepository
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem


ECOSYSTEM = "confluent_cloud"
TENANT_NAME = "clean-confluent"
TENANT_ID = "northstar-confluent"
SELF_MANAGED_ECOSYSTEM = "self_managed_kafka"
SELF_MANAGED_TENANT_NAME = "clean-self-managed"
SELF_MANAGED_TENANT_ID = "northstar-self-managed"
ANCHOR_DATE = date(2026, 9, 2)
FUTURE_ANCHOR_DATE = date(2026, 10, 15)


@dataclass(frozen=True)
class _DemoStateTemplate:
    state_dir: Path
    profile: DemoProfile
    anchor_date: date
    ccloud_scenario: ConfluentDemoScenario
    self_managed_scenario: CleanSelfManagedKafkaScenario


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


def _assert_empty_self_managed(backend: SQLModelBackend) -> None:
    """Assert that no tenant-owned self-managed data survived a failed generation."""
    snapshot = _snapshot_self_managed_state(backend)
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
    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    if schema_only:
        backend = _create_real_backend(config_path)
        backend.dispose()

    result = generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=ANCHOR_DATE)

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
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    """Same-day and next-day launches validate and reuse the maximum persisted fact date."""
    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    expected_ccloud_scenario = clean_demo_state_template.ccloud_scenario

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
    result = generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=requested_anchor)

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
            clean_demo_state_template.self_managed_scenario,
        )
    finally:
        self_managed_backend.dispose()


def test_generator_rejects_a_manifested_state_with_an_empty_replacement_tenant_database(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    state_dir, initial_config = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    expected_ccloud_scenario = clean_demo_state_template.ccloud_scenario
    ccloud_backend = _create_real_backend(initial_config)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend, expected_ccloud_scenario)
    finally:
        ccloud_backend.dispose()

    mixed_config = _write_config(
        tmp_path,
        state_dir / "confluent.db",
        state_dir / "second-self-managed.db",
    )
    with pytest.raises(ValueError, match="no persisted chargeback dates"):
        generate_or_reuse_clean_demo(
            config_path=mixed_config,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )
    ccloud_backend = _create_real_backend(mixed_config)
    try:
        assert _snapshot_persisted_state(ccloud_backend, expected_ccloud_scenario) == ccloud_before
    finally:
        ccloud_backend.dispose()
    self_managed_backend = _create_real_backend(mixed_config, SELF_MANAGED_TENANT_NAME)
    try:
        _assert_empty_self_managed(self_managed_backend)
    finally:
        self_managed_backend.dispose()


@pytest.mark.parametrize(
    "corruption",
    ["missing-resource", "unexpected-resource-type", "deleted-resource", "deleted-identity"],
)
def test_generator_rejects_missing_unexpected_or_deleted_persisted_topology_without_repairing_it(
    tmp_path: Path,
    corruption: str,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    scenario = clean_demo_state_template.ccloud_scenario
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
        generate_or_reuse_clean_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )

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
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    scenario = clean_demo_state_template.self_managed_scenario
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
        generate_or_reuse_clean_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )

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
    state_dir = _profile_state_dir(tmp_path)
    config_path = _write_config(
        tmp_path,
        state_dir / "partial-confluent.db",
        state_dir / "partial-self-managed.db",
    )
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
        generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=ANCHOR_DATE)
    assert str(error.value) == (
        "Incompatible clean demo state: tenant clean-confluent contains existing demo data. "
        "Run './demo reset --clean' to regenerate it."
    )

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend) == before
    finally:
        backend.dispose()


def test_generator_rejects_partial_tenant_persistence_without_a_manifest_or_repair(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A failed second tenant leaves no manifest and requires an explicit reset."""
    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    original_add_tag = SQLModelEntityTagRepository.add_tag

    def fail_self_managed_tag_write(
        self: SQLModelEntityTagRepository,
        tenant_id: str,
        entity_type: str,
        entity_id: str,
        tag_key: str,
        tag_value: str,
        created_by: str,
    ) -> object:
        if tenant_id == SELF_MANAGED_TENANT_ID:
            raise OSError("simulated self-managed entity-tag persistence failure")
        return original_add_tag(self, tenant_id, entity_type, entity_id, tag_key, tag_value, created_by)

    with monkeypatch.context() as failing_patch:
        failing_patch.setattr(SQLModelEntityTagRepository, "add_tag", fail_self_managed_tag_write)
        with pytest.raises(OSError, match="simulated self-managed entity-tag persistence failure"):
            generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=ANCHOR_DATE)

    assert not _manifest_path(state_dir).exists()
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend)
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()

    with pytest.raises(ValueError, match=re.escape("./demo reset --clean")):
        generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=ANCHOR_DATE)
    assert not _manifest_path(state_dir).exists()

    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_persisted_state(ccloud_backend) == ccloud_before
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


def test_generator_does_not_report_success_when_preview_evidence_persistence_fails_and_rejects_incomplete_reuse(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)

    def fail_preview_evidence(
        self: CCloudNativeSourceEvidenceCapture,
        *_args: object,
        **_kwargs: object,
    ) -> NoReturn:
        raise OSError("simulated preview-evidence persistence failure")

    with monkeypatch.context() as failing_patch:
        failing_patch.setattr(CCloudNativeSourceEvidenceCapture, "persist", fail_preview_evidence)
        with pytest.raises(OSError, match="simulated preview-evidence persistence failure"):
            generate_or_reuse_clean_demo(config_path=config_path, state_dir=state_dir, anchor_date=ANCHOR_DATE)

    assert not _manifest_path(state_dir).exists()
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend)
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()

    with pytest.raises(ValueError, match=re.escape("./demo reset --clean")):
        generate_or_reuse_clean_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )
    assert not _manifest_path(state_dir).exists()

    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_persisted_state(ccloud_backend) == ccloud_before
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


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
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    """Production startup serves both generated tenants without provider or metrics acquisition."""
    _state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    scenario = clean_demo_state_template.ccloud_scenario
    self_managed_scenario = clean_demo_state_template.self_managed_scenario

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
    future_clean_demo_state_template: _DemoStateTemplate,
) -> None:
    """A future rolling demo anchor remains eligible for a real Preview request."""
    _, config_path = _copy_demo_state(tmp_path, future_clean_demo_state_template, name="future-clean")

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


def _assert_persisted_showcase_collections(backend: SQLModelBackend, scenario: Any) -> None:
    """Compare Showcase facts without retaining a second full actual collection."""
    with backend.create_read_only_unit_of_work() as uow:
        actual_tags, tag_total = uow.tags.find_tags_for_tenant(TENANT_ID, limit=max(1, len(scenario.entity_tags)))
        assert tag_total == len(scenario.entity_tags)
        assert Counter(_tag_fields(tag) for tag in actual_tags) == Counter(
            _tag_fields(tag) for tag in scenario.entity_tags
        )

        _one_billing, billing_total = uow.billing.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
        assert billing_total == len(scenario.billing_lines)
        assert billing_total > 10_000
        expected_billing_by_date: defaultdict[date, Counter[tuple[object, ...]]] = defaultdict(Counter)
        for line in scenario.billing_lines:
            expected_billing_by_date[line.timestamp.date()][_billing_fields(line)] += 1
        traversed_billing = 0
        for expected_date, expected_rows in expected_billing_by_date.items():
            actual_rows = uow.billing.find_by_date(ECOSYSTEM, TENANT_ID, expected_date)
            traversed_billing += len(actual_rows)
            assert (
                Counter(_billing_fields(cast("CCloudBillingLineItem", line)) for line in actual_rows) == expected_rows
            )
        assert traversed_billing == billing_total

        _one_chargeback, chargeback_total = uow.chargebacks.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
        assert chargeback_total == len(scenario.chargebacks)
        assert chargeback_total > 10_000
        expected_chargebacks = Counter(_chargeback_fields(row) for row in scenario.chargebacks)
        streamed_chargebacks = 0
        for chargeback_row in uow.chargebacks.iter_by_filters(ECOSYSTEM, TENANT_ID, batch_size=257):
            key = _chargeback_fields(chargeback_row)
            assert expected_chargebacks[key] > 0
            expected_chargebacks[key] -= 1
            streamed_chargebacks += 1
        assert streamed_chargebacks == chargeback_total
        assert not +expected_chargebacks

        _one_topic, topic_total = uow.topic_attributions.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
        assert topic_total == len(scenario.topic_attributions)
        assert topic_total > 10_000
        expected_topics = Counter(_topic_attribution_fields(row) for row in scenario.topic_attributions)
        streamed_topics = 0
        for topic_row in uow.topic_attributions.iter_by_filters(ECOSYSTEM, TENANT_ID, batch_size=257):
            key = _topic_attribution_fields(topic_row)
            assert expected_topics[key] > 0
            expected_topics[key] -= 1
            streamed_topics += 1
        assert streamed_topics == topic_total
        assert not +expected_topics


def _status_counts(records: Any, *, deleted: Any) -> dict[str, tuple[int, int, int]]:
    by_type: defaultdict[str, list[Any]] = defaultdict(list)
    for record in records:
        record_type = record.resource_type if hasattr(record, "resource_type") else record.identity_type
        by_type[record_type].append(record)
    return {
        record_type: (
            len(values),
            sum(
                record.status is not deleted if hasattr(record, "status") else record.deleted_at is None
                for record in values
            ),
            sum(
                record.status is deleted if hasattr(record, "status") else record.deleted_at is not None
                for record in values
            ),
        )
        for record_type, values in by_type.items()
    }


def _repository_counts(counts: Any) -> dict[str, tuple[int, int, int]]:
    return {record_type: (count.total, count.active, count.deleted) for record_type, count in counts.items()}


def _persisted_billing_total(backend: SQLModelBackend) -> int:
    with backend.create_read_only_unit_of_work() as uow:
        _rows, total = uow.billing.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
    return total


def _persisted_chargeback_snapshot(
    backend: SQLModelBackend,
    tracking_date: date,
) -> tuple[int, Counter[tuple[object, ...]]]:
    with backend.create_read_only_unit_of_work() as uow:
        _one_row, total = uow.chargebacks.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
        return total, Counter(
            _chargeback_fields(chargeback_row)
            for chargeback_row in uow.chargebacks.find_by_date(
                ECOSYSTEM,
                TENANT_ID,
                tracking_date,
            )
        )


def _persisted_topic_snapshot(
    backend: SQLModelBackend,
    tracking_date: date,
) -> tuple[int, Counter[tuple[object, ...]]]:
    with backend.create_read_only_unit_of_work() as uow:
        _one_row, total = uow.topic_attributions.find_by_filters(ECOSYSTEM, TENANT_ID, limit=1)
        return total, Counter(
            _topic_attribution_fields(topic_row)
            for topic_row in uow.topic_attributions.find_by_date(
                ECOSYSTEM,
                TENANT_ID,
                tracking_date,
            )
        )


def _replace_chargeback_day(
    backend: SQLModelBackend,
    tracking_date: date,
    rows: list[ChargebackRow],
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.chargebacks.delete_by_date(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.chargebacks.upsert_batch(rows)
        uow.commit()


def _replace_topic_day(
    backend: SQLModelBackend,
    tracking_date: date,
    rows: list[TopicAttributionRow],
) -> None:
    with backend.create_unit_of_work() as uow:
        uow.topic_attributions.delete_by_date(ECOSYSTEM, TENANT_ID, tracking_date)
        uow.topic_attributions.upsert_batch(rows)
        uow.commit()


def test_showcase_generator_persists_complete_collections_with_bounded_validation_and_reuses(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Profile-aware generation validates every Showcase fact through bounded repository seams."""
    from demo.generator import DemoProfile, generate_or_reuse_demo
    from plugins.confluent_cloud.demo.scenario import build_showcase_demo_scenario

    state_dir = _profile_state_dir(tmp_path, "showcase")
    config_path = _write_config(
        tmp_path,
        state_dir / "showcase-confluent.db",
        state_dir / "showcase-self-managed.db",
    )
    billing_limits: list[int] = []
    chargeback_limits: list[int] = []
    topic_limits: list[int] = []
    chargeback_batches: list[int] = []
    topic_batches: list[int] = []
    original_billing_find = CCloudBillingRepository.find_by_filters
    original_chargeback_find = CCloudChargebackRepository.find_by_filters
    original_topic_find = TopicAttributionRepository.find_by_filters
    original_chargeback_iter = CCloudChargebackRepository.iter_by_filters
    original_topic_iter = TopicAttributionRepository.iter_by_filters

    def record_billing_limit(
        self: CCloudBillingRepository,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> tuple[list[CCloudBillingLineItem], int]:
        billing_limits.append(limit)
        return original_billing_find(
            self,
            ecosystem,
            tenant_id,
            start,
            end,
            product_type,
            resource_id,
            limit,
            offset,
        )

    def record_chargeback_limit(
        self: CCloudChargebackRepository,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[ChargebackRow], int]:
        chargeback_limits.append(limit)
        return original_chargeback_find(
            self,
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            limit,
            offset,
            tag_key,
            tag_value,
            tags_repo,
        )

    def record_topic_limit(
        self: TopicAttributionRepository,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> tuple[list[TopicAttributionRow], int]:
        topic_limits.append(limit)
        return original_topic_find(
            self,
            ecosystem,
            tenant_id,
            start,
            end,
            cluster_resource_id,
            topic_name,
            product_type,
            attribution_method,
            limit,
            offset,
            tag_key,
            tag_value,
            tags_repo,
        )

    def record_chargeback_batches(
        self: CCloudChargebackRepository,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        identity_id: str | None = None,
        product_type: str | None = None,
        resource_id: str | None = None,
        cost_type: str | None = None,
        batch_size: int = 5000,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[ChargebackRow]:
        chargeback_batches.append(batch_size)
        yield from original_chargeback_iter(
            self,
            ecosystem,
            tenant_id,
            start,
            end,
            identity_id,
            product_type,
            resource_id,
            cost_type,
            batch_size,
            tag_key,
            tag_value,
            tags_repo,
        )

    def record_topic_batches(
        self: TopicAttributionRepository,
        ecosystem: str,
        tenant_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        cluster_resource_id: str | None = None,
        topic_name: str | None = None,
        product_type: str | None = None,
        attribution_method: str | None = None,
        batch_size: int = 5000,
        tag_key: str | None = None,
        tag_value: str | None = None,
        tags_repo: EntityTagRepository | None = None,
    ) -> Iterator[TopicAttributionRow]:
        topic_batches.append(batch_size)
        yield from original_topic_iter(
            self,
            ecosystem,
            tenant_id,
            start,
            end,
            cluster_resource_id,
            topic_name,
            product_type,
            attribution_method,
            batch_size,
            tag_key,
            tag_value,
            tags_repo,
        )

    monkeypatch.setattr(CCloudBillingRepository, "find_by_filters", record_billing_limit)
    monkeypatch.setattr(CCloudChargebackRepository, "find_by_filters", record_chargeback_limit)
    monkeypatch.setattr(TopicAttributionRepository, "find_by_filters", record_topic_limit)
    monkeypatch.setattr(CCloudChargebackRepository, "iter_by_filters", record_chargeback_batches)
    monkeypatch.setattr(TopicAttributionRepository, "iter_by_filters", record_topic_batches)

    result = generate_or_reuse_demo(
        config_path=config_path,
        state_dir=state_dir,
        anchor_date=ANCHOR_DATE,
        profile=DemoProfile.SHOWCASE,
    )

    assert result == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    assert billing_limits and set(billing_limits) == {1}
    assert chargeback_limits and set(chargeback_limits) == {1}
    assert topic_limits and set(topic_limits) == {1}
    assert chargeback_batches and all(0 < batch_size < 10_000 for batch_size in chargeback_batches)
    assert topic_batches and all(0 < batch_size < 10_000 for batch_size in topic_batches)

    scenario = build_showcase_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)
    backend = _create_real_backend(config_path)
    try:
        _assert_persisted_showcase_collections(backend, scenario)
        with backend.create_read_only_unit_of_work() as uow:
            resource_counts = _repository_counts(uow.resources.count_by_type(ECOSYSTEM, TENANT_ID))
            identity_counts = _repository_counts(uow.identities.count_by_type(ECOSYSTEM, TENANT_ID))
            assert resource_counts == _status_counts(
                scenario.resources,
                deleted=ResourceStatus.DELETED,
            )
            assert identity_counts == _status_counts(
                scenario.identities,
                deleted=ResourceStatus.DELETED,
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

    billing_limits.clear()
    chargeback_limits.clear()
    topic_limits.clear()
    chargeback_batches.clear()
    topic_batches.clear()

    def fail_write_uow(self: SQLModelBackend) -> NoReturn:
        raise AssertionError("a complete Showcase state must be reused without persistence")

    with monkeypatch.context() as reuse_patch:
        reuse_patch.setattr(SQLModelBackend, "create_unit_of_work", fail_write_uow)
        assert generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
            profile=DemoProfile.SHOWCASE,
        ) == _generation_results(GenerationResult.REUSED, GenerationResult.REUSED)
        assert generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
            profile=DemoProfile.SHOWCASE,
        ) == _generation_results(GenerationResult.REUSED, GenerationResult.REUSED)
    assert billing_limits and set(billing_limits) == {1}
    assert chargeback_limits and set(chargeback_limits) == {1}
    assert topic_limits and set(topic_limits) == {1}
    assert chargeback_batches and topic_batches


def test_generator_rejects_clean_and_showcase_state_profile_mismatches_without_repairing(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
    showcase_demo_state_template: _DemoStateTemplate,
) -> None:
    from demo.generator import DemoProfile, generate_or_reuse_demo

    clean_state_dir, clean_config = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    assert generate_or_reuse_clean_demo(
        config_path=clean_config,
        state_dir=clean_state_dir,
        anchor_date=ANCHOR_DATE,
    ) == _generation_results(
        GenerationResult.REUSED,
        GenerationResult.REUSED,
    )
    clean_backend = _create_real_backend(clean_config)
    try:
        clean_total = _persisted_billing_total(clean_backend)
    finally:
        clean_backend.dispose()

    with pytest.raises(ValueError, match="Incompatible showcase demo state"):
        generate_or_reuse_demo(
            config_path=clean_config,
            state_dir=clean_state_dir,
            anchor_date=ANCHOR_DATE,
            profile=DemoProfile.SHOWCASE,
        )

    clean_backend = _create_real_backend(clean_config)
    try:
        assert _persisted_billing_total(clean_backend) == clean_total
    finally:
        clean_backend.dispose()

    showcase_state_dir, showcase_config = _copy_demo_state(
        tmp_path,
        showcase_demo_state_template,
        name="showcase",
    )
    showcase_backend = _create_real_backend(showcase_config)
    try:
        showcase_total = _persisted_billing_total(showcase_backend)
    finally:
        showcase_backend.dispose()

    with pytest.raises(ValueError, match="Incompatible clean demo state"):
        generate_or_reuse_clean_demo(
            config_path=showcase_config,
            state_dir=showcase_state_dir,
            anchor_date=ANCHOR_DATE,
        )

    showcase_backend = _create_real_backend(showcase_config)
    try:
        assert _persisted_billing_total(showcase_backend) == showcase_total
    finally:
        showcase_backend.dispose()


def test_showcase_generator_rejects_missing_unexpected_and_same_total_billing_replacements(
    tmp_path: Path,
    showcase_demo_state_template: _DemoStateTemplate,
) -> None:
    from demo.generator import DemoProfile, generate_or_reuse_demo

    state_dir, config_path = _copy_demo_state(tmp_path, showcase_demo_state_template, name="showcase")
    scenario = showcase_demo_state_template.ccloud_scenario
    expected = scenario.billing_lines[-1]
    expected_day_rows = [line for line in scenario.billing_lines if line.timestamp.date() == expected.timestamp.date()]

    backend = _create_real_backend(config_path)
    try:
        with backend.create_unit_of_work() as uow:
            billing_repository = cast("CCloudBillingRepository", uow.billing)
            billing_repository.replace_for_date(
                ECOSYSTEM,
                TENANT_ID,
                expected.timestamp.date(),
                [line for line in expected_day_rows if line != expected],
            )
            uow.commit()
        missing_total = _persisted_billing_total(backend)
    finally:
        backend.dispose()
    with pytest.raises(ValueError, match="persisted billing"):
        generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
            profile=DemoProfile.SHOWCASE,
        )
    backend = _create_real_backend(config_path)
    try:
        assert _persisted_billing_total(backend) == missing_total
        with backend.create_unit_of_work() as uow:
            billing_repository = cast("CCloudBillingRepository", uow.billing)
            billing_repository.replace_for_date(ECOSYSTEM, TENANT_ID, expected.timestamp.date(), expected_day_rows)
            uow.commit()
    finally:
        backend.dispose()

    unexpected = replace(expected, product_type=f"{expected.product_type}_unexpected")
    backend = _create_real_backend(config_path)
    try:
        with backend.create_unit_of_work() as uow:
            uow.billing.upsert(unexpected)
            uow.commit()
        unexpected_total = _persisted_billing_total(backend)
    finally:
        backend.dispose()
    with pytest.raises(ValueError, match="persisted billing"):
        generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
            profile=DemoProfile.SHOWCASE,
        )
    backend = _create_real_backend(config_path)
    try:
        assert _persisted_billing_total(backend) == unexpected_total
        with backend.create_unit_of_work() as uow:
            billing_repository = cast("CCloudBillingRepository", uow.billing)
            billing_repository.replace_for_date(ECOSYSTEM, TENANT_ID, expected.timestamp.date(), expected_day_rows)
            uow.commit()
    finally:
        backend.dispose()

    backend = _create_real_backend(config_path)
    try:
        replacement_rows = [unexpected if line == expected else line for line in expected_day_rows]
        with backend.create_unit_of_work() as uow:
            billing_repository = cast("CCloudBillingRepository", uow.billing)
            billing_repository.replace_for_date(ECOSYSTEM, TENANT_ID, expected.timestamp.date(), replacement_rows)
            uow.commit()
        replacement_total = _persisted_billing_total(backend)
    finally:
        backend.dispose()
    assert replacement_total == len(scenario.billing_lines)
    with pytest.raises(ValueError, match="persisted billing"):
        generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
            profile=DemoProfile.SHOWCASE,
        )
    backend = _create_real_backend(config_path)
    try:
        assert _persisted_billing_total(backend) == replacement_total
    finally:
        backend.dispose()


def test_showcase_generator_rejects_missing_unexpected_and_same_total_fact_replacements(
    tmp_path: Path,
    showcase_demo_state_template: _DemoStateTemplate,
) -> None:
    from demo.generator import DemoProfile, generate_or_reuse_demo

    state_dir, config_path = _copy_demo_state(tmp_path, showcase_demo_state_template, name="showcase")
    scenario = showcase_demo_state_template.ccloud_scenario

    corruption_kinds = ("missing", "unexpected", "replacement")

    def exercise_chargebacks(corruption: str) -> None:
        expected = scenario.chargebacks[-1]
        expected_day_rows = [row for row in scenario.chargebacks if row.timestamp.date() == expected.timestamp.date()]
        unexpected = replace(expected, product_type=f"{expected.product_type}_unexpected")
        if corruption == "missing":
            corrupted_rows = [row for row in expected_day_rows if row != expected]
        elif corruption == "unexpected":
            corrupted_rows = [*expected_day_rows, unexpected]
        else:
            corrupted_rows = [unexpected if row == expected else row for row in expected_day_rows]

        backend = _create_real_backend(config_path)
        try:
            _replace_chargeback_day(backend, expected.timestamp.date(), corrupted_rows)
            state_before_failure = _persisted_chargeback_snapshot(backend, expected.timestamp.date())
        finally:
            backend.dispose()
        expected_total = len(scenario.chargebacks) + (
            1 if corruption == "unexpected" else -1 if corruption == "missing" else 0
        )
        assert state_before_failure[0] == expected_total

        with pytest.raises(ValueError, match="persisted Showcase chargebacks"):
            generate_or_reuse_demo(
                config_path=config_path,
                state_dir=state_dir,
                anchor_date=ANCHOR_DATE + timedelta(days=1),
                profile=DemoProfile.SHOWCASE,
            )

        backend = _create_real_backend(config_path)
        try:
            assert _persisted_chargeback_snapshot(backend, expected.timestamp.date()) == state_before_failure
            _replace_chargeback_day(backend, expected.timestamp.date(), expected_day_rows)
        finally:
            backend.dispose()

    def exercise_topics(corruption: str) -> None:
        expected = scenario.topic_attributions[-1]
        expected_day_rows = [
            row for row in scenario.topic_attributions if row.timestamp.date() == expected.timestamp.date()
        ]
        unexpected = replace(expected, product_type=f"{expected.product_type}_unexpected")
        if corruption == "missing":
            corrupted_rows = [row for row in expected_day_rows if row != expected]
        elif corruption == "unexpected":
            corrupted_rows = [*expected_day_rows, unexpected]
        else:
            corrupted_rows = [unexpected if row == expected else row for row in expected_day_rows]

        backend = _create_real_backend(config_path)
        try:
            _replace_topic_day(backend, expected.timestamp.date(), corrupted_rows)
            state_before_failure = _persisted_topic_snapshot(backend, expected.timestamp.date())
        finally:
            backend.dispose()
        expected_total = len(scenario.topic_attributions) + (
            1 if corruption == "unexpected" else -1 if corruption == "missing" else 0
        )
        assert state_before_failure[0] == expected_total

        with pytest.raises(ValueError, match="persisted Showcase topic attributions"):
            generate_or_reuse_demo(
                config_path=config_path,
                state_dir=state_dir,
                anchor_date=ANCHOR_DATE + timedelta(days=1),
                profile=DemoProfile.SHOWCASE,
            )

        backend = _create_real_backend(config_path)
        try:
            assert _persisted_topic_snapshot(backend, expected.timestamp.date()) == state_before_failure
            _replace_topic_day(backend, expected.timestamp.date(), expected_day_rows)
        finally:
            backend.dispose()

    for corruption in corruption_kinds:
        exercise_chargebacks(corruption)
        exercise_topics(corruption)


def test_generated_showcase_data_uses_normal_api_startup_without_provider_or_metrics_requests(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    showcase_demo_state_template: _DemoStateTemplate,
) -> None:
    _, config_path = _copy_demo_state(tmp_path, showcase_demo_state_template, name="showcase")
    scenario = showcase_demo_state_template.ccloud_scenario

    provider_requests: list[tuple[str, str]] = []

    def fail_provider_request(
        self: CCloudConnection,
        method: str,
        url: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        provider_requests.append((method, url))
        raise AssertionError(f"unexpected provider request: {method} {url}")

    metrics_requests: list[object] = []

    def fail_metrics_query(self: PrometheusMetricsSource, *_args: object, **_kwargs: object) -> NoReturn:
        metrics_requests.append(self)
        raise AssertionError("unexpected Prometheus query")

    monkeypatch.setattr(CCloudConnection, "_request", fail_provider_request)
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
        assert health.json()["version"]

        readiness = client.get("/api/v1/readiness")
        assert readiness.status_code == 200
        assert readiness.json()["mode"] == "api"
        assert readiness.json()["status"] == "ready"

        tenants = client.get("/api/v1/tenants")
        assert tenants.status_code == 200
        assert [(item["tenant_name"], item["tenant_id"]) for item in tenants.json()["tenants"]] == [
            (TENANT_NAME, TENANT_ID),
            (SELF_MANAGED_TENANT_NAME, SELF_MANAGED_TENANT_ID),
        ]

        cost = client.get(
            f"{api_prefix}/chargebacks/aggregate",
            params={**period_params, "group_by": "resource_id", "time_bucket": "day"},
        )
        assert cost.status_code == 200
        assert cost.json()["buckets"]
        assert _decimal_from_response(cost.json()["total_amount"]) == sum(
            (line.total_cost for line in scenario.billing_lines),
            Decimal("0"),
        )

        topic_attributions = client.get(f"{api_prefix}/topic-attributions", params=period_params)
        assert topic_attributions.status_code == 200
        assert topic_attributions.json()["total"] == len(scenario.topic_attributions)
        assert topic_attributions.json()["total"] > 10_000

    assert provider_requests == []
    assert metrics_requests == []


def test_canonical_demo_state_materialization_is_test_local(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    """Canonical state copies keep SQLite, config, and preview writes test-local."""
    template = clean_demo_state_template
    first_state_dir, first_config_path = _copy_demo_state(tmp_path, template, name="first")
    second_state_dir, second_config_path = _copy_demo_state(tmp_path, template, name="second")

    first_settings = load_config(first_config_path)
    second_settings = load_config(second_config_path)
    first_paths = (
        first_state_dir,
        Path(
            first_settings.tenants[TENANT_NAME].storage.connection_string.get_secret_value().removeprefix("sqlite:///")
        ),
        Path(
            first_settings.tenants[SELF_MANAGED_TENANT_NAME]
            .storage.connection_string.get_secret_value()
            .removeprefix("sqlite:///")
        ),
        first_config_path,
        first_settings.preview.artifact_root,
    )
    second_paths = (
        second_state_dir,
        Path(
            second_settings.tenants[TENANT_NAME].storage.connection_string.get_secret_value().removeprefix("sqlite:///")
        ),
        Path(
            second_settings.tenants[SELF_MANAGED_TENANT_NAME]
            .storage.connection_string.get_secret_value()
            .removeprefix("sqlite:///")
        ),
        second_config_path,
        second_settings.preview.artifact_root,
    )

    assert all(path.is_relative_to(tmp_path) for path in (*first_paths, *second_paths))
    assert first_paths[0] == first_paths[1].parent == first_paths[2].parent
    assert second_paths[0] == second_paths[1].parent == second_paths[2].parent
    assert all(first_path != second_path for first_path, second_path in zip(first_paths, second_paths, strict=True))
    assert (first_state_dir / "demo-state.json").is_file()
    assert (second_state_dir / "demo-state.json").is_file()

    expected_tag = template.ccloud_scenario.entity_tags[0]
    mutated_tag_value = "isolated-mutation"
    first_backend = _create_real_backend(first_config_path)
    try:
        with first_backend.create_unit_of_work() as uow:
            first_tag = next(
                tag
                for tag in uow.tags.get_tags(TENANT_ID, expected_tag.entity_type, expected_tag.entity_id)
                if tag.tag_key == expected_tag.tag_key
            )
            assert _tag_fields(first_tag) == _tag_fields(expected_tag)
            assert first_tag.tag_id is not None
            assert uow.tags.update_tag(first_tag.tag_id, mutated_tag_value).tag_value == mutated_tag_value
            uow.commit()
    finally:
        first_backend.dispose()

    first_reopened_backend = _create_real_backend(first_config_path)
    try:
        with first_reopened_backend.create_read_only_unit_of_work() as uow:
            first_reopened_tag = next(
                tag
                for tag in uow.tags.get_tags(TENANT_ID, expected_tag.entity_type, expected_tag.entity_id)
                if tag.tag_key == expected_tag.tag_key
            )
            assert _tag_fields(first_reopened_tag) == _tag_fields(replace(expected_tag, tag_value=mutated_tag_value))
    finally:
        first_reopened_backend.dispose()

    second_backend = _create_real_backend(second_config_path)
    try:
        with second_backend.create_read_only_unit_of_work() as uow:
            second_tag = next(
                tag
                for tag in uow.tags.get_tags(TENANT_ID, expected_tag.entity_type, expected_tag.entity_id)
                if tag.tag_key == expected_tag.tag_key
            )
            assert _tag_fields(second_tag) == _tag_fields(expected_tag)
    finally:
        second_backend.dispose()

    later_state_dir, later_config_path = _copy_demo_state(tmp_path, template, name="later")
    assert later_state_dir.is_relative_to(tmp_path)
    assert later_config_path.is_relative_to(tmp_path)
    later_backend = _create_real_backend(later_config_path)
    try:
        with later_backend.create_read_only_unit_of_work() as uow:
            later_tag = next(
                tag
                for tag in uow.tags.get_tags(TENANT_ID, expected_tag.entity_type, expected_tag.entity_id)
                if tag.tag_key == expected_tag.tag_key
            )
            assert _tag_fields(later_tag) == _tag_fields(expected_tag)
    finally:
        later_backend.dispose()


def _profile_state_dir(tmp_path: Path, profile: str = "clean") -> Path:
    state_dir = tmp_path / f"{profile}-state"
    state_dir.mkdir()
    return state_dir


def _state_config(tmp_path: Path, state_dir: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    return _write_config(tmp_path, state_dir / "confluent.db", state_dir / "self-managed.db")


def _build_demo_state_template(
    tmp_path_factory: pytest.TempPathFactory,
    *,
    profile: DemoProfile,
    anchor_date: date,
    name: str,
) -> _DemoStateTemplate:
    root = tmp_path_factory.mktemp(name)
    state_dir = root / "state"
    state_dir.mkdir()
    config_path = _state_config(root, state_dir)
    result = generate_or_reuse_demo(
        config_path=config_path,
        state_dir=state_dir,
        anchor_date=anchor_date,
        profile=profile,
    )
    assert result == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    ccloud_scenario = (
        build_showcase_demo_scenario(tenant_id=TENANT_ID, anchor_date=anchor_date)
        if profile is DemoProfile.SHOWCASE
        else build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=anchor_date)
    )
    return _DemoStateTemplate(
        state_dir=state_dir,
        profile=profile,
        anchor_date=anchor_date,
        ccloud_scenario=ccloud_scenario,
        self_managed_scenario=build_clean_self_managed_kafka_scenario(
            tenant_id=SELF_MANAGED_TENANT_ID,
            anchor_date=anchor_date,
        ),
    )


@pytest.fixture(scope="session")
def clean_demo_state_template(tmp_path_factory: pytest.TempPathFactory) -> _DemoStateTemplate:
    return _build_demo_state_template(
        tmp_path_factory,
        profile=DemoProfile.CLEAN,
        anchor_date=ANCHOR_DATE,
        name="clean-canonical",
    )


@pytest.fixture(scope="session")
def future_clean_demo_state_template(tmp_path_factory: pytest.TempPathFactory) -> _DemoStateTemplate:
    return _build_demo_state_template(
        tmp_path_factory,
        profile=DemoProfile.CLEAN,
        anchor_date=FUTURE_ANCHOR_DATE,
        name="future-clean-canonical",
    )


@pytest.fixture(scope="session")
def showcase_demo_state_template(tmp_path_factory: pytest.TempPathFactory) -> _DemoStateTemplate:
    return _build_demo_state_template(
        tmp_path_factory,
        profile=DemoProfile.SHOWCASE,
        anchor_date=ANCHOR_DATE,
        name="showcase-canonical",
    )


def _copy_demo_state(
    tmp_path: Path,
    template: _DemoStateTemplate,
    *,
    name: str,
) -> tuple[Path, Path]:
    case_root = tmp_path / name
    case_root.mkdir()
    state_dir = case_root / "state"
    shutil.copytree(template.state_dir, state_dir)
    return state_dir, _state_config(case_root, state_dir)


def _manifest_path(state_dir: Path) -> Path:
    return state_dir / "demo-state.json"


def _manifest_snapshot(state_dir: Path) -> tuple[bytes, int]:
    manifest = _manifest_path(state_dir)
    return manifest.read_bytes(), manifest.stat().st_mtime_ns


def _remove_all_generator_owned_rows(backend: SQLModelBackend) -> None:
    """Create manifested empty state without changing the profile completion marker."""
    table_names = sorted(inspect(backend._engine).get_table_names())
    with backend._engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys = OFF")
        for table_name in table_names:
            if table_name == "alembic_version":
                continue
            connection.exec_driver_sql(f'DELETE FROM "{table_name}"')
        connection.commit()


def test_generator_writes_an_exact_profile_local_completion_manifest_only_after_full_validation(
    tmp_path: Path,
) -> None:
    import demo.generator as generator

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)

    result = generator.generate_or_reuse_demo(
        config_path=config_path,
        state_dir=state_dir,
        anchor_date=ANCHOR_DATE,
    )

    assert result == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    assert json.loads(_manifest_path(state_dir).read_text(encoding="utf-8")) == {
        "schema_version": 1,
        "generator_version": 1,
        "profile": "clean",
        "anchor_date": ANCHOR_DATE.isoformat(),
    }


def test_generator_final_validation_failure_leaves_no_manifest_and_the_partial_state_requires_reset(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    import demo.generator as generator

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)

    def fail_preview_validation(*_args: object, **_kwargs: object) -> NoReturn:
        raise OSError("controlled preview validation failure")

    with monkeypatch.context() as failing_patch:
        failing_patch.setattr(generator, "_validate_ccloud_preview", fail_preview_validation)
        with pytest.raises(OSError, match="controlled preview validation failure"):
            generator.generate_or_reuse_demo(
                config_path=config_path,
                state_dir=state_dir,
                anchor_date=ANCHOR_DATE,
            )
    assert not _manifest_path(state_dir).exists()
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend)
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()
    with pytest.raises(ValueError, match=re.escape("./demo reset --clean")):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
        )
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_persisted_state(ccloud_backend) == ccloud_before
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


def test_clean_and_showcase_manifests_are_independent_and_reuse_does_not_rewrite_them(tmp_path: Path) -> None:
    import demo.generator as generator

    clean_state_dir = _profile_state_dir(tmp_path, "clean")
    showcase_state_dir = _profile_state_dir(tmp_path, "showcase")
    clean_config = _state_config(tmp_path / "clean", clean_state_dir)
    showcase_config = _state_config(tmp_path / "showcase", showcase_state_dir)

    assert generator.generate_or_reuse_demo(
        config_path=clean_config,
        state_dir=clean_state_dir,
        anchor_date=ANCHOR_DATE,
    ) == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    assert generator.generate_or_reuse_demo(
        config_path=showcase_config,
        state_dir=showcase_state_dir,
        anchor_date=ANCHOR_DATE,
        profile=generator.DemoProfile.SHOWCASE,
    ) == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    clean_manifest = _manifest_path(clean_state_dir)
    clean_before = (clean_manifest.read_bytes(), clean_manifest.stat().st_mtime_ns)

    assert generator.generate_or_reuse_demo(
        config_path=clean_config,
        state_dir=clean_state_dir,
        anchor_date=ANCHOR_DATE + timedelta(days=1),
    ) == _generation_results(GenerationResult.REUSED, GenerationResult.REUSED)

    assert json.loads(clean_manifest.read_text(encoding="utf-8"))["profile"] == "clean"
    assert json.loads(_manifest_path(showcase_state_dir).read_text(encoding="utf-8"))["profile"] == "showcase"
    assert (clean_manifest.read_bytes(), clean_manifest.stat().st_mtime_ns) == clean_before


@pytest.mark.parametrize(
    "metadata",
    [
        "not-json",
        {},
        {"schema_version": 1, "generator_version": 1, "profile": "clean", "anchor_date": "2026-09-02", "extra": 1},
        {"schema_version": "1", "generator_version": 1, "profile": "clean", "anchor_date": "2026-09-02"},
        {"schema_version": 2, "generator_version": 1, "profile": "clean", "anchor_date": "2026-09-02"},
        {"schema_version": 1, "generator_version": 2, "profile": "clean", "anchor_date": "2026-09-02"},
        {"schema_version": 1, "generator_version": 1, "profile": "showcase", "anchor_date": "2026-09-02"},
        {"schema_version": 1, "generator_version": 1, "profile": "clean", "anchor_date": "not-a-date"},
    ],
    ids=[
        "malformed-json",
        "missing-fields",
        "extra-field",
        "wrong-field-type",
        "schema-mismatch",
        "generator-mismatch",
        "profile-mismatch",
        "invalid-anchor",
    ],
)
def test_generator_rejects_incompatible_manifest_before_opening_a_backend(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    metadata: str | dict[str, object],
) -> None:
    import demo.generator as generator

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    manifest_contents = metadata if isinstance(metadata, str) else json.dumps(metadata)
    _manifest_path(state_dir).write_text(manifest_contents, encoding="utf-8")

    def fail_backend_creation(*_args: object, **_kwargs: object) -> NoReturn:
        raise AssertionError("manifest compatibility must be checked before backend creation")

    monkeypatch.setattr(generator, "create_storage_backend", fail_backend_creation)
    with pytest.raises(ValueError) as error:
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
        )

    detail = str(error.value)
    assert detail.startswith("Incompatible clean demo state:")
    assert "./demo reset --clean" in detail


def test_generator_retries_unmanifested_schema_only_databases_only_after_all_tenant_and_preview_stores_are_empty(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    import demo.generator as generator
    from plugins.confluent_cloud.storage.preview_unit_of_work import CCloudPreviewGenerationReadSQLModelUnitOfWork

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    ccloud_backend.dispose()
    self_managed_backend.dispose()
    observed_contexts: list[bool] = []

    def record_empty_preview_check(
        self: CCloudPreviewGenerationReadSQLModelUnitOfWork,
        ecosystem: str,
        tenant_id: str,
    ) -> bool:
        if self._session is None:
            raise AssertionError("schema-only emptiness check must run inside the preview read UoW")
        observed_contexts.append(True)
        assert (ecosystem, tenant_id) == (ECOSYSTEM, TENANT_ID)
        return False

    monkeypatch.setattr(
        CCloudPreviewGenerationReadSQLModelUnitOfWork,
        "has_any_preview_evidence",
        record_empty_preview_check,
        raising=False,
    )
    assert generator.generate_or_reuse_demo(
        config_path=config_path,
        state_dir=state_dir,
        anchor_date=ANCHOR_DATE,
    ) == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    assert observed_contexts == [True]


def test_generator_rejects_unmanifested_artifacts_or_preview_evidence_without_generation_writes(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    import demo.generator as generator
    from plugins.confluent_cloud.storage.preview_unit_of_work import CCloudPreviewGenerationReadSQLModelUnitOfWork

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    ccloud_backend.dispose()
    self_managed_backend.dispose()

    def report_existing_preview_evidence(
        self: CCloudPreviewGenerationReadSQLModelUnitOfWork,
        _ecosystem: str,
        _tenant_id: str,
    ) -> bool:
        if self._session is None:
            raise AssertionError("schema-only emptiness check must run inside the preview read UoW")
        return True

    monkeypatch.setattr(
        CCloudPreviewGenerationReadSQLModelUnitOfWork,
        "has_any_preview_evidence",
        report_existing_preview_evidence,
        raising=False,
    )
    with pytest.raises(ValueError, match=re.escape("./demo reset --clean")):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
        )
    assert not _manifest_path(state_dir).exists()
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        _assert_empty(ccloud_backend)
        _assert_empty_self_managed(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


def test_generator_propagates_a_schema_only_preview_emptiness_failure_without_writing_state(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    import demo.generator as generator
    from plugins.confluent_cloud.storage.preview_unit_of_work import CCloudPreviewGenerationReadSQLModelUnitOfWork

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    ccloud_backend.dispose()
    self_managed_backend.dispose()

    def fail_preview_emptiness(
        self: CCloudPreviewGenerationReadSQLModelUnitOfWork,
        _ecosystem: str,
        _tenant_id: str,
    ) -> bool:
        if self._session is None:
            raise AssertionError("schema-only emptiness check must run inside the preview read UoW")
        raise OSError("controlled preview emptiness failure")

    monkeypatch.setattr(
        CCloudPreviewGenerationReadSQLModelUnitOfWork,
        "has_any_preview_evidence",
        fail_preview_emptiness,
        raising=False,
    )
    with pytest.raises(OSError, match="controlled preview emptiness failure"):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
        )
    assert not _manifest_path(state_dir).exists()
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        _assert_empty(ccloud_backend)
        _assert_empty_self_managed(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


def test_generator_allows_one_absent_and_one_schema_only_tenant_database_after_complete_empty_proof(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    import demo.generator as generator
    from plugins.confluent_cloud.storage.preview_unit_of_work import CCloudPreviewGenerationReadSQLModelUnitOfWork

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    self_managed_backend.dispose()
    observed: list[tuple[str, str]] = []

    def report_empty_preview(
        self: CCloudPreviewGenerationReadSQLModelUnitOfWork,
        ecosystem: str,
        tenant_id: str,
    ) -> bool:
        if self._session is None:
            raise AssertionError("schema-only emptiness check must run inside the preview read UoW")
        observed.append((ecosystem, tenant_id))
        return False

    monkeypatch.setattr(
        CCloudPreviewGenerationReadSQLModelUnitOfWork,
        "has_any_preview_evidence",
        report_empty_preview,
        raising=False,
    )
    assert generator.generate_or_reuse_demo(
        config_path=config_path,
        state_dir=state_dir,
        anchor_date=ANCHOR_DATE,
    ) == _generation_results(GenerationResult.GENERATED, GenerationResult.GENERATED)
    assert observed == [(ECOSYSTEM, TENANT_ID)]


def test_generator_rejects_unmanifested_non_database_artifacts_before_generation(tmp_path: Path) -> None:
    import demo.generator as generator

    state_dir = _profile_state_dir(tmp_path)
    config_path = _state_config(tmp_path, state_dir)
    artifact = state_dir / "interrupted-output.txt"
    artifact.write_text("not a demo database", encoding="utf-8")

    with pytest.raises(ValueError, match=re.escape("./demo reset --clean")):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE,
        )
    assert artifact.read_text(encoding="utf-8") == "not a demo database"
    assert not _manifest_path(state_dir).exists()


def test_generator_rejects_manifested_empty_generator_owned_state_without_regenerating(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    import demo.generator as generator

    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    manifest_before = _manifest_snapshot(state_dir)
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        _remove_all_generator_owned_rows(ccloud_backend)
        _remove_all_generator_owned_rows(self_managed_backend)
        ccloud_before = _snapshot_persisted_state(ccloud_backend)
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()

    with pytest.raises(ValueError, match="no persisted chargeback dates"):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )

    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_persisted_state(ccloud_backend) == ccloud_before
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()
    assert _manifest_snapshot(state_dir) == manifest_before


def test_generator_rejects_compatible_manifest_pipeline_state_corruption_without_repairing_it(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    import demo.generator as generator

    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    scenario = clean_demo_state_template.ccloud_scenario
    manifest_before = _manifest_snapshot(state_dir)
    backend = _create_real_backend(config_path)
    try:
        with backend.create_unit_of_work() as uow:
            states = uow.pipeline_state.find_by_range(ECOSYSTEM, TENANT_ID, date.min, date.max)
            assert states
            uow.pipeline_state.upsert(replace(states[0], billing_gathered=not states[0].billing_gathered))
            uow.commit()
        before = _snapshot_persisted_state(backend, scenario)
    finally:
        backend.dispose()

    with pytest.raises(ValueError, match="persisted pipeline state"):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend, scenario) == before
    finally:
        backend.dispose()
    assert _manifest_snapshot(state_dir) == manifest_before


def test_generator_rejects_compatible_manifest_preview_evidence_corruption_without_repairing_it(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    import demo.generator as generator
    from plugins.confluent_cloud.storage.tables import CCloudCostSourceTable

    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    scenario = clean_demo_state_template.ccloud_scenario
    manifest_before = _manifest_snapshot(state_dir)
    expected_source = scenario.preview_source_capture.records[0]
    backend = _create_real_backend(config_path)
    try:
        with backend._engine.begin() as connection:
            deleted = connection.execute(
                delete(CCloudCostSourceTable).where(
                    CCloudCostSourceTable.ecosystem == ECOSYSTEM,
                    CCloudCostSourceTable.tenant_id == TENANT_ID,
                    CCloudCostSourceTable.source_record_id == expected_source.source_record_id,
                    CCloudCostSourceTable.evidence_scope_start == expected_source.evidence_scope_start,
                    CCloudCostSourceTable.evidence_scope_end == expected_source.evidence_scope_end,
                )
            )
        assert deleted.rowcount == 1
        before = _snapshot_persisted_state(backend, scenario)
    finally:
        backend.dispose()

    with pytest.raises(ValueError, match="preview evidence does not match the generated source records"):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=1),
        )

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend, scenario) == before
    finally:
        backend.dispose()
    assert _manifest_snapshot(state_dir) == manifest_before


def test_generator_rejects_a_manifest_anchor_that_differs_from_persisted_chargebacks(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    import demo.generator as generator

    state_dir, config_path = _copy_demo_state(tmp_path, clean_demo_state_template, name="clean")
    manifest = json.loads(_manifest_path(state_dir).read_text(encoding="utf-8"))
    manifest["anchor_date"] = (ANCHOR_DATE + timedelta(days=2)).isoformat()
    _manifest_path(state_dir).write_text(json.dumps(manifest), encoding="utf-8")
    manifest_before = _manifest_snapshot(state_dir)
    backend = _create_real_backend(config_path)
    try:
        before = _snapshot_persisted_state(backend)
    finally:
        backend.dispose()

    with pytest.raises(ValueError, match="anchor"):
        generator.generate_or_reuse_demo(
            config_path=config_path,
            state_dir=state_dir,
            anchor_date=ANCHOR_DATE + timedelta(days=2),
        )

    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend) == before
    finally:
        backend.dispose()
    assert _manifest_snapshot(state_dir) == manifest_before


@pytest.mark.parametrize(
    ("elapsed_days", "expected_notice"),
    [(15, False), (16, True)],
    ids=["exactly-fifteen-days", "sixteen-days"],
)
def test_generator_stale_manifest_notice_is_non_blocking_and_never_refreshes_state(
    tmp_path: Path,
    elapsed_days: int,
    expected_notice: bool,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    workspace, config_path, environment, _compose_log = _launcher_with_real_generator(
        tmp_path,
        template=clean_demo_state_template,
    )
    first = _run_public_launcher(workspace, environment)
    assert first.returncode == 0, first.stderr
    scenario = clean_demo_state_template.ccloud_scenario
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        ccloud_before = _snapshot_persisted_state(ccloud_backend, scenario)
        self_managed_before = _snapshot_self_managed_state(self_managed_backend)
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()
    manifest = _manifest_path(workspace / ".demo" / "state" / "clean")
    manifest_before = _manifest_snapshot(manifest.parent)
    environment["DEMO_TEST_ANCHOR"] = (ANCHOR_DATE + timedelta(days=elapsed_days)).isoformat()

    second = _run_public_launcher(workspace, environment)

    assert second.returncode == 0, second.stderr
    assert "Demo clean-confluent reused, clean-self-managed reused." in second.stdout
    assert ("days old" in second.stdout) is expected_notice
    assert ("./demo reset --clean" in second.stdout) is expected_notice
    assert _manifest_snapshot(manifest.parent) == manifest_before
    ccloud_backend = _create_real_backend(config_path)
    self_managed_backend = _create_real_backend(config_path, SELF_MANAGED_TENANT_NAME)
    try:
        assert _snapshot_persisted_state(ccloud_backend, scenario) == ccloud_before
        assert _snapshot_self_managed_state(self_managed_backend) == self_managed_before
    finally:
        ccloud_backend.dispose()
        self_managed_backend.dispose()


def test_public_launcher_reports_incompatible_metadata_from_the_real_generator(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    workspace, config_path, environment, _compose_log = _launcher_with_real_generator(
        tmp_path,
        template=clean_demo_state_template,
    )
    first = _run_public_launcher(workspace, environment)
    assert first.returncode == 0, first.stderr
    manifest = _manifest_path(workspace / ".demo" / "state" / "clean")
    metadata = json.loads(manifest.read_text(encoding="utf-8"))
    metadata["schema_version"] = 2
    manifest.write_text(json.dumps(metadata), encoding="utf-8")
    backend = _create_real_backend(config_path)
    try:
        before = _snapshot_persisted_state(backend)
    finally:
        backend.dispose()

    failed = _run_public_launcher(workspace, environment)

    assert failed.returncode == 1
    assert "Demo generation failed: Incompatible clean demo state:" in failed.stderr
    assert "./demo reset --clean" in failed.stderr
    backend = _create_real_backend(config_path)
    try:
        assert _snapshot_persisted_state(backend) == before
    finally:
        backend.dispose()


@pytest.mark.parametrize("profile", [DemoProfile.CLEAN, DemoProfile.SHOWCASE])
def test_supported_public_tag_mutations_survive_reuse_without_any_tag_write(
    tmp_path: Path,
    profile: DemoProfile,
    clean_demo_state_template: _DemoStateTemplate,
    showcase_demo_state_template: _DemoStateTemplate,
) -> None:
    template = clean_demo_state_template if profile is DemoProfile.CLEAN else showcase_demo_state_template
    workspace, config_path, environment, _compose_log = _launcher_with_real_generator(
        tmp_path,
        profile,
        template=template,
    )
    launcher_arguments = ["--showcase"] if profile is DemoProfile.SHOWCASE else []
    scenario = template.ccloud_scenario
    first = _run_public_launcher(workspace, environment, *launcher_arguments)
    assert first.returncode == 0, first.stderr
    settings = load_config(config_path)
    api_prefix = f"/api/v1/tenants/{TENANT_NAME}"
    first_resource, second_resource = scenario.resources[:2]
    with TestClient(create_app(settings, mode="api")) as client:
        single = client.post(
            f"{api_prefix}/entities/resource/{first_resource.resource_id}/tags",
            json={"tag_key": "restart-single", "tag_value": "created", "created_by": "test-user"},
        )
        assert single.status_code == 201
        updated = client.put(
            f"{api_prefix}/entities/resource/{first_resource.resource_id}/tags/restart-single",
            json={"tag_value": "updated"},
        )
        assert updated.status_code == 200
        created_then_deleted = client.post(
            f"{api_prefix}/entities/resource/{first_resource.resource_id}/tags",
            json={"tag_key": "restart-deleted", "tag_value": "temporary", "created_by": "test-user"},
        )
        assert created_then_deleted.status_code == 201
        deleted = client.delete(f"{api_prefix}/entities/resource/{first_resource.resource_id}/tags/restart-deleted")
        assert deleted.status_code == 204
        bulk = client.post(
            f"{api_prefix}/tags/bulk",
            json={
                "items": [
                    {
                        "entity_type": "resource",
                        "entity_id": first_resource.resource_id,
                        "tag_key": "restart-single",
                        "tag_value": "bulk-overridden",
                    },
                    {
                        "entity_type": "resource",
                        "entity_id": second_resource.resource_id,
                        "tag_key": "restart-bulk",
                        "tag_value": "bulk-created",
                    },
                ],
                "override_existing": True,
                "created_by": "bulk-user",
            },
        )
        assert bulk.status_code == 200
        assert bulk.json() == {"created_count": 1, "updated_count": 1, "skipped_count": 0}
        filtered = client.post(
            f"{api_prefix}/tags/bulk-by-filter",
            json={
                "start_date": scenario.start_date.isoformat(),
                "end_date": scenario.anchor_date.isoformat(),
                "timezone": "UTC",
                "tag_key": "restart-filter",
                "display_name": "Filtered",
                "created_by": "filter-user",
                "override_existing": True,
            },
        )
        assert filtered.status_code == 200
        assert filtered.json()["created_count"] > 0

    backend = _create_real_backend(config_path)
    try:
        with backend.create_read_only_unit_of_work() as uow:
            tags_before, total_before = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        tag_snapshot = tuple(sorted(_tag_fields(tag) for tag in tags_before))
        assert total_before == len(tag_snapshot)
    finally:
        backend.dispose()

    state_dir = workspace / ".demo" / "state" / profile.value
    manifest = _manifest_path(state_dir)
    manifest_before = (manifest.read_bytes(), manifest.stat().st_mtime_ns)
    guard_directory = tmp_path / "tag-write-guard"
    guard_directory.mkdir()
    guard_marker = guard_directory / "installed"
    (guard_directory / "sitecustomize.py").write_text(
        "\n".join(
            [
                "from pathlib import Path",
                "import os",
                "from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository",
                "Path(os.environ['DEMO_TAG_WRITE_GUARD']).write_text('installed', encoding='utf-8')",
                "def reject_tag_write(*_args, **_kwargs):",
                "    raise AssertionError('ordinary demo reuse must not open a tag write path')",
                "for method_name in ('add_tag', 'update_tag', 'delete_tag', 'bulk_add_tags'):",
                "    setattr(SQLModelEntityTagRepository, method_name, reject_tag_write)",
                "",
            ]
        ),
        encoding="utf-8",
    )
    environment["PYTHONPATH"] = f"{guard_directory}:{environment['PYTHONPATH']}"
    environment["DEMO_TAG_WRITE_GUARD"] = str(guard_marker)
    environment["DEMO_TEST_ANCHOR"] = (ANCHOR_DATE + timedelta(days=1)).isoformat()

    second = _run_public_launcher(workspace, environment, *launcher_arguments)

    assert second.returncode == 0, second.stderr
    assert guard_marker.read_text(encoding="utf-8") == "installed"
    assert second.stdout.count(" reused") == 2
    assert (manifest.read_bytes(), manifest.stat().st_mtime_ns) == manifest_before
    assert json.loads(manifest.read_text(encoding="utf-8"))["anchor_date"] == ANCHOR_DATE.isoformat()

    backend = _create_real_backend(config_path)
    try:
        with backend.create_read_only_unit_of_work() as uow:
            tags_after, total_after = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        assert total_after == total_before
        assert tuple(sorted(_tag_fields(tag) for tag in tags_after)) == tag_snapshot
    finally:
        backend.dispose()

    with TestClient(create_app(settings, mode="api")) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"


def _launcher_with_real_generator(
    tmp_path: Path,
    profile: DemoProfile = DemoProfile.CLEAN,
    *,
    template: _DemoStateTemplate | None = None,
) -> tuple[Path, Path, dict[str, str], Path]:
    repository_root = Path(__file__).resolve().parents[5]
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    shutil.copy2(repository_root / "demo", workspace / "demo")
    (workspace / "demo").chmod(0o755)
    shutil.copytree(repository_root / "examples" / "demo", workspace / "examples" / "demo")
    state_dir = workspace / ".demo" / "state" / profile.value
    if template is not None:
        shutil.copytree(template.state_dir, state_dir)
    config_path = _write_config(
        workspace / "examples" / "demo",
        state_dir / "confluent.db",
        state_dir / "self-managed.db",
    )
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    compose_log = tmp_path / "compose.log"
    (fake_bin / "date").write_text(
        "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$DEMO_TEST_ANCHOR\"\n",
        encoding="utf-8",
    )
    (fake_bin / "docker").write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >>"$DEMO_COMPOSE_LOG"
case "$*" in
    "compose version")
        ;;
    "compose -f examples/demo/docker-compose.yml build demo-generator chitragupta chitragupta-ui")
        ;;
    "compose -f examples/demo/docker-compose.yml run --rm demo-generator")
        state_dir=$(cd "examples/demo/$DEMO_STATE_DIR" && pwd)
        "$DEMO_PYTHON" -m demo.generator --config "$PWD/examples/demo/config.yaml" \\
            --state-dir "$state_dir" --anchor "$DEMO_ANCHOR_DATE" --profile "$DEMO_PROFILE"
        ;;
    "compose -f examples/demo/docker-compose.yml stop chitragupta chitragupta-ui")
        ;;
    "compose -f examples/demo/docker-compose.yml up --detach --wait --force-recreate chitragupta chitragupta-ui")
        ;;
    *)
        exit 98
        ;;
esac
""",
        encoding="utf-8",
    )
    for executable in (fake_bin / "date", fake_bin / "docker"):
        executable.chmod(0o755)
    environment = os.environ.copy()
    environment.update(
        {
            "PATH": f"{fake_bin}:{environment['PATH']}",
            "PYTHONPATH": str(repository_root / "src"),
            "DEMO_COMPOSE_LOG": str(compose_log),
            "DEMO_PYTHON": sys.executable,
            "DEMO_TEST_ANCHOR": ANCHOR_DATE.isoformat(),
        }
    )
    return workspace, config_path, environment, compose_log


def _run_public_launcher(
    workspace: Path,
    environment: dict[str, str],
    *arguments: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(workspace / "demo"), *arguments],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )


def test_public_launcher_runs_the_real_generator_and_reset_restores_baseline_tags(
    tmp_path: Path,
    clean_demo_state_template: _DemoStateTemplate,
) -> None:
    workspace, config_path, environment, compose_log = _launcher_with_real_generator(
        tmp_path,
        template=clean_demo_state_template,
    )

    first = subprocess.run(
        [str(workspace / "demo")],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )

    assert first.returncode == 0, first.stderr
    settings = load_config(config_path)
    initial_scenario = clean_demo_state_template.ccloud_scenario
    with TestClient(create_app(settings, mode="api")) as client:
        mutation = client.post(
            f"/api/v1/tenants/{TENANT_NAME}/entities/resource/{initial_scenario.resources[0].resource_id}/tags",
            json={"tag_key": "reset-proof", "tag_value": "mutated", "created_by": "test-user"},
        )
        assert mutation.status_code == 201
    environment["DEMO_TEST_ANCHOR"] = (ANCHOR_DATE + timedelta(days=1)).isoformat()
    reset = subprocess.run(
        [str(workspace / "demo"), "reset", "--clean"],
        cwd=workspace,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )

    assert reset.returncode == 0, reset.stderr
    assert "Resetting clean demo state at .demo/state/clean" in reset.stdout
    assert compose_log.read_text(encoding="utf-8").splitlines()[-1] == (
        "compose -f examples/demo/docker-compose.yml up --detach --wait --force-recreate chitragupta chitragupta-ui"
    )
    backend = _create_real_backend(config_path)
    reset_scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE + timedelta(days=1))
    try:
        with backend.create_read_only_unit_of_work() as uow:
            persisted_tags, persisted_total = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        assert persisted_total == len(reset_scenario.entity_tags)
        assert {_tag_fields(tag) for tag in persisted_tags} == {_tag_fields(tag) for tag in reset_scenario.entity_tags}
    finally:
        backend.dispose()
