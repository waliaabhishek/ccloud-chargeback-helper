from __future__ import annotations

import argparse
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import inspect

from core.config.loader import load_config
from core.models import ChargebackRow, CoreBillingLineItem, EntityTag, PipelineState
from core.preview.evidence import PreviewEvidenceScope
from core.preview.organization_authority import OrganizationAuthorityFinalStatus
from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.registry import create_storage_backend
from plugins.confluent_cloud.demo.scenario import (
    ECOSYSTEM as CCLOUD_ECOSYSTEM,
)
from plugins.confluent_cloud.demo.scenario import (
    CleanDemoScenario,
    ConfluentDemoScenario,
    ShowcaseDemoScenario,
    build_clean_demo_scenario,
    build_showcase_demo_scenario,
    validate_clean_demo_scenario,
    validate_showcase_demo_scenario,
)
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.self_managed_kafka.demo.scenario import (
    ECOSYSTEM as SELF_MANAGED_ECOSYSTEM,
)
from plugins.self_managed_kafka.demo.scenario import (
    CleanSelfManagedKafkaScenario,
    build_clean_self_managed_kafka_scenario,
    validate_clean_self_managed_kafka_scenario,
)
from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

if TYPE_CHECKING:
    from core.config.models import AppSettings, TenantConfig
    from core.plugin.protocols import StorageModule
    from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork


class GenerationResult(StrEnum):
    """Outcome of one tenant's deterministic demo generation attempt."""

    GENERATED = "generated"
    REUSED = "reused"


Scenario = ConfluentDemoScenario | CleanSelfManagedKafkaScenario
BillingLine = CCloudBillingLineItem | CoreBillingLineItem


class DemoProfile(StrEnum):
    """Supported deterministic demo data profiles."""

    CLEAN = "clean"
    SHOWCASE = "showcase"


_VALIDATION_BATCH_SIZE = 2048


def _at_midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=UTC)


def _at_end_of_day(day: date) -> datetime:
    return _at_midnight(day) + timedelta(hours=23, minutes=59, seconds=59)


def _select_demo_tenants(settings: AppSettings) -> tuple[tuple[str, TenantConfig], ...]:
    if len(settings.tenants) != 2:
        raise ValueError("demo configuration must contain exactly two tenants")
    selected = tuple(settings.tenants.items())
    ecosystems = {tenant_config.ecosystem for _name, tenant_config in selected}
    if ecosystems != {CCLOUD_ECOSYSTEM, SELF_MANAGED_ECOSYSTEM}:
        raise ValueError("demo configuration must contain one Confluent Cloud and one self-managed Kafka tenant")
    if selected[0][1].ecosystem != CCLOUD_ECOSYSTEM:
        selected = tuple(sorted(selected, key=lambda item: item[1].ecosystem != CCLOUD_ECOSYSTEM))
    return selected


def _tenant_parts(tenant_config: TenantConfig) -> tuple[str, str, StorageModule]:
    if tenant_config.ecosystem == CCLOUD_ECOSYSTEM:
        return (
            CCLOUD_ECOSYSTEM,
            tenant_config.tenant_id,
            CCloudStorageModule(),
        )
    if tenant_config.ecosystem == SELF_MANAGED_ECOSYSTEM:
        return (
            SELF_MANAGED_ECOSYSTEM,
            tenant_config.tenant_id,
            SelfManagedKafkaStorageModule(),
        )
    raise ValueError(f"unsupported demo ecosystem: {tenant_config.ecosystem!r}")


def _state_is_empty(uow: ReadOnlyUnitOfWork, ecosystem: str, tenant_id: str, tenant_name: str) -> bool:
    resource_counts = uow.resources.count_by_type(ecosystem, tenant_id)
    identity_counts = uow.identities.count_by_type(ecosystem, tenant_id)
    _billing_rows, billing_total = uow.billing.find_by_filters(ecosystem, tenant_id, limit=1)
    _chargeback_rows, chargeback_total = uow.chargebacks.find_by_filters(ecosystem, tenant_id, limit=1)
    _tag_rows, tag_total = uow.tags.find_tags_for_tenant(tenant_id, limit=1)
    _topic_rows, topic_total = uow.topic_attributions.find_by_filters(ecosystem, tenant_id, limit=1)
    pipeline_states = uow.pipeline_state.find_by_range(ecosystem, tenant_id, date.min, date.max)
    pipeline_runs = uow.pipeline_runs.list_runs_for_tenant(tenant_name, limit=1)
    return not (
        resource_counts
        or identity_counts
        or billing_total
        or chargeback_total
        or tag_total
        or topic_total
        or pipeline_states
        or pipeline_runs
    )


def _should_use_migrations(connection_string: str) -> bool:
    """Use Alembic for fresh/versioned databases and preserve direct schemas."""
    engine = get_or_create_engine(connection_string)
    tables = set(inspect(engine).get_table_names())
    return not tables or "alembic_version" in tables


def _resource_counts(uow: ReadOnlyUnitOfWork, ecosystem: str, tenant_id: str) -> dict[str, tuple[int, int, int]]:
    return {
        resource_type: (counts.total, counts.active, counts.deleted)
        for resource_type, counts in uow.resources.count_by_type(ecosystem, tenant_id).items()
    }


def _identity_counts(uow: ReadOnlyUnitOfWork, ecosystem: str, tenant_id: str) -> dict[str, tuple[int, int, int]]:
    return {
        identity_type: (counts.total, counts.active, counts.deleted)
        for identity_type, counts in uow.identities.count_by_type(ecosystem, tenant_id).items()
    }


def _tag_fields(tag: EntityTag) -> tuple[str, str, str, str, str, str]:
    return (tag.tenant_id, tag.entity_type, tag.entity_id, tag.tag_key, tag.tag_value, tag.created_by)


def _billing_fields(line: BillingLine) -> tuple[object, ...]:
    return (
        line.ecosystem,
        line.tenant_id,
        line.timestamp,
        getattr(line, "env_id", ""),
        line.resource_id,
        line.product_category,
        line.product_type,
        line.quantity,
        line.unit_price,
        line.total_cost,
        line.currency,
        line.granularity,
        tuple(sorted(line.metadata.items())),
    )


def _chargeback_fields(row: ChargebackRow) -> tuple[object, ...]:
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
        tuple(sorted(row.tags.items())),
        tuple(sorted(row.metadata.items())),
    )


def _topic_fields(row: Any) -> tuple[object, ...]:
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
        tuple(sorted(row.metadata.items())),
    )


def _pipeline_fields(state: PipelineState) -> tuple[object, ...]:
    return (
        state.ecosystem,
        state.tenant_id,
        state.tracking_date,
        state.billing_gathered,
        state.resources_gathered,
        state.chargeback_calculated,
        state.calculation_id,
        state.calculation_completed_at,
        state.topic_overlay_gathered,
        state.topic_attribution_calculated,
    )


def _expected_counts(scenario: Scenario) -> tuple[dict[str, tuple[int, int, int]], dict[str, tuple[int, int, int]]]:
    resource_counts: dict[str, tuple[int, int, int]] = {}
    resources_by_type: dict[str, list[Any]] = {}
    for resource in scenario.resources:
        resources_by_type.setdefault(resource.resource_type, []).append(resource)
    for resource_type, resources in resources_by_type.items():
        resource_counts[resource_type] = (
            len(resources),
            sum(resource.status.value == "active" for resource in resources),
            sum(resource.status.value == "deleted" for resource in resources),
        )
    identity_counts: dict[str, tuple[int, int, int]] = {}
    identities_by_type: dict[str, list[Any]] = {}
    for identity in scenario.identities:
        identities_by_type.setdefault(identity.identity_type, []).append(identity)
    for identity_type, identities in identities_by_type.items():
        identity_counts[identity_type] = (
            len(identities),
            sum(identity.deleted_at is None for identity in identities),
            sum(identity.deleted_at is not None for identity in identities),
        )
    return (
        resource_counts,
        identity_counts,
    )


def _validate_scenario(scenario: Scenario) -> None:
    """Dispatch profile-specific pure validation before persistence or reuse."""
    if isinstance(scenario, ShowcaseDemoScenario):
        validate_showcase_demo_scenario(scenario)
    elif isinstance(scenario, CleanDemoScenario):
        validate_clean_demo_scenario(scenario)
    elif isinstance(scenario, CleanSelfManagedKafkaScenario):
        validate_clean_self_managed_kafka_scenario(scenario)
    else:
        raise TypeError(f"unsupported demo scenario: {type(scenario).__name__}")


def _build_scenario(
    *,
    ecosystem: str,
    tenant_id: str,
    anchor_date: date,
    profile: DemoProfile,
) -> Scenario:
    """Build the selected profile for one configured tenant."""
    if ecosystem == CCLOUD_ECOSYSTEM:
        if profile is DemoProfile.SHOWCASE:
            return build_showcase_demo_scenario(tenant_id=tenant_id, anchor_date=anchor_date)
        return build_clean_demo_scenario(tenant_id=tenant_id, anchor_date=anchor_date)
    if ecosystem == SELF_MANAGED_ECOSYSTEM:
        return build_clean_self_managed_kafka_scenario(tenant_id=tenant_id, anchor_date=anchor_date)
    raise ValueError(f"unsupported demo ecosystem: {ecosystem!r}")


def _validate_persisted_scenario(
    uow: ReadOnlyUnitOfWork,
    tenant_name: str,
    scenario: Scenario,
) -> None:
    """Validate stored state through bounded repository reads."""
    if isinstance(scenario, (CleanDemoScenario, ShowcaseDemoScenario)):
        ecosystem = CCLOUD_ECOSYSTEM
    else:
        ecosystem = SELF_MANAGED_ECOSYSTEM
    _validate_scenario(scenario)
    profile_name = "Showcase" if isinstance(scenario, ShowcaseDemoScenario) else "Clean"
    expected_resource_counts, expected_identity_counts = _expected_counts(scenario)
    if _resource_counts(uow, ecosystem, scenario.tenant_id) != expected_resource_counts:
        raise ValueError(f"persisted {profile_name} resources do not match the expected topology")
    if _identity_counts(uow, ecosystem, scenario.tenant_id) != expected_identity_counts:
        raise ValueError(f"persisted {profile_name} identities do not match the expected topology")

    for expected_resource in scenario.resources:
        actual_resource = uow.resources.get(ecosystem, scenario.tenant_id, expected_resource.resource_id)
        if actual_resource != expected_resource:
            raise ValueError(f"persisted {profile_name} resources do not match the expected topology")
    for expected_identity in scenario.identities:
        actual_identity = uow.identities.get(ecosystem, scenario.tenant_id, expected_identity.identity_id)
        if actual_identity != expected_identity:
            raise ValueError(f"persisted {profile_name} identities do not match the expected topology")

    actual_tags, tag_total = uow.tags.find_tags_for_tenant(
        scenario.tenant_id,
        limit=max(1, len(scenario.entity_tags)),
    )
    if tag_total != len(scenario.entity_tags) or {_tag_fields(tag) for tag in actual_tags} != {
        _tag_fields(tag) for tag in scenario.entity_tags
    }:
        if profile_name == "Clean":
            raise ValueError("persisted entity tags do not match the expected Clean assignments")
        raise ValueError("persisted entity tags do not match the expected Showcase assignments")

    _one_billing, billing_total = uow.billing.find_by_filters(ecosystem, scenario.tenant_id, limit=1)
    if billing_total != len(scenario.billing_lines):
        if profile_name == "Clean":
            raise ValueError("persisted billing does not match the expected Clean lines")
        raise ValueError("persisted billing does not match the expected Showcase lines")
    expected_billing_by_date: dict[date, Counter[tuple[object, ...]]] = {}
    for line in scenario.billing_lines:
        expected_billing_by_date.setdefault(line.timestamp.date(), Counter())[_billing_fields(line)] += 1
    traversed_billing = 0
    for tracking_date, expected_rows in expected_billing_by_date.items():
        actual_rows = uow.billing.find_by_date(ecosystem, scenario.tenant_id, tracking_date)
        traversed_billing += len(actual_rows)
        actual_counter = Counter(_billing_fields(cast("BillingLine", line)) for line in actual_rows)
        if actual_counter != expected_rows:
            if profile_name == "Clean":
                raise ValueError("persisted billing does not match the expected Clean lines")
            raise ValueError("persisted billing does not match the expected Showcase lines")
    if traversed_billing != billing_total:
        if profile_name == "Clean":
            raise ValueError("persisted billing does not match the expected Clean lines")
        raise ValueError("persisted Showcase billing contains unexpected dates")

    _one_chargeback, chargeback_total = uow.chargebacks.find_by_filters(ecosystem, scenario.tenant_id, limit=1)
    if chargeback_total != len(scenario.chargebacks):
        if profile_name == "Clean":
            raise ValueError("persisted chargebacks do not match the expected Clean allocations")
        raise ValueError("persisted Showcase chargebacks do not match the expected allocations")
    expected_chargebacks = Counter(_chargeback_fields(row) for row in scenario.chargebacks)
    streamed_chargebacks = 0
    for row in uow.chargebacks.iter_by_filters(
        ecosystem,
        scenario.tenant_id,
        batch_size=_VALIDATION_BATCH_SIZE,
    ):
        key = _chargeback_fields(row)
        if expected_chargebacks[key] <= 0:
            if profile_name == "Clean":
                raise ValueError("persisted chargebacks do not match the expected Clean allocations")
            raise ValueError("persisted Showcase chargebacks do not match the expected allocations")
        expected_chargebacks[key] -= 1
        streamed_chargebacks += 1
    if streamed_chargebacks != chargeback_total or +expected_chargebacks:
        if profile_name == "Clean":
            raise ValueError("persisted chargebacks do not match the expected Clean allocations")
        raise ValueError("persisted Showcase chargebacks do not match the expected allocations")

    _one_topics, topic_total = uow.topic_attributions.find_by_filters(ecosystem, scenario.tenant_id, limit=1)
    if topic_total != len(scenario.topic_attributions):
        if profile_name == "Clean":
            raise ValueError("persisted topic attributions do not match the expected Clean overlay")
        raise ValueError("persisted Showcase topic attributions do not match the expected overlay")
    expected_topics = Counter(_topic_fields(row) for row in scenario.topic_attributions)
    streamed_topics = 0
    for topic_row in uow.topic_attributions.iter_by_filters(
        ecosystem,
        scenario.tenant_id,
        batch_size=_VALIDATION_BATCH_SIZE,
    ):
        key = _topic_fields(topic_row)
        if expected_topics[key] <= 0:
            if profile_name == "Clean":
                raise ValueError("persisted topic attributions do not match the expected Clean overlay")
            raise ValueError("persisted Showcase topic attributions do not match the expected overlay")
        expected_topics[key] -= 1
        streamed_topics += 1
    if streamed_topics != topic_total or +expected_topics:
        if profile_name == "Clean":
            raise ValueError("persisted topic attributions do not match the expected Clean overlay")
        raise ValueError("persisted Showcase topic attributions do not match the expected overlay")

    actual_states = uow.pipeline_state.find_by_range(ecosystem, scenario.tenant_id, date.min, date.max)
    if sorted(_pipeline_fields(state) for state in actual_states) != sorted(
        _pipeline_fields(state) for state in scenario.pipeline_states
    ):
        raise ValueError("persisted pipeline state does not match the expected Clean history")
    latest_run = uow.pipeline_runs.get_latest_run(tenant_name)
    if (
        latest_run is None
        or latest_run.id is None
        or latest_run.id <= 0
        or latest_run.tenant_name != tenant_name
        or latest_run.status != "completed"
        or latest_run.started_at != _at_midnight(scenario.start_date)
        or latest_run.ended_at != _at_end_of_day(scenario.anchor_date)
        or latest_run.stage is not None
        or latest_run.current_date is not None
        or latest_run.dates_gathered != len(scenario.pipeline_states)
        or latest_run.dates_calculated != len(scenario.pipeline_states)
        or latest_run.rows_written != len(scenario.chargebacks)
        or latest_run.error_message is not None
    ):
        raise ValueError("persisted PipelineRun does not match the expected completed run")
    if {state.calculation_run_id for state in actual_states} != {latest_run.id}:
        raise ValueError("persisted pipeline state is not linked to the completed PipelineRun")


def _persist_scenario(uow: UnitOfWork, tenant_name: str, scenario: Scenario) -> None:
    for resource in scenario.resources:
        uow.resources.upsert(resource)
    for identity in scenario.identities:
        uow.identities.upsert(identity)
    for tag in scenario.entity_tags:
        uow.tags.add_tag(tag.tenant_id, tag.entity_type, tag.entity_id, tag.tag_key, tag.tag_value, tag.created_by)
    for line in scenario.billing_lines:
        uow.billing.upsert(line)
    uow.chargebacks.upsert_batch(list(scenario.chargebacks))
    uow.topic_attributions.upsert_batch(list(scenario.topic_attributions))
    run = uow.pipeline_runs.create_run(tenant_name, _at_midnight(scenario.start_date))
    if run.id is None:
        raise RuntimeError("generated PipelineRun did not receive an id")
    for state in scenario.pipeline_states:
        uow.pipeline_state.upsert(replace(state, calculation_run_id=run.id))
    completed_at = _at_end_of_day(scenario.anchor_date)
    uow.pipeline_runs.update_run(
        replace(
            run,
            status="completed",
            ended_at=completed_at,
            stage=None,
            current_date=None,
            dates_gathered=len(scenario.pipeline_states),
            dates_calculated=len(scenario.pipeline_states),
            rows_written=len(scenario.chargebacks),
            error_message=None,
        )
    )


def _persist_ccloud_preview(backend: SQLModelBackend, scenario: ConfluentDemoScenario) -> None:
    captured_at = _at_midnight(scenario.anchor_date + timedelta(days=1)) + timedelta(hours=1)
    with backend.create_preview_evidence_unit_of_work() as uow:
        source_attempt = uow.source_readiness.begin_attempt(
            CCLOUD_ECOSYSTEM,
            scenario.tenant_id,
            f"clean-demo:{scenario.start_date.isoformat()}:{scenario.anchor_date.isoformat()}",
            scenario.preview_source_capture.refresh_start,
            scenario.preview_source_capture.refresh_end,
            scenario.organization_authority_at,
        )
        authority_attempt = uow.organization_authority.begin(
            CCLOUD_ECOSYSTEM,
            scenario.tenant_id,
            scenario.organization_authority_at,
        )
        scenario.preview_source_capture.persist(
            uow.source_windows,
            uow.source_readiness,
            attempt_sequence=source_attempt.attempt_sequence,
            captured_at=captured_at,
        )
        states_by_date = {state.tracking_date: state for state in scenario.pipeline_states}
        for lineage in scenario.allocation_lineage_runs:
            state = states_by_date[lineage.tracking_date]
            if state.calculation_completed_at is None:
                raise ValueError("generated pipeline state has no completion timestamp")
            uow.allocation_lineage.replace_calculation_lineage(
                lineage,
                calculation_completed_at=state.calculation_completed_at,
            )
        uow.organization_authority.finalize(
            authority_attempt.attempt_sequence,
            OrganizationAuthorityFinalStatus.AVAILABLE,
            completed_at=captured_at,
            organization_id=scenario.organization_authority_id,
            reason=None,
        )
        uow.commit()


def _validate_ccloud_preview(backend: SQLModelBackend, scenario: ConfluentDemoScenario) -> None:
    scope = PreviewEvidenceScope(
        CCLOUD_ECOSYSTEM,
        scenario.tenant_id,
        scenario.preview_source_capture.refresh_start,
        scenario.preview_source_capture.refresh_end,
    )
    with backend.create_preview_generation_read_unit_of_work() as uow:
        sources = tuple(uow.cost_evidence.iter_preview_sources(scope))
        if len(sources) != len(scenario.preview_source_capture.records):
            raise ValueError("preview evidence does not match the generated source records")
        if {source.source_record_id for source in sources} != {
            record.source_record_id for record in scenario.preview_source_capture.records
        }:
            raise ValueError("preview evidence does not match the generated source records")
        calculation_ids = tuple(state.calculation_id for state in scenario.pipeline_states if state.calculation_id)
        runs = tuple(uow.allocation_evidence.iter_preview_allocation_runs(scope, calculation_ids))
        if len(runs) != len(scenario.allocation_lineage_runs) or any(
            run.capture_status.value != "complete" for run in runs
        ):
            raise ValueError("preview evidence allocation lineage is incomplete")
        readiness = uow.source_readiness.list_covering(
            CCLOUD_ECOSYSTEM,
            scenario.tenant_id,
            scope.start,
            scope.end,
        )
        if not readiness or sum(item.source_count for item in readiness) != len(sources):
            raise ValueError("preview evidence source readiness is incomplete")
        authority = uow.organization_authority.get_latest(CCLOUD_ECOSYSTEM, scenario.tenant_id)
        if (
            authority is None
            or authority.organization_id != scenario.organization_authority_id
            or authority.status.value != "available"
        ):
            raise ValueError("preview evidence organization authority is incomplete")


def _persist_or_validate_tenant(
    tenant_name: str,
    tenant_config: TenantConfig,
    scenario: Scenario,
    profile: DemoProfile,
) -> GenerationResult:
    ecosystem, tenant_id, storage_module = _tenant_parts(tenant_config)
    connection_string = tenant_config.storage.connection_string.get_secret_value()
    backend = create_storage_backend(
        tenant_config.storage,
        storage_module=storage_module,
        use_migrations=_should_use_migrations(connection_string),
        focus_preview_enabled=tenant_config.focus_preview_enabled,
    )
    if not isinstance(backend, SQLModelBackend):
        raise TypeError("demo generation requires the SQLModel storage backend")
    try:
        backend.create_tables()
        with backend.create_read_only_unit_of_work() as uow:
            empty = _state_is_empty(uow, ecosystem, tenant_id, tenant_name)
            if empty:
                persisted_anchor = scenario.anchor_date
            else:
                persisted_dates = uow.chargebacks.get_distinct_dates(ecosystem, tenant_id)
                if not persisted_dates:
                    if isinstance(scenario, ShowcaseDemoScenario):
                        raise ValueError("nonempty Showcase state has no persisted chargeback dates")
                    raise ValueError("nonempty Clean state has no persisted chargeback dates")
                persisted_anchor = max(persisted_dates)
        if persisted_anchor != scenario.anchor_date:
            scenario = _build_scenario(
                ecosystem=ecosystem,
                tenant_id=tenant_id,
                anchor_date=persisted_anchor,
                profile=profile,
            )
        if empty:
            _validate_scenario(scenario)
            with backend.create_unit_of_work() as uow:
                _persist_scenario(uow, tenant_name, scenario)
                uow.commit()
            if ecosystem == CCLOUD_ECOSYSTEM:
                _persist_ccloud_preview(backend, cast("ConfluentDemoScenario", scenario))
            with backend.create_read_only_unit_of_work() as uow:
                _validate_persisted_scenario(uow, tenant_name, scenario)
            if ecosystem == CCLOUD_ECOSYSTEM:
                _validate_ccloud_preview(backend, cast("ConfluentDemoScenario", scenario))
            return GenerationResult.GENERATED

        with backend.create_read_only_unit_of_work() as uow:
            _validate_persisted_scenario(uow, tenant_name, scenario)
        if ecosystem == CCLOUD_ECOSYSTEM:
            _validate_ccloud_preview(backend, cast("ConfluentDemoScenario", scenario))
        return GenerationResult.REUSED
    finally:
        backend.dispose()


def generate_or_reuse_demo(
    *,
    config_path: Path,
    anchor_date: date,
    profile: DemoProfile = DemoProfile.CLEAN,
) -> dict[str, GenerationResult]:
    """Generate or validate the two deterministic demo tenant databases."""
    settings = load_config(config_path)
    selected = _select_demo_tenants(settings)
    results: dict[str, GenerationResult] = {}
    for tenant_name, tenant_config in selected:
        scenario = _build_scenario(
            ecosystem=tenant_config.ecosystem,
            tenant_id=tenant_config.tenant_id,
            anchor_date=anchor_date,
            profile=profile,
        )
        results[tenant_name] = _persist_or_validate_tenant(tenant_name, tenant_config, scenario, profile)
    return results


def generate_or_reuse_clean_demo(*, config_path: Path, anchor_date: date) -> dict[str, GenerationResult]:
    """Compatibility wrapper for the default Clean demo profile."""
    return generate_or_reuse_demo(
        config_path=config_path,
        anchor_date=anchor_date,
        profile=DemoProfile.CLEAN,
    )


def _parse_anchor(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("anchor must be an ISO date (YYYY-MM-DD)") from exc


def main(argv: Sequence[str] | None = None) -> int:
    """Run the one-shot demo generator CLI."""
    parser = argparse.ArgumentParser(description="Generate deterministic demo state")
    parser.add_argument("--config", type=Path, required=True, help="Path to the demo YAML configuration")
    parser.add_argument("--anchor", type=_parse_anchor, required=True, help="UTC anchor date (YYYY-MM-DD)")
    parser.add_argument(
        "--profile",
        type=DemoProfile,
        choices=tuple(DemoProfile),
        default=DemoProfile.CLEAN,
        help="Deterministic demo profile to generate",
    )
    args = parser.parse_args(argv)
    try:
        results = generate_or_reuse_demo(config_path=args.config, anchor_date=args.anchor, profile=args.profile)
    except Exception as exc:
        print(f"Demo generation failed: {exc}", file=sys.stderr)
        return 1
    print(f"Demo {', '.join(f'{name} {result.value}' for name, result in results.items())}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
