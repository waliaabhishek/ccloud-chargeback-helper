from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, cast

from sqlalchemy import inspect

from core.config.loader import load_config
from core.storage.backends.sqlmodel.engine import get_or_create_engine
from core.storage.registry import create_storage_backend
from plugins.confluent_cloud.demo.scenario import (
    ECOSYSTEM,
    build_clean_demo_scenario,
)
from plugins.confluent_cloud.storage.module import CCloudStorageModule

if TYPE_CHECKING:
    from core.config.models import AppSettings, TenantConfig
    from core.models import ChargebackRow, EntityTag, PipelineState
    from core.storage.interface import ReadOnlyUnitOfWork, UnitOfWork
    from plugins.confluent_cloud.demo.scenario import CleanDemoScenario
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem


class GenerationResult(StrEnum):
    """Outcome of a Clean demo generation attempt."""

    GENERATED = "generated"
    REUSED = "reused"


def _select_clean_tenant(settings: AppSettings) -> tuple[str, TenantConfig]:
    if len(settings.tenants) != 1:
        raise ValueError("demo configuration must contain exactly one tenant")
    tenant_name, tenant_config = next(iter(settings.tenants.items()))
    if tenant_config.ecosystem != ECOSYSTEM:
        raise ValueError("demo configuration tenant must use the confluent_cloud ecosystem")
    return tenant_name, tenant_config


def _state_is_empty(uow: ReadOnlyUnitOfWork, tenant_id: str) -> bool:
    resource_counts = uow.resources.count_by_type(ECOSYSTEM, tenant_id)
    identity_counts = uow.identities.count_by_type(ECOSYSTEM, tenant_id)
    _billing_rows, billing_total = uow.billing.find_by_filters(ECOSYSTEM, tenant_id, limit=1)
    _chargeback_rows, chargeback_total = uow.chargebacks.find_by_filters(ECOSYSTEM, tenant_id, limit=1)
    _tag_rows, tag_total = uow.tags.find_tags_for_tenant(tenant_id, limit=1)
    pipeline_states = uow.pipeline_state.find_by_range(ECOSYSTEM, tenant_id, date.min, date.max)
    return not (resource_counts or identity_counts or billing_total or chargeback_total or tag_total or pipeline_states)


def _should_use_migrations(connection_string: str) -> bool:
    """Use Alembic for fresh/versioned databases and preserve direct schemas."""
    engine = get_or_create_engine(connection_string)
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())
    return not tables or "alembic_version" in tables


def _resource_counts(uow: ReadOnlyUnitOfWork, tenant_id: str) -> dict[str, tuple[int, int, int]]:
    return {
        resource_type: (counts.total, counts.active, counts.deleted)
        for resource_type, counts in uow.resources.count_by_type(ECOSYSTEM, tenant_id).items()
    }


def _identity_counts(uow: ReadOnlyUnitOfWork, tenant_id: str) -> dict[str, tuple[int, int, int]]:
    return {
        identity_type: (counts.total, counts.active, counts.deleted)
        for identity_type, counts in uow.identities.count_by_type(ECOSYSTEM, tenant_id).items()
    }


def _tag_fields(tag: EntityTag) -> tuple[str, str, str, str, str, str]:
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
        line.ecosystem,
        line.tenant_id,
        line.timestamp,
        line.env_id,
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
        state.calculation_run_id,
        state.topic_overlay_gathered,
        state.topic_attribution_calculated,
    )


def _validate_persisted_scenario(uow: ReadOnlyUnitOfWork, scenario: CleanDemoScenario) -> None:
    """Compare every Clean repository's logical contents with the expected scenario."""
    expected_resource_counts = {
        resource_type: (count, count, 0)
        for resource_type, count in {
            "organization": 1,
            "environment": 2,
            "kafka_cluster": 2,
            "topic": 6,
            "connector": 2,
            "schema_registry": 2,
            "ksqldb_cluster": 1,
            "flink_compute_pool": 1,
            "flink_statement": 1,
        }.items()
    }
    expected_identity_counts = {
        identity_type: (count, count, 0)
        for identity_type, count in {
            "service_account": 3,
            "user": 2,
            "identity_provider": 1,
            "identity_pool": 1,
            "api_key": 4,
        }.items()
    }
    if _resource_counts(uow, scenario.tenant_id) != expected_resource_counts:
        raise ValueError("persisted Clean resources do not match the expected topology")
    if _identity_counts(uow, scenario.tenant_id) != expected_identity_counts:
        raise ValueError("persisted Clean identities do not match the expected topology")

    for expected_resource in scenario.resources:
        actual_resource = uow.resources.get(ECOSYSTEM, scenario.tenant_id, expected_resource.resource_id)
        if actual_resource != expected_resource:
            raise ValueError(f"persisted resource {expected_resource.resource_id!r} does not match Clean state")
    for expected_identity in scenario.identities:
        actual_identity = uow.identities.get(ECOSYSTEM, scenario.tenant_id, expected_identity.identity_id)
        if actual_identity != expected_identity:
            raise ValueError(f"persisted identity {expected_identity.identity_id!r} does not match Clean state")

    actual_tags, tag_total = uow.tags.find_tags_for_tenant(
        scenario.tenant_id,
        limit=max(1000, len(scenario.entity_tags) + 1),
    )
    if tag_total != len(scenario.entity_tags) or {_tag_fields(tag) for tag in actual_tags} != {
        _tag_fields(tag) for tag in scenario.entity_tags
    }:
        raise ValueError("persisted entity tags do not match the expected Clean assignments")

    actual_billing, billing_total = uow.billing.find_by_filters(
        ECOSYSTEM,
        scenario.tenant_id,
        limit=max(1000, len(scenario.billing_lines) + 1),
    )
    if billing_total != len(scenario.billing_lines) or sorted(
        _billing_fields(cast("CCloudBillingLineItem", line)) for line in actual_billing
    ) != sorted(_billing_fields(line) for line in scenario.billing_lines):
        raise ValueError("persisted billing does not match the expected Clean lines")

    actual_chargebacks, chargeback_total = uow.chargebacks.find_by_filters(
        ECOSYSTEM,
        scenario.tenant_id,
        limit=max(1000, len(scenario.chargebacks) + 1),
    )
    if chargeback_total != len(scenario.chargebacks) or sorted(
        _chargeback_fields(row) for row in actual_chargebacks
    ) != sorted(_chargeback_fields(row) for row in scenario.chargebacks):
        raise ValueError("persisted chargebacks do not match the expected Clean allocations")

    actual_states = uow.pipeline_state.find_by_range(ECOSYSTEM, scenario.tenant_id, date.min, date.max)
    if sorted(_pipeline_fields(state) for state in actual_states) != sorted(
        _pipeline_fields(state) for state in scenario.pipeline_states
    ):
        raise ValueError("persisted pipeline state does not match the expected Clean history")


def _persist_scenario(uow: UnitOfWork, scenario: CleanDemoScenario) -> None:
    for resource in scenario.resources:
        uow.resources.upsert(resource)
    for identity in scenario.identities:
        uow.identities.upsert(identity)
    for tag in scenario.entity_tags:
        uow.tags.add_tag(
            tag.tenant_id,
            tag.entity_type,
            tag.entity_id,
            tag.tag_key,
            tag.tag_value,
            tag.created_by,
        )
    for line in scenario.billing_lines:
        uow.billing.upsert(line)
    uow.chargebacks.upsert_batch(list(scenario.chargebacks))
    for state in scenario.pipeline_states:
        uow.pipeline_state.upsert(state)


def generate_or_reuse_clean_demo(*, config_path: Path, anchor_date: date) -> GenerationResult:
    """Generate an empty Clean database or validate and reuse a complete one."""
    settings = load_config(config_path)
    _tenant_name, tenant_config = _select_clean_tenant(settings)
    tenant_id = tenant_config.tenant_id
    connection_string = tenant_config.storage.connection_string.get_secret_value()
    backend = create_storage_backend(
        tenant_config.storage,
        storage_module=CCloudStorageModule(),
        use_migrations=_should_use_migrations(connection_string),
    )
    try:
        backend.create_tables()
        with backend.create_read_only_unit_of_work() as uow:
            empty = _state_is_empty(uow, tenant_id)
            if empty:
                persisted_anchor = anchor_date
            else:
                persisted_dates = uow.chargebacks.get_distinct_dates(ECOSYSTEM, tenant_id)
                if not persisted_dates:
                    raise ValueError("nonempty Clean state has no persisted chargeback dates")
                persisted_anchor = max(persisted_dates)

        scenario = build_clean_demo_scenario(tenant_id=tenant_id, anchor_date=persisted_anchor)
        if empty:
            with backend.create_unit_of_work() as uow:
                _persist_scenario(uow, scenario)
                uow.commit()
            with backend.create_read_only_unit_of_work() as uow:
                _validate_persisted_scenario(uow, scenario)
            return GenerationResult.GENERATED

        with backend.create_read_only_unit_of_work() as uow:
            _validate_persisted_scenario(uow, scenario)
        return GenerationResult.REUSED
    finally:
        backend.dispose()


def _parse_anchor(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("anchor must be an ISO date (YYYY-MM-DD)") from exc


def main(argv: Sequence[str] | None = None) -> int:
    """Run the one-shot Clean generator CLI."""
    parser = argparse.ArgumentParser(description="Generate the Clean Confluent demo state")
    parser.add_argument("--config", type=Path, required=True, help="Path to the demo YAML configuration")
    parser.add_argument("--anchor", type=_parse_anchor, required=True, help="UTC anchor date (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    try:
        result = generate_or_reuse_clean_demo(config_path=args.config, anchor_date=args.anchor)
    except Exception as exc:
        print(f"Clean demo generation failed: {exc}", file=sys.stderr)
        return 1

    print(f"Clean demo {result.value}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
