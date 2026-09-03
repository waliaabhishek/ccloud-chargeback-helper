"""Real-storage and production-API coverage for the Clean Confluent demo."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.config.loader import load_config
from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.repositories import SQLModelEntityTagRepository
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from core.storage.registry import create_storage_backend
from plugins.confluent_cloud.connections import CCloudConnection
from plugins.confluent_cloud.demo.generator import GenerationResult, generate_or_reuse_clean_demo
from plugins.confluent_cloud.demo.scenario import CleanDemoScenario, build_clean_demo_scenario
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository

if TYPE_CHECKING:
    from pytest import MonkeyPatch

    from core.models.chargeback import ChargebackRow
    from core.models.entity_tag import EntityTag
    from plugins.confluent_cloud.models.billing import CCloudBillingLineItem


ECOSYSTEM = "confluent_cloud"
TENANT_NAME = "clean-confluent"
TENANT_ID = "northstar-confluent"
ANCHOR_DATE = date(2026, 9, 2)


def _write_config(tmp_path: Path, db_path: Path) -> Path:
    """Write the one-tenant API-only configuration consumed by production startup."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"""
api:
  host: "127.0.0.1"
  port: 8080
features:
  enable_periodic_refresh: false
tenants:
  {TENANT_NAME}:
    ecosystem: {ECOSYSTEM}
    tenant_id: {TENANT_ID}
    lookback_days: 200
    cutoff_days: 5
    storage:
      backend: sqlmodel
      connection_string: "sqlite:///{db_path}"
    plugin_settings:
      ccloud_api:
        key: "demo-placeholder-key"
        secret: "demo-placeholder-secret"  # pragma: allowlist secret
      topic_attribution:
        enabled: false
""".lstrip(),
        encoding="utf-8",
    )
    return config_path


def _create_real_backend(config_path: Path) -> SQLModelBackend:
    """Create the normal Confluent storage backend for direct persisted assertions."""
    settings = load_config(config_path)
    backend = create_storage_backend(
        settings.tenants[TENANT_NAME].storage,
        storage_module=CCloudStorageModule(),
        use_migrations=False,
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


def _snapshot_persisted_state(backend: SQLModelBackend) -> dict[str, object]:
    """Capture logical repository contents for no-write and fail-closed checks."""
    with backend.create_read_only_unit_of_work() as uow:
        tags, tag_total = uow.tags.find_tags_for_tenant(TENANT_ID, limit=10_000)
        billing, billing_total = uow.billing.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        chargebacks, chargeback_total = uow.chargebacks.find_by_filters(ECOSYSTEM, TENANT_ID, limit=10_000)
        states = uow.pipeline_state.find_by_range(ECOSYSTEM, TENANT_ID, date.min, date.max)
        return {
            "resource_counts": uow.resources.count_by_type(ECOSYSTEM, TENANT_ID),
            "identity_counts": uow.identities.count_by_type(ECOSYSTEM, TENANT_ID),
            "tags": tuple(sorted(_tag_fields(tag) for tag in tags)),
            "tag_total": tag_total,
            "billing": tuple(sorted(_billing_fields(line) for line in billing)),
            "billing_total": billing_total,
            "chargebacks": tuple(sorted(_chargeback_fields(row) for row in chargebacks)),
            "chargeback_total": chargeback_total,
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
        }


def _assert_empty(backend: SQLModelBackend) -> None:
    """Assert that no tenant-owned Clean data survived a rolled-back transaction."""
    snapshot = _snapshot_persisted_state(backend)
    assert snapshot["resource_counts"] == {}
    assert snapshot["identity_counts"] == {}
    assert snapshot["tag_total"] == 0
    assert snapshot["billing_total"] == 0
    assert snapshot["chargeback_total"] == 0
    assert snapshot["pipeline_states"] == ()


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

        actual_states = uow.pipeline_state.find_by_range(
            ECOSYSTEM,
            TENANT_ID,
            scenario.start_date,
            scenario.anchor_date + timedelta(days=1),
        )
        assert actual_states == list(scenario.pipeline_states)

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


@pytest.mark.parametrize("schema_only", [False, True], ids=["new-database", "schema-only-database"])
def test_generator_persists_the_complete_clean_scenario_for_empty_database_states(
    tmp_path: Path,
    schema_only: bool,
) -> None:
    """Empty and schema-only CCloud databases generate one complete tagged scenario."""
    config_path = _write_config(tmp_path, tmp_path / "clean.db")
    if schema_only:
        backend = _create_real_backend(config_path)
        backend.dispose()

    result = generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE)

    assert result is GenerationResult.GENERATED
    backend = _create_real_backend(config_path)
    try:
        _assert_persisted_scenario(
            backend,
            build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE),
        )
    finally:
        backend.dispose()


@pytest.mark.parametrize("requested_anchor", [ANCHOR_DATE, ANCHOR_DATE + timedelta(days=1)])
def test_generator_reuses_a_complete_dataset_from_its_persisted_chargeback_anchor(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
    requested_anchor: date,
) -> None:
    """Same-day and next-day launches validate and reuse the maximum persisted fact date."""
    config_path = _write_config(tmp_path, tmp_path / "clean.db")
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) is GenerationResult.GENERATED

    backend = _create_real_backend(config_path)
    try:
        before = _snapshot_persisted_state(backend)
    finally:
        backend.dispose()

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

    assert result is GenerationResult.REUSED
    assert observed_date_queries
    assert observed_date_queries[-1][-1] == ANCHOR_DATE

    backend = _create_real_backend(config_path)
    try:
        after = _snapshot_persisted_state(backend)
        assert after == before
        _assert_persisted_scenario(
            backend,
            build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE),
        )
    finally:
        backend.dispose()


@pytest.mark.parametrize("partial_state", ["resource", "tag"], ids=["resource-only", "tag-only"])
def test_generator_rejects_nonempty_partial_state_without_repairing_it(
    tmp_path: Path,
    partial_state: str,
) -> None:
    """Representative partial persisted state fails closed and remains unchanged."""
    config_path = _write_config(tmp_path, tmp_path / "partial.db")
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
    config_path = _write_config(tmp_path, tmp_path / "rollback.db")

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

    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) is GenerationResult.GENERATED


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
    """Production startup exposes reconciled tags and tenant-root graph without provider I/O."""
    config_path = _write_config(tmp_path, tmp_path / "api.db")
    scenario = build_clean_demo_scenario(tenant_id=TENANT_ID, anchor_date=ANCHOR_DATE)
    assert generate_or_reuse_clean_demo(config_path=config_path, anchor_date=ANCHOR_DATE) is GenerationResult.GENERATED

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
        tenant = next(item for item in tenants.json()["tenants"] if item["tenant_name"] == TENANT_NAME)
        assert tenant["tenant_id"] == TENANT_ID
        assert tenant["dates_calculated"] == len(scenario.pipeline_states)
        assert tenant["last_calculated_date"] == scenario.anchor_date.isoformat()

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
        assert len(environment_nodes) == 2
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

    assert provider_requests == []
