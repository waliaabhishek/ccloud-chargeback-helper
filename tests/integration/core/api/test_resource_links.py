from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from core.api.app import create_app
from core.api.routes import resource_links
from core.config.models import AppSettings, StorageConfig, TenantConfig
from core.models.identity import CoreIdentity, Identity
from core.models.resource import CoreResource, Resource, ResourceStatus
from core.plugin.registry import PluginRegistry
from core.storage.backend_provider import ApiTenantBackendProvider
from core.storage.backends.sqlmodel.repositories import (
    SQLModelIdentityRepository,
    SQLModelResourceRepository,
)
from core.storage.backends.sqlmodel.unit_of_work import ReadOnlySQLModelUnitOfWork
from tests.integration.core.api.backend_provider import FixedTenantBackendProvider

if TYPE_CHECKING:
    from pathlib import Path

    from httpx import Response

    from core.api.schemas import ResourceLinkResourceResponse
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from core.storage.interface import StorageBackend


_PATH = "/api/v1/tenants/{tenant_name}/resource-links/resolve"
_RESOLVE_PATH = _PATH.format(tenant_name="test-tenant")
_AT = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)
_MISSING = object()


def _settings(*tenant_names: str) -> AppSettings:
    return AppSettings(
        tenants={
            tenant_name: TenantConfig(
                ecosystem="test-eco",
                tenant_id=tenant_name,
                storage=StorageConfig(connection_string=f"sqlite:////tmp/{tenant_name}-resource-links.db"),
            )
            for tenant_name in tenant_names
        }
    )


def _resource(
    resource_id: str,
    *,
    ecosystem: str = "test-eco",
    tenant_id: str = "test-tenant",
    resource_type: str = "kafka_cluster",
    parent_id: str | None = "env-a",
    metadata: dict[str, Any] | None = None,
    deleted_at: datetime | None = None,
) -> CoreResource:
    return CoreResource(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=f"display-{resource_id}",
        parent_id=parent_id,
        owner_id="owner-a",
        status=ResourceStatus.ACTIVE,
        created_at=_AT,
        deleted_at=deleted_at,
        last_seen_at=_AT,
        metadata={} if metadata is None else metadata,
    )


def _identity(
    identity_id: str,
    *,
    tenant_id: str = "test-tenant",
    identity_type: str = "user",
    deleted_at: datetime | None = None,
) -> CoreIdentity:
    return CoreIdentity(
        ecosystem="test-eco",
        tenant_id=tenant_id,
        identity_id=identity_id,
        identity_type=identity_type,
        display_name=f"display-{identity_id}",
        created_at=_AT,
        deleted_at=deleted_at,
        last_seen_at=_AT,
        metadata={"private": "must-not-leak"},
    )


def _seed(
    backend: SQLModelBackend,
    *,
    resources: Sequence[CoreResource] = (),
    identities: Sequence[CoreIdentity] = (),
) -> None:
    with backend.create_unit_of_work() as uow:
        for resource in resources:
            uow.resources.upsert(resource)
        for identity in identities:
            uow.identities.upsert(identity)
        uow.commit()


class _RejectingProvider:
    """Tenant provider that records any forbidden runtime access."""

    def __init__(self) -> None:
        self.acquisitions: list[str] = []

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[StorageBackend]:
        del tenant_config
        self.acquisitions.append(tenant_name)
        raise AssertionError("request validation accessed a backend")
        yield cast("StorageBackend", None)

    def close(self) -> None:
        return None


class _FailingProvider:
    """Complete tenant-provider protocol with a controlled lease-entry failure."""

    def __init__(self) -> None:
        self.acquisitions: list[str] = []

    @contextmanager
    def acquire_backend(
        self,
        tenant_name: str,
        tenant_config: TenantConfig,
    ) -> Iterator[StorageBackend]:
        del tenant_config
        self.acquisitions.append(tenant_name)
        raise RuntimeError("backend acquisition failed")
        yield cast("StorageBackend", None)

    def close(self) -> None:
        return None


def _error_body(response: Response) -> dict[str, Any]:
    body = cast("dict[str, Any]", response.json())
    assert body["detail"] == "Internal server error"
    assert set(body) == {"detail", "error_id"}
    assert str(UUID(body["error_id"])) == body["error_id"]
    return body


class TestResolveResourceLinks:
    def test_returns_minimal_active_rows_in_request_first_occurrence_order(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        _seed(
            in_memory_backend,
            resources=(
                _resource("cluster-a", metadata={"private": "nope"}),
                _resource("collision", resource_type="environment", parent_id=None),
                _resource(
                    "ksql-a",
                    resource_type="ksqldb_cluster",
                    metadata={"kafka_cluster_id": "cluster-a", "private": "nope"},
                ),
            ),
            identities=(
                _identity("user-a"),
                _identity("collision", identity_type="service_account"),
            ),
        )

        response = app_with_backend.post(
            _RESOLVE_PATH,
            json={"identifiers": ["cluster-a", "user-a", "cluster-a", "collision", "ksql-a"]},
        )

        assert response.status_code == 200
        body = response.json()
        assert list(body) == ["resources", "identities"]
        assert list(body["resources"]) == ["cluster-a", "collision", "ksql-a"]
        assert list(body["identities"]) == ["user-a", "collision"]
        assert body == {
            "resources": {
                "cluster-a": {
                    "resource_type": "kafka_cluster",
                    "parent_id": "env-a",
                    "kafka_cluster_id": None,
                },
                "collision": {
                    "resource_type": "environment",
                    "parent_id": None,
                    "kafka_cluster_id": None,
                },
                "ksql-a": {
                    "resource_type": "ksqldb_cluster",
                    "parent_id": "env-a",
                    "kafka_cluster_id": "cluster-a",
                },
            },
            "identities": {
                "user-a": {"identity_type": "user"},
                "collision": {"identity_type": "service_account"},
            },
        }

    @pytest.mark.parametrize(
        "metadata",
        (
            {"kafka_cluster_id": ""},
            {"kafka_cluster_id": 7},
            {"kafka_cluster_id": None},
            {},
        ),
    )
    def test_normalizes_invalid_or_empty_ksqldb_parent_cluster_metadata_to_null(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        metadata: dict[str, object],
    ) -> None:
        _seed(
            in_memory_backend,
            resources=(_resource("ksql-a", resource_type="ksqldb_cluster", metadata=metadata),),
        )

        response = app_with_backend.post(_RESOLVE_PATH, json={"identifiers": ["ksql-a"]})

        assert response.status_code == 200
        assert response.json() == {
            "resources": {
                "ksql-a": {
                    "resource_type": "ksqldb_cluster",
                    "parent_id": "env-a",
                    "kafka_cluster_id": None,
                }
            },
            "identities": {},
        }

    def test_omits_deleted_and_unknown_rows_but_returns_unsupported_matches(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        _seed(
            in_memory_backend,
            resources=(
                _resource("deleted-resource", deleted_at=_AT),
                _resource("connector-a", resource_type="connector"),
            ),
            identities=(
                _identity("deleted-identity", deleted_at=_AT),
                _identity("pool-a", identity_type="identity_pool"),
            ),
        )

        response = app_with_backend.post(
            _RESOLVE_PATH,
            json={
                "identifiers": [
                    "deleted-resource",
                    "deleted-identity",
                    "unknown",
                    "connector-a",
                    "pool-a",
                ]
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "resources": {
                "connector-a": {
                    "resource_type": "connector",
                    "parent_id": "env-a",
                    "kafka_cluster_id": None,
                }
            },
            "identities": {"pool-a": {"identity_type": "identity_pool"}},
        }

    def test_scopes_identical_identifiers_to_the_requested_tenant(
        self,
        in_memory_backend: SQLModelBackend,
    ) -> None:
        _seed(
            in_memory_backend,
            resources=(
                _resource("shared-resource", tenant_id="tenant-a", parent_id="env-a"),
                _resource("shared-resource", tenant_id="tenant-b", parent_id="env-b"),
            ),
            identities=(
                _identity("shared-identity", tenant_id="tenant-a", identity_type="user"),
                _identity("shared-identity", tenant_id="tenant-b", identity_type="service_account"),
            ),
        )
        provider = FixedTenantBackendProvider({"tenant-a": in_memory_backend, "tenant-b": in_memory_backend})
        app = create_app(_settings("tenant-a", "tenant-b"))

        with TestClient(app) as client:
            app.state.backend_provider = provider
            tenant_a = client.post(
                _PATH.format(tenant_name="tenant-a"),
                json={"identifiers": ["shared-resource", "shared-identity"]},
            )
            tenant_b = client.post(
                _PATH.format(tenant_name="tenant-b"),
                json={"identifiers": ["shared-resource", "shared-identity"]},
            )

        assert tenant_a.status_code == 200
        assert tenant_a.json() == {
            "resources": {
                "shared-resource": {
                    "resource_type": "kafka_cluster",
                    "parent_id": "env-a",
                    "kafka_cluster_id": None,
                }
            },
            "identities": {"shared-identity": {"identity_type": "user"}},
        }
        assert tenant_b.status_code == 200
        assert tenant_b.json() == {
            "resources": {
                "shared-resource": {
                    "resource_type": "kafka_cluster",
                    "parent_id": "env-b",
                    "kafka_cluster_id": None,
                }
            },
            "identities": {"shared-identity": {"identity_type": "service_account"}},
        }
        assert provider.acquisitions == ["tenant-a", "tenant-b"]

    def test_uses_one_real_batch_query_per_repository_for_a_large_catalog(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed(
            in_memory_backend,
            resources=tuple(_resource(f"resource-{index:03d}") for index in range(140)) + (_resource("collision"),),
            identities=tuple(_identity(f"identity-{index:03d}") for index in range(140))
            + (_identity("collision", identity_type="service_account"),),
        )
        resource_calls: list[tuple[str, str, tuple[str, ...]]] = []
        identity_calls: list[tuple[str, str, tuple[str, ...]]] = []
        original_resource_get_many = SQLModelResourceRepository.get_many
        original_identity_get_many = SQLModelIdentityRepository.get_many

        def count_resource_get_many(
            repository: SQLModelResourceRepository,
            ecosystem: str,
            tenant_id: str,
            resource_ids: Sequence[str],
        ) -> dict[str, Resource]:
            resource_calls.append((ecosystem, tenant_id, tuple(resource_ids)))
            return cast(
                "dict[str, Resource]", original_resource_get_many(repository, ecosystem, tenant_id, resource_ids)
            )

        def count_identity_get_many(
            repository: SQLModelIdentityRepository,
            ecosystem: str,
            tenant_id: str,
            identity_ids: Sequence[str],
        ) -> dict[str, Identity]:
            identity_calls.append((ecosystem, tenant_id, tuple(identity_ids)))
            return cast(
                "dict[str, Identity]", original_identity_get_many(repository, ecosystem, tenant_id, identity_ids)
            )

        monkeypatch.setattr(SQLModelResourceRepository, "get_many", count_resource_get_many)
        monkeypatch.setattr(SQLModelIdentityRepository, "get_many", count_identity_get_many)
        requested = ["resource-001", "identity-001", "collision", "unknown"]

        response = app_with_backend.post(_RESOLVE_PATH, json={"identifiers": requested})

        assert response.status_code == 200
        body = response.json()
        assert resource_calls == [("test-eco", "test-tenant", tuple(requested))]
        assert identity_calls == [("test-eco", "test-tenant", tuple(requested))]
        assert len(body["resources"]) <= len(requested)
        assert len(body["identities"]) <= len(requested)
        assert len(set(body["resources"]) | set(body["identities"])) <= len(requested)
        assert len(body["resources"]) + len(body["identities"]) <= 2 * len(requested)
        assert body["resources"]["collision"]["resource_type"] == "kafka_cluster"
        assert body["identities"]["collision"] == {"identity_type": "service_account"}

    def test_projects_the_response_before_read_only_cleanup(
        self,
        app_with_backend: TestClient,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _seed(in_memory_backend, resources=(_resource("resource-a"),))
        events: list[str] = []
        original_resource_response = resource_links.ResourceLinkResourceResponse
        original_uow_exit = ReadOnlySQLModelUnitOfWork.__exit__

        def record_resource_response(**kwargs: Any) -> ResourceLinkResourceResponse:
            events.append("resource projection")
            return original_resource_response(**kwargs)

        def record_uow_exit(
            unit_of_work: ReadOnlySQLModelUnitOfWork,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: object,
        ) -> None:
            events.append("uow cleanup")
            original_uow_exit(unit_of_work, exc_type, exc_value, traceback)

        monkeypatch.setattr(resource_links, "ResourceLinkResourceResponse", record_resource_response)
        monkeypatch.setattr(ReadOnlySQLModelUnitOfWork, "__exit__", record_uow_exit)

        response = app_with_backend.post(_RESOLVE_PATH, json={"identifiers": ["resource-a"]})

        assert response.status_code == 200
        assert events == ["resource projection", "uow cleanup"]


@pytest.mark.parametrize(
    ("payload", "content", "headers", "error_type", "location", "message"),
    (
        (_MISSING, None, None, "missing", ["body"], None),
        ({}, None, None, "missing", ["body", "identifiers"], None),
        (_MISSING, b'{"identifiers":', {"content-type": "application/json"}, "json_invalid", ["body", 15], None),
        ([], None, None, "model_attributes_type", ["body"], None),
        ({"identifiers": "one"}, None, None, "list_type", ["body", "identifiers"], None),
        ({"identifiers": ["valid", 1]}, None, None, "string_type", ["body", "identifiers", 1], None),
        ({"identifiers": []}, None, None, "too_short", ["body", "identifiers"], None),
        (
            {"identifiers": [f"item-{index}" for index in range(101)]},
            None,
            None,
            "too_long",
            ["body", "identifiers"],
            None,
        ),
        (
            {"identifiers": ["duplicate"] * 101},
            None,
            None,
            "too_long",
            ["body", "identifiers"],
            None,
        ),
        (
            {"identifiers": ["valid", "   "]},
            None,
            None,
            "value_error",
            ["body", "identifiers"],
            "Value error, identifiers must not contain blank values",
        ),
    ),
)
def test_invalid_request_bodies_return_422_before_all_runtime_work(
    payload: object,
    content: bytes | None,
    headers: dict[str, str] | None,
    error_type: str,
    location: list[str | int],
    message: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _RejectingProvider()
    app = create_app(_settings("test-tenant"))
    get_settings = Mock(side_effect=AssertionError("settings lookup must not run"))
    get_tenant_config = Mock(side_effect=AssertionError("tenant lookup must not run"))
    get_backend_provider = Mock(side_effect=AssertionError("provider lookup must not run"))
    resource_get_many = Mock(side_effect=AssertionError("resource query must not run"))
    identity_get_many = Mock(side_effect=AssertionError("identity query must not run"))
    uow_enter = Mock(side_effect=AssertionError("unit of work must not open"))
    monkeypatch.setattr(resource_links, "get_settings", get_settings)
    monkeypatch.setattr(resource_links, "get_tenant_config", get_tenant_config)
    monkeypatch.setattr(resource_links, "get_backend_provider", get_backend_provider)
    monkeypatch.setattr(SQLModelResourceRepository, "get_many", resource_get_many)
    monkeypatch.setattr(SQLModelIdentityRepository, "get_many", identity_get_many)
    monkeypatch.setattr(ReadOnlySQLModelUnitOfWork, "__enter__", uow_enter)

    with TestClient(app) as client:
        app.state.backend_provider = provider
        del app.state.settings
        if content is not None:
            response = client.post(_RESOLVE_PATH, content=content, headers=headers)
        elif payload is _MISSING:
            response = client.post(_RESOLVE_PATH)
        else:
            response = client.post(_RESOLVE_PATH, json=payload)

    assert response.status_code == 422
    detail = response.json()["detail"]
    error = next(item for item in detail if item["type"] == error_type and item["loc"] == location)
    if message is not None:
        assert error["msg"] == message
    assert provider.acquisitions == []
    assert get_settings.call_count == 0
    assert get_tenant_config.call_count == 0
    assert get_backend_provider.call_count == 0
    assert resource_get_many.call_count == 0
    assert identity_get_many.call_count == 0
    assert uow_enter.call_count == 0


class TestResolveResourceLinksDependencyOrdering:
    def test_unknown_tenant_returns_404_before_provider_lookup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        provider = _RejectingProvider()
        app = create_app(_settings("test-tenant"))
        get_backend_provider = Mock(side_effect=AssertionError("provider lookup must not run"))
        monkeypatch.setattr(resource_links, "get_backend_provider", get_backend_provider)

        with TestClient(app) as client:
            app.state.backend_provider = provider
            response = client.post(
                _PATH.format(tenant_name="unknown-tenant"),
                json={"identifiers": ["resource-a"]},
            )

        assert response.status_code == 404
        assert response.json() == {"detail": "Tenant 'unknown-tenant' not found"}
        assert provider.acquisitions == []
        assert get_backend_provider.call_count == 0

    @pytest.mark.parametrize("unavailable_provider", (None, object()))
    def test_unavailable_provider_returns_503_before_backend_acquisition(
        self,
        unavailable_provider: object | None,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = create_app(_settings("test-tenant"))
        original_get_backend_provider = resource_links.get_backend_provider
        get_backend_provider = Mock(wraps=original_get_backend_provider)
        resource_get_many = Mock(side_effect=AssertionError("resource query must not run"))
        identity_get_many = Mock(side_effect=AssertionError("identity query must not run"))
        monkeypatch.setattr(resource_links, "get_backend_provider", get_backend_provider)
        monkeypatch.setattr(SQLModelResourceRepository, "get_many", resource_get_many)
        monkeypatch.setattr(SQLModelIdentityRepository, "get_many", identity_get_many)

        with TestClient(app) as client:
            app.state.backend_provider = unavailable_provider
            response = client.post(_RESOLVE_PATH, json={"identifiers": ["resource-a"]})

        assert response.status_code == 503
        assert response.json() == {"detail": "Storage backend provider is unavailable"}
        assert get_backend_provider.call_count == 1
        assert resource_get_many.call_count == 0
        assert identity_get_many.call_count == 0

    def test_missing_settings_returns_the_global_500_contract_without_runtime_work(self) -> None:
        provider = _RejectingProvider()
        app = create_app(_settings("test-tenant"))

        with TestClient(app, raise_server_exceptions=False) as client:
            app.state.backend_provider = provider
            del app.state.settings
            response = client.post(_RESOLVE_PATH, json={"identifiers": ["resource-a"]})

        _error_body(response)
        assert response.status_code == 500
        assert provider.acquisitions == []

    def test_backend_acquisition_failure_returns_the_global_500_contract_without_a_unit_of_work(self) -> None:
        provider = _FailingProvider()
        app = create_app(_settings("test-tenant"))

        with TestClient(app, raise_server_exceptions=False) as client:
            app.state.backend_provider = provider
            response = client.post(_RESOLVE_PATH, json={"identifiers": ["resource-a"]})

        _error_body(response)
        assert response.status_code == 500
        assert provider.acquisitions == ["test-tenant"]

    @pytest.mark.parametrize("repository_name", ("resources", "identities"))
    def test_query_failures_return_the_global_500_contract_and_close_the_lease_and_unit_of_work(
        self,
        in_memory_backend: SQLModelBackend,
        monkeypatch: pytest.MonkeyPatch,
        repository_name: str,
    ) -> None:
        provider = FixedTenantBackendProvider({"test-tenant": in_memory_backend})
        app = create_app(_settings("test-tenant"))
        uow_exits: list[type[BaseException] | None] = []
        original_exit = ReadOnlySQLModelUnitOfWork.__exit__

        def record_uow_exit(
            unit_of_work: ReadOnlySQLModelUnitOfWork,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: object,
        ) -> None:
            uow_exits.append(exc_type)
            original_exit(unit_of_work, exc_type, exc_value, traceback)

        def fail_resource_get_many(
            repository: SQLModelResourceRepository,
            ecosystem: str,
            tenant_id: str,
            resource_ids: Sequence[str],
        ) -> dict[str, Resource]:
            del repository, ecosystem, tenant_id, resource_ids
            raise RuntimeError("resource query failed")

        def fail_identity_get_many(
            repository: SQLModelIdentityRepository,
            ecosystem: str,
            tenant_id: str,
            identity_ids: Sequence[str],
        ) -> dict[str, Identity]:
            del repository, ecosystem, tenant_id, identity_ids
            raise RuntimeError("identity query failed")

        monkeypatch.setattr(ReadOnlySQLModelUnitOfWork, "__exit__", record_uow_exit)
        if repository_name == "resources":
            monkeypatch.setattr(SQLModelResourceRepository, "get_many", fail_resource_get_many)
        else:
            monkeypatch.setattr(SQLModelIdentityRepository, "get_many", fail_identity_get_many)

        with TestClient(app, raise_server_exceptions=False) as client:
            app.state.backend_provider = provider
            response = client.post(_RESOLVE_PATH, json={"identifiers": ["resource-a"]})

        _error_body(response)
        assert response.status_code == 500
        assert provider.lease_events == [("enter", "test-tenant"), ("exit", "test-tenant")]
        assert uow_exits == [RuntimeError]


def test_registered_route_publishes_the_bounded_request_and_minimal_response_openapi_contract() -> None:
    app = create_app(_settings("test-tenant"))

    operation = app.openapi()["paths"][_PATH]["post"]

    assert operation["requestBody"]["required"] is True
    assert operation["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ResourceLinkResolveRequest"
    }
    assert operation["responses"]["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ResourceLinkResolveResponse"
    }


def test_production_plugin_registry_provider_and_sqlmodel_storage_resolve_a_link(tmp_path: Path) -> None:
    from plugins.confluent_cloud.plugin import ConfluentCloudPlugin

    settings = AppSettings(
        tenants={
            "production": TenantConfig(
                ecosystem="confluent_cloud",
                tenant_id="production",
                storage=StorageConfig(connection_string=f"sqlite:///{tmp_path / 'production.db'}"),
                plugin_settings={"ccloud_api": {"key": "key", "secret": "secret"}},  # pragma: allowlist secret
            )
        }
    )
    registry = PluginRegistry()
    registry.register("confluent_cloud", ConfluentCloudPlugin)
    app = create_app(settings, plugin_registry=registry)

    with TestClient(app) as client:
        provider = app.state.backend_provider
        assert isinstance(provider, ApiTenantBackendProvider)
        with (
            provider.acquire_backend("production", settings.tenants["production"]) as backend,
            backend.create_unit_of_work() as unit_of_work,
        ):
            unit_of_work.resources.upsert(
                _resource(
                    "lkc-production",
                    ecosystem="confluent_cloud",
                    tenant_id="production",
                    parent_id="env-production",
                )
            )
            unit_of_work.commit()

        response = client.post(
            _PATH.format(tenant_name="production"),
            json={"identifiers": ["lkc-production"]},
        )

    assert response.status_code == 200
    assert response.json() == {
        "resources": {
            "lkc-production": {
                "resource_type": "kafka_cluster",
                "parent_id": "env-production",
                "kafka_cluster_id": None,
            }
        },
        "identities": {},
    }
