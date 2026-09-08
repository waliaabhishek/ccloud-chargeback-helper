from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Path, Request

from core.api.dependencies import get_backend_provider, get_settings, get_tenant_config
from core.api.schemas import (
    ResourceLinkIdentityResponse,
    ResourceLinkResolveRequest,
    ResourceLinkResolveResponse,
    ResourceLinkResourceResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["resource-links"])


@router.post(
    "/tenants/{tenant_name}/resource-links/resolve",
    response_model=ResourceLinkResolveResponse,
)
async def resolve_resource_links(
    request: Request,
    tenant_name: Annotated[str, Path(description="Tenant name from config")],
    body: ResourceLinkResolveRequest,
) -> ResourceLinkResolveResponse:
    settings = get_settings(request)
    tenant_config = get_tenant_config(tenant_name, settings)
    provider = get_backend_provider(request)

    unique_identifiers = list(dict.fromkeys(body.identifiers))
    ecosystem = tenant_config.ecosystem
    tenant_id = tenant_config.tenant_id

    with (
        provider.acquire_backend(tenant_name, tenant_config) as backend,
        backend.create_read_only_unit_of_work() as uow,
    ):
        resources_by_id = uow.resources.get_many(ecosystem, tenant_id, unique_identifiers)
        identities_by_id = uow.identities.get_many(ecosystem, tenant_id, unique_identifiers)

        resources: dict[str, ResourceLinkResourceResponse] = {}
        identities: dict[str, ResourceLinkIdentityResponse] = {}

        for identifier in unique_identifiers:
            resource = resources_by_id.get(identifier)
            if resource is not None and resource.deleted_at is None:
                kafka_cluster_id = resource.metadata.get("kafka_cluster_id")
                resources[resource.resource_id] = ResourceLinkResourceResponse(
                    resource_type=resource.resource_type,
                    parent_id=resource.parent_id,
                    kafka_cluster_id=(
                        kafka_cluster_id if isinstance(kafka_cluster_id, str) and kafka_cluster_id else None
                    ),
                )

            identity = identities_by_id.get(identifier)
            if identity is not None and identity.deleted_at is None:
                identities[identity.identity_id] = ResourceLinkIdentityResponse(identity_type=identity.identity_type)

        response = ResourceLinkResolveResponse(resources=resources, identities=identities)
        logger.info(
            "Resolved resource link context requested=%d resources=%d identities=%d",
            len(unique_identifiers),
            len(response.resources),
            len(response.identities),
        )
        return response
