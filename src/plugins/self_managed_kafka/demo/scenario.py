from __future__ import annotations

import calendar
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING, Any, NoReturn

from core.engine.topic_attribution_models import TopicAttributionRowOutputContext, build_reconciled_topic_rows
from core.models import (
    ChargebackRow,
    CoreBillingLineItem,
    CoreIdentity,
    CoreResource,
    CostType,
    EntityTag,
    PipelineState,
    ResourceStatus,
)

if TYPE_CHECKING:
    from core.models.topic_attribution import TopicAttributionRow

ECOSYSTEM = "self_managed_kafka"
TEAM_VALUES: tuple[str, ...] = ("orders", "platform", "analytics", "security")
_CLUSTERS: tuple[tuple[str, str], ...] = (
    ("northstar-logistics-kafka", "Logistics Kafka"),
    ("northstar-analytics-kafka", "Analytics Kafka"),
)
_TOPIC_SEEDS: tuple[tuple[str, ...], ...] = (
    (
        "orders.created.v1",
        "orders.updated.v1",
        "inventory.reserved.v1",
        "inventory.replenished.v1",
    ),
    (
        "analytics.events.v1",
        "analytics.aggregates.v1",
        "platform.usage.v1",
        "platform.alerts.v1",
    ),
)
_IDENTITIES: tuple[tuple[str, str, str], ...] = (
    ("principal-orders", "Orders Workloads", "orders"),
    ("principal-payments", "Payments Workloads", "orders"),
    ("principal-fulfillment", "Fulfillment Workloads", "orders"),
    ("principal-customer", "Customer Workloads", "orders"),
    ("principal-analytics", "Analytics Workloads", "analytics"),
    ("principal-platform", "Platform Operations", "platform"),
    ("principal-data", "Data Operations", "analytics"),
    ("principal-security", "Security Operations", "security"),
    ("principal-observability", "Observability Operations", "platform"),
    ("principal-finance", "Finance Operations", "security"),
)
_PRODUCTS: tuple[tuple[str, Decimal, Decimal], ...] = (
    ("SELF_KAFKA_COMPUTE", Decimal("24.0"), Decimal("1.25")),
    ("SELF_KAFKA_STORAGE", Decimal("180.0"), Decimal("0.01")),
    ("SELF_KAFKA_NETWORK_INGRESS", Decimal("40.0"), Decimal("0.02")),
    ("SELF_KAFKA_NETWORK_EGRESS", Decimal("32.0"), Decimal("0.04")),
)
_CENT = Decimal("0.01")
_QUANTITY_PRECISION = Decimal("0.1")


@dataclass(frozen=True)
class CleanSelfManagedKafkaScenario:
    """Pure, deterministic logical state for the self-managed Kafka demo."""

    tenant_id: str
    anchor_date: date
    start_date: date
    resources: tuple[CoreResource, ...]
    identities: tuple[CoreIdentity, ...]
    entity_tags: tuple[EntityTag, ...]
    billing_lines: tuple[CoreBillingLineItem, ...]
    chargebacks: tuple[ChargebackRow, ...]
    topic_attributions: tuple[TopicAttributionRow, ...]
    pipeline_states: tuple[PipelineState, ...]


def _window_start(anchor_date: date) -> date:
    month_index = anchor_date.year * 12 + anchor_date.month - 1 - 6
    year, month_zero_based = divmod(month_index, 12)
    month = month_zero_based + 1
    day = min(anchor_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day) + timedelta(days=1)


def _at_midnight(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=UTC)


def _topics_for_cluster(index: int) -> tuple[str, ...]:
    seed = _TOPIC_SEEDS[index]
    names = list(seed)
    prefix = seed[0].split(".")[0]
    while len(names) < 12:
        names.append(f"{prefix}.events.{len(names) + 1:02d}.v1")
    return tuple(names)


def _resource(
    tenant_id: str,
    resource_id: str,
    resource_type: str,
    display_name: str,
    created_at: datetime,
    parent_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> CoreResource:
    return CoreResource(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        resource_id=resource_id,
        resource_type=resource_type,
        display_name=display_name,
        parent_id=parent_id,
        owner_id=None,
        status=ResourceStatus.ACTIVE,
        created_at=created_at,
        deleted_at=None,
        last_seen_at=created_at,
        metadata={} if metadata is None else metadata,
    )


def _identity(
    tenant_id: str,
    identity_id: str,
    display_name: str,
    created_at: datetime,
) -> CoreIdentity:
    return CoreIdentity(
        ecosystem=ECOSYSTEM,
        tenant_id=tenant_id,
        identity_id=identity_id,
        identity_type="principal",
        display_name=display_name,
        created_at=created_at,
        deleted_at=None,
        last_seen_at=created_at,
        metadata={},
    )


def _build_resources(tenant_id: str, created_at: datetime) -> tuple[CoreResource, ...]:
    resources: list[CoreResource] = []
    for index, (cluster_id, display_name) in enumerate(_CLUSTERS):
        resources.append(
            _resource(
                tenant_id,
                cluster_id,
                "cluster",
                display_name,
                created_at,
                metadata={
                    "cluster_id": cluster_id,
                    "broker_count": 3,
                    "metrics_identifier": cluster_id,
                    "region": "us-east-1" if index == 0 else "us-west-2",
                },
            )
        )
        resources.extend(
            _resource(
                tenant_id,
                f"{cluster_id}:topic:{topic_name}",
                "topic",
                topic_name,
                created_at,
                parent_id=cluster_id,
            )
            for topic_name in _topics_for_cluster(index)
        )
    return tuple(resources)


def _build_identities(tenant_id: str, created_at: datetime) -> tuple[CoreIdentity, ...]:
    return tuple(
        _identity(tenant_id, identity_id, display_name, created_at) for identity_id, display_name, _team in _IDENTITIES
    )


def _build_tags(
    tenant_id: str,
    resources: tuple[CoreResource, ...],
    identities: tuple[CoreIdentity, ...],
) -> tuple[EntityTag, ...]:
    cluster_teams = {
        "northstar-logistics-kafka": "orders",
        "northstar-analytics-kafka": "analytics",
    }
    tags: list[EntityTag] = []
    for resource in resources:
        cluster_id = resource.resource_id.split(":topic:", 1)[0]
        team = cluster_teams.get(cluster_id, "platform")
        tags.append(
            EntityTag(
                tag_id=None,
                tenant_id=tenant_id,
                entity_type="resource",
                entity_id=resource.resource_id,
                tag_key="team",
                tag_value=team,
                created_by="demo-generator",
                created_at=None,
            )
        )
    for identity_id, _display_name, team in _IDENTITIES:
        tags.append(
            EntityTag(
                tag_id=None,
                tenant_id=tenant_id,
                entity_type="identity",
                entity_id=identity_id,
                tag_key="team",
                tag_value=team,
                created_by="demo-generator",
                created_at=None,
            )
        )
    return tuple(tags)


def _build_billing_and_chargebacks(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
) -> tuple[tuple[CoreBillingLineItem, ...], tuple[ChargebackRow, ...]]:
    billing_lines: list[CoreBillingLineItem] = []
    chargebacks: list[ChargebackRow] = []
    targets_by_cluster = {
        "northstar-logistics-kafka": ("principal-orders", "principal-fulfillment"),
        "northstar-analytics-kafka": ("principal-analytics", "principal-platform"),
    }
    for offset in range((anchor_date - start_date).days + 1):
        tracking_date = start_date + timedelta(days=offset)
        timestamp = _at_midnight(tracking_date)
        scale = Decimal("1") + Decimal(offset % 7) / Decimal("100")
        for cluster_id, _display_name in _CLUSTERS:
            for product_type, base_quantity, unit_price in _PRODUCTS:
                quantity = (base_quantity * scale).quantize(_QUANTITY_PRECISION, rounding=ROUND_HALF_UP)
                total_cost = (quantity * unit_price).quantize(_CENT, rounding=ROUND_HALF_UP)
                billing_lines.append(
                    CoreBillingLineItem(
                        ecosystem=ECOSYSTEM,
                        tenant_id=tenant_id,
                        timestamp=timestamp,
                        resource_id=cluster_id,
                        product_category="kafka",
                        product_type=product_type,
                        quantity=quantity,
                        unit_price=unit_price,
                        total_cost=total_cost,
                        currency="USD",
                        granularity="daily",
                        metadata={},
                    )
                )
                targets = targets_by_cluster[cluster_id]
                portions = (Decimal("0.60"), Decimal("0.40"))
                cost_type = CostType.USAGE if "NETWORK" in product_type else CostType.SHARED
                allocation_method = "usage_ratio" if cost_type is CostType.USAGE else "even_split"
                allocation_detail = "usage_ratio_allocation" if cost_type is CostType.USAGE else "even_split_allocation"
                allocated = Decimal("0")
                for target_index, (identity_id, ratio) in enumerate(zip(targets, portions, strict=True)):
                    amount = (
                        total_cost - allocated
                        if target_index == 1
                        else (total_cost * ratio).quantize(_CENT, rounding=ROUND_HALF_UP)
                    )
                    allocated += amount
                    team = next(team for candidate_id, _name, team in _IDENTITIES if candidate_id == identity_id)
                    chargebacks.append(
                        ChargebackRow(
                            ecosystem=ECOSYSTEM,
                            tenant_id=tenant_id,
                            timestamp=timestamp,
                            resource_id=cluster_id,
                            product_category="kafka",
                            product_type=product_type,
                            identity_id=identity_id,
                            cost_type=cost_type,
                            amount=amount,
                            allocation_method=allocation_method,
                            allocation_detail=allocation_detail,
                            tags={},
                            metadata={"team": team},
                        )
                    )
    return tuple(billing_lines), tuple(chargebacks)


def _build_topic_attributions(
    billing_lines: tuple[CoreBillingLineItem, ...],
) -> tuple[TopicAttributionRow, ...]:
    rows: list[TopicAttributionRow] = []
    for line in billing_lines:
        cluster_index = next(
            index for index, (cluster_id, _name) in enumerate(_CLUSTERS) if cluster_id == line.resource_id
        )
        topic_names = _topics_for_cluster(cluster_index)[:2]
        rows.extend(
            build_reconciled_topic_rows(
                TopicAttributionRowOutputContext(
                    ecosystem=ECOSYSTEM,
                    tenant_id=line.tenant_id,
                    timestamp=line.timestamp,
                    env_id="",
                    cluster_resource_id=line.resource_id,
                    product_category=line.product_category,
                    product_type=line.product_type,
                    cluster_cost=line.total_cost,
                ),
                cluster_quantity=line.quantity,
                pool_usage=Decimal("3"),
                topic_usage={topic_names[0]: Decimal("1"), topic_names[1]: Decimal("2")},
                attribution_method=("bytes_ratio" if "NETWORK" in line.product_type else "shared_even_v1"),
                residual_method="complete_topic_telemetry",
            )
        )
    return tuple(rows)


def _build_pipeline_states(
    tenant_id: str,
    start_date: date,
    anchor_date: date,
) -> tuple[PipelineState, ...]:
    return tuple(
        PipelineState(
            ecosystem=ECOSYSTEM,
            tenant_id=tenant_id,
            tracking_date=tracking_date,
            billing_gathered=True,
            resources_gathered=True,
            chargeback_calculated=True,
            calculation_id=f"clean-self-managed-calculation-{tracking_date.isoformat()}",
            calculation_completed_at=_at_midnight(tracking_date) + timedelta(hours=1),
            topic_overlay_gathered=True,
            topic_attribution_calculated=True,
        )
        for tracking_date in (
            start_date + timedelta(days=offset) for offset in range((anchor_date - start_date).days + 1)
        )
    )


def build_clean_self_managed_kafka_scenario(*, tenant_id: str, anchor_date: date) -> CleanSelfManagedKafkaScenario:
    """Build the deterministic self-managed Kafka scenario for ``anchor_date``."""
    if not tenant_id.strip():
        raise ValueError("tenant_id must not be blank")
    start_date = _window_start(anchor_date)
    created_at = _at_midnight(start_date - timedelta(days=30))
    resources = _build_resources(tenant_id, created_at)
    identities = _build_identities(tenant_id, created_at)
    billing_lines, chargebacks = _build_billing_and_chargebacks(tenant_id, start_date, anchor_date)
    scenario = CleanSelfManagedKafkaScenario(
        tenant_id=tenant_id,
        anchor_date=anchor_date,
        start_date=start_date,
        resources=resources,
        identities=identities,
        entity_tags=_build_tags(tenant_id, resources, identities),
        billing_lines=billing_lines,
        chargebacks=chargebacks,
        topic_attributions=_build_topic_attributions(billing_lines),
        pipeline_states=_build_pipeline_states(tenant_id, start_date, anchor_date),
    )
    validate_clean_self_managed_kafka_scenario(scenario)
    return scenario


def _raise(message: str) -> NoReturn:
    raise ValueError(message)


def validate_clean_self_managed_kafka_scenario(scenario: CleanSelfManagedKafkaScenario) -> None:
    """Validate the exact self-managed topology and all persisted logical data."""
    if not scenario.tenant_id.strip() or scenario.start_date != _window_start(scenario.anchor_date):
        _raise("invalid self-managed scenario identity or date window")
    expected_dates = tuple(
        scenario.start_date + timedelta(days=offset)
        for offset in range((scenario.anchor_date - scenario.start_date).days + 1)
    )
    resources_by_id: dict[str, CoreResource] = {}
    for resource in scenario.resources:
        if (
            resource.ecosystem != ECOSYSTEM
            or resource.tenant_id != scenario.tenant_id
            or resource.status is not ResourceStatus.ACTIVE
            or resource.resource_id in resources_by_id
            or any(
                value is not None and (value.tzinfo is None or value.utcoffset() != timedelta(0) or value.microsecond)
                for value in (resource.created_at, resource.deleted_at, resource.last_seen_at)
            )
        ):
            _raise("self-managed resource state is invalid")
        resources_by_id[resource.resource_id] = resource
    if dict(Counter(resource.resource_type for resource in scenario.resources)) != {"cluster": 2, "topic": 24}:
        _raise("self-managed resource topology mismatch")
    clusters = {
        resource.resource_id: resource for resource in scenario.resources if resource.resource_type == "cluster"
    }
    if set(clusters) != {cluster_id for cluster_id, _ in _CLUSTERS} or any(
        cluster.parent_id is not None for cluster in clusters.values()
    ):
        _raise("self-managed cluster topology mismatch")
    topics_by_cluster: dict[str, set[str]] = defaultdict(set)
    for topic in (resource for resource in scenario.resources if resource.resource_type == "topic"):
        if (
            topic.parent_id not in clusters
            or not topic.display_name
            or topic.display_name in topics_by_cluster[str(topic.parent_id)]
        ):
            _raise("self-managed topic relationship is invalid")
        if topic.resource_id != f"{topic.parent_id}:topic:{topic.display_name}":
            _raise("self-managed topic storage ID is invalid")
        topics_by_cluster[str(topic.parent_id)].add(topic.display_name)
    if any(len(topic_names) != 12 for topic_names in topics_by_cluster.values()):
        _raise("self-managed cluster must contain twelve topics")

    identities_by_id: dict[str, CoreIdentity] = {}
    for identity in scenario.identities:
        if (
            identity.ecosystem != ECOSYSTEM
            or identity.tenant_id != scenario.tenant_id
            or identity.identity_type != "principal"
            or identity.identity_id in identities_by_id
            or any(
                value is not None and (value.tzinfo is None or value.utcoffset() != timedelta(0) or value.microsecond)
                for value in (identity.created_at, identity.deleted_at, identity.last_seen_at)
            )
        ):
            _raise("self-managed identity state is invalid")
        identities_by_id[identity.identity_id] = identity
    if len(identities_by_id) != 10:
        _raise("self-managed identity topology mismatch")

    entity_keys = {("resource", resource_id) for resource_id in resources_by_id} | {
        ("identity", identity_id) for identity_id in identities_by_id
    }
    tag_by_entity: dict[tuple[str, str], EntityTag] = {}
    for tag in scenario.entity_tags:
        tag_key = (tag.entity_type, tag.entity_id)
        if (
            tag_key in tag_by_entity
            or tag.tenant_id != scenario.tenant_id
            or tag.entity_type not in {"resource", "identity"}
            or tag_key not in entity_keys
            or tag.tag_key != "team"
            or tag.tag_value not in TEAM_VALUES
            or tag.created_by != "demo-generator"
            or tag.tag_id is not None
            or tag.created_at is not None
        ):
            _raise("self-managed entity tag is invalid")
        tag_by_entity[tag_key] = tag
    if set(tag_by_entity) != entity_keys or {tag.tag_value for tag in tag_by_entity.values()} != set(TEAM_VALUES):
        _raise("self-managed entities must have complete team tags")

    expected_products = {product_type for product_type, _quantity, _price in _PRODUCTS}
    billing_by_key: dict[tuple[datetime, str, str, str], CoreBillingLineItem] = {}
    billing_dates: Counter[date] = Counter()
    for line in scenario.billing_lines:
        billing_key = (line.timestamp, line.resource_id, line.product_category, line.product_type)
        if (
            line.ecosystem != ECOSYSTEM
            or line.tenant_id != scenario.tenant_id
            or line.timestamp.date() not in expected_dates
            or line.timestamp.time() != datetime.min.time()
            or line.resource_id not in clusters
            or line.product_category != "kafka"
            or line.product_type not in expected_products
            or line.quantity <= 0
            or line.total_cost <= 0
            or line.total_cost != (line.quantity * line.unit_price).quantize(_CENT, rounding=ROUND_HALF_UP)
            or billing_key in billing_by_key
        ):
            _raise("self-managed billing line is invalid")
        billing_by_key[billing_key] = line
        billing_dates[line.timestamp.date()] += 1
    if tuple(billing_dates) != expected_dates or set(billing_dates.values()) != {len(_CLUSTERS) * len(_PRODUCTS)}:
        _raise("self-managed billing date coverage is incomplete")

    chargeback_totals: defaultdict[tuple[datetime, str, str, str], Decimal] = defaultdict(Decimal)
    for chargeback_row in scenario.chargebacks:
        chargeback_key = (
            chargeback_row.timestamp,
            chargeback_row.resource_id or "",
            chargeback_row.product_category,
            chargeback_row.product_type,
        )
        target = identities_by_id.get(chargeback_row.identity_id)
        if (
            target is None
            or ("identity", chargeback_row.identity_id) not in tag_by_entity
            or chargeback_row.ecosystem != ECOSYSTEM
            or chargeback_row.tenant_id != scenario.tenant_id
            or chargeback_row.timestamp.date() not in expected_dates
            or chargeback_row.resource_id not in clusters
            or chargeback_row.product_category != "kafka"
            or chargeback_row.product_type not in expected_products
            or chargeback_row.amount <= 0
            or chargeback_row.amount != chargeback_row.amount.quantize(_CENT)
            or not chargeback_row.metadata.get("team")
            or chargeback_row.metadata["team"] != tag_by_entity[("identity", chargeback_row.identity_id)].tag_value
            or chargeback_row.tags
            or chargeback_key not in billing_by_key
        ):
            _raise("self-managed chargeback row is invalid")
        chargeback_totals[chargeback_key] += chargeback_row.amount
    if dict(chargeback_totals) != {key: line.total_cost for key, line in billing_by_key.items()}:
        _raise("self-managed billing must reconcile to chargebacks")

    topic_totals: defaultdict[tuple[datetime, str, str, str], Decimal] = defaultdict(Decimal)
    for topic_row in scenario.topic_attributions:
        topic_key = (
            topic_row.timestamp,
            topic_row.cluster_resource_id,
            topic_row.product_category,
            topic_row.product_type,
        )
        if (
            topic_row.ecosystem != ECOSYSTEM
            or topic_row.tenant_id != scenario.tenant_id
            or topic_row.env_id != ""
            or topic_row.cluster_resource_id not in clusters
            or topic_row.topic_name == "__UNATTRIBUTED__"
            or topic_row.topic_name not in topics_by_cluster[topic_row.cluster_resource_id]
            or topic_row.product_category != "kafka"
            or topic_row.product_type not in expected_products
            or topic_row.amount <= 0
            or topic_key not in billing_by_key
        ):
            _raise("self-managed topic attribution row is invalid")
        topic_totals[topic_key] += topic_row.amount
    if dict(topic_totals) != {key: line.total_cost for key, line in billing_by_key.items()}:
        _raise("self-managed billing must reconcile to topic attribution")

    pipeline_by_date: dict[date, PipelineState] = {}
    for state in scenario.pipeline_states:
        if (
            state.ecosystem != ECOSYSTEM
            or state.tenant_id != scenario.tenant_id
            or state.tracking_date not in expected_dates
            or state.tracking_date in pipeline_by_date
            or not state.billing_gathered
            or not state.resources_gathered
            or not state.has_usable_calculation
            or not state.topic_overlay_gathered
            or not state.topic_attribution_calculated
        ):
            _raise("self-managed pipeline state is incomplete")
        pipeline_by_date[state.tracking_date] = state
    if tuple(pipeline_by_date) != expected_dates:
        _raise("self-managed pipeline date coverage is incomplete")
