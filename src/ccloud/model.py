from dataclasses import dataclass, field
from dataclasses_json import dataclass_json
from enum import Enum, auto
from typing import List


######################################################################
# Confluent Cloud Metrics Endpoint Request Payload signature setup
# CCME == Confluent Cloud Metrics Endpoint
######################################################################


class CCMEReq_Granularity(Enum):
    PT1M = auto()
    PT5M = auto()
    PT15M = auto()
    PT30M = auto()
    PT1H = auto()
    PT6H = auto()
    PT12H = auto()
    P1D = auto()
    ALL = auto()


class CCMEReq_Order(Enum):
    ASCENDING = auto()
    DESCENDING = auto()


class CCMEReq_CompareOp(Enum):
    EQ = auto()
    GT = auto()
    GTE = auto()


class CCMEReq_ConditionalOp(Enum):
    AND = auto()
    OR = auto()


class CCMEReq_UnaryOp(Enum):
    NOT = auto()


class CCMEReq_Format(Enum):
    FLAT = auto()
    GROUPED = auto()


@dataclass(kw_only=True)
class CCMEReq_OrderBy:
    metric: str
    agg: str
    order: CCMEReq_Order


@dataclass(kw_only=True)
class CCMEReq_FieldFilter:
    op: CCMEReq_CompareOp
    field: str
    value: str | int


@dataclass(kw_only=True)
class CCMEReq_UnaryFilter:
    op: CCMEReq_UnaryOp
    filter: CCMEReq_FieldFilter


@dataclass(kw_only=True)
class CCMEReq_CompoundFilter:
    op: CCMEReq_ConditionalOp
    filters: List[CCMEReq_FieldFilter | CCMEReq_UnaryFilter]


@dataclass
class CCMEReq_Aggregations:
    metric: str
    agg: str = field(default="SUM")


@dataclass_json
@dataclass(kw_only=True)
class CCMERequest:
    aggregations: List[CCMEReq_Aggregations]
    group_by: List[str] = field(default_factory=list)
    granularity: CCMEReq_Granularity = field(default=CCMEReq_Granularity.P1D.name)
    filter: CCMEReq_FieldFilter | CCMEReq_UnaryFilter | CCMEReq_CompoundFilter = field(default=None)
    order_by: List[CCMEReq_OrderBy] = field(default=None, repr=False)
    intervals: List[str]
    limit: int = field(default=100)
    format: CCMEReq_Format = field(default=CCMEReq_Format.FLAT.name)
