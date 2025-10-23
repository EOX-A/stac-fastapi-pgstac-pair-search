import attr
from enum import Enum
from typing import List, Optional

from fastapi import FastAPI
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi_pgstac_pair_search.models import PairSearchRequest


class PairSearchConformanceClasses(str, Enum):
    """Conformance classes for the Pair-Search extension."""

    SearchCore = "https://api.stacspec.org/1.0.0/pair-search"
    Filter = "https://api.stacspec.org/1.0.0/pair-search#filter"
    CQL2NumberDifference = "https://spec.eox.at/ext/cql2/1.0/conf/number-difference"
    CQL2TimeDifference = "https://spec.eox.at/ext/cql2/1.0/conf/time-difference"
    CQL2TimeStart = "https://spec.eox.at/ext/cql2/1.0/conf/time-start"
    CQL2TimeEnd = "https://spec.eox.at/ext/cql2/1.0/conf/time-end"
    CQL2RelativeGeometryOverlap = (
        "https://spec.eox.at/ext/cql2/1.0/conf/relative-geometry-overlap"
    )


@attr.s
class PairSearchExtension(ApiExtension):
    """Query Extension.

    The Query extension adds an additional `query` parameter to `/search` requests which
    allows the caller to perform queries against item metadata (ex. find all images with
    cloud cover less than 15%).
    https://github.com/stac-api-extensions/query
    """

    GET = PairSearchRequest
    POST = PairSearchRequest

    conformance_classes: List[str] = attr.ib(
        factory=lambda: PairSearchConformanceClasses
    )
    schema_href: Optional[str] = attr.ib(default=None)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        pass
