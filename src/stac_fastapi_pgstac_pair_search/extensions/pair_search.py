import attr
from enum import Enum
from typing import List, Optional

from fastapi import FastAPI
from stac_fastapi.types.extension import ApiExtension

from stac_fastapi_pgstac_pair_search.models import PairSearchRequest


class PairSearchConformanceClasses(str, Enum):
    """Conformance classes for the Pair-Search extension."""

    SearchCore = "https://api.stacspec.org/v0.0.1/pair-search"
    Query = "https://api.stacspec.org/v0.0.1/pair-search#query"
    CQL2NumberDifference = "https://eox.at/ext/cq12/1.0/conf/number-difference"
    CQL2TimeDifference = "https://eox.at/ext/cq12/1.0/conf/time-difference"
    CQL2RelativeGeometryOverlap = (
        "https://eox.at/ext/cq12/1.0/conf/relative-geometry-overlap"
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
        factory=lambda: [
            PairSearchConformanceClasses.SearchCore,
            PairSearchConformanceClasses.Query,
            PairSearchConformanceClasses.CQL2NumberDifference,
            PairSearchConformanceClasses.CQL2TimeDifference,
            PairSearchConformanceClasses.CQL2RelativeGeometryOverlap,
        ]
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
