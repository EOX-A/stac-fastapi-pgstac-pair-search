from typing import Dict, Optional, List, Annotated, Any, Literal, Union

from fastapi import Query
from pydantic import Field, AfterValidator, BaseModel, model_validator
from datetime import datetime as dt
from stac_fastapi.extensions.core.filter.request import FilterLang
from stac_fastapi.types.search import Limit, APIRequest
from stac_fastapi.pgstac.models.links import BaseLinks
from stac_pydantic.api.search import Intersection
from stac_pydantic.links import Relations
from stac_pydantic.shared import (
    MimeTypes,
    BBox,
    validate_bbox,
    validate_datetime,
    str_to_datetimes,
)


class PairSearchRequest(BaseModel, APIRequest):
    """Request model for the pair-search endpoint."""

    # based on https://github.com/stac-utils/stac-pydantic/blob/270a7da99cd0ae0864e2c038249f9bf69f8b44c6/stac_pydantic/api/search.py

    # from slides:
    # limit  (number of result pairs/groups)
    # first-bbox, second-bbox (analogy to search bbox)
    # first-datetime, second-datetime (analogy to search datetime)
    # first-intersects, second-intersects (analogy to search second-intersects)
    # first-ids, second-ids (analogy to search ids)
    # first-collections, second-collections (analogy to search collections)
    #   important: collection selection
    # response-type=pair|first-only|second-only

    limit: Optional[Limit] = Field(
        10,
        description="Limits the number of results that are included in each page of the response (capped to 10_000).",  # noqa: E501
    )

    first_bbox: Annotated[Optional[BBox], AfterValidator(validate_bbox)] = Field(
        alias="first-bbox", default=None
    )
    second_bbox: Annotated[Optional[BBox], AfterValidator(validate_bbox)] = Field(
        alias="second-bbox", default=None
    )

    first_datetime: Annotated[Optional[str], AfterValidator(validate_datetime)] = Field(
        alias="first-datetime", default=None
    )
    second_datetime: Annotated[Optional[str], AfterValidator(validate_datetime)] = (
        Field(alias="second-datetime", default=None)
    )

    first_intersects: Optional[Intersection] = Field(
        alias="first-intersects", default=None
    )
    second_intersects: Optional[Intersection] = Field(
        alias="second-intersects", default=None
    )

    first_ids: Optional[List[str]] = Field(alias="first-ids", default=None)
    second_ids: Optional[List[str]] = Field(alias="second-ids", default=None)

    first_collections: Union[str, List[str]] = Field(alias="first-collections")
    second_collections: Union[str, List[str]] = Field(alias="second-collections")

    response_type: Optional[Literal["pair", "first-only", "second-only"]] = Field(
        alias="response-type", default="pair"
    )

    filter_expr: Annotated[
        Optional[str],
        Query(
            alias="filter",
            description="""A CQL2 filter expression for filtering items.\n
Supports `CQL2-JSON` as defined in https://docs.ogc.org/is/21-065r2/21-065r2.htmln
Remember to URL encode the CQL2-JSON if using GET""",
            openapi_examples={
                "user-provided": {"value": None},
                "landsat8-item": {
                    "value": "id='LC08_L1TP_060247_20180905_20180912_01_T1_L1TP' AND collection='landsat8_l1tp'"  # noqa: E501
                },
            },
        ),
    ] = None
    filter_crs: Annotated[
        Optional[str],
        Query(
            alias="filter-crs",
            description="The coordinate reference system (CRS) used by spatial literals in the 'filter' value. Default is `http://www.opengis.net/def/crs/OGC/1.3/CRS84`",  # noqa: E501
        ),
    ] = None
    filter_lang: Annotated[
        Optional[FilterLang],
        Query(
            alias="filter-lang",
            description="The CQL filter encoding that the 'filter' value uses.",
        ),
    ] = "cql2-text"

    @property
    def start_date(self) -> Optional[dt]:
        start_date: Optional[dt] = None
        if self.datetime:
            start_date = str_to_datetimes(self.datetime)[0]
        return start_date

    @property
    def end_date(self) -> Optional[dt]:
        end_date: Optional[dt] = None
        if self.datetime:
            dates = str_to_datetimes(self.datetime)
            end_date = dates[0] if len(dates) == 1 else dates[1]
        return end_date

    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.
    @model_validator(mode="before")
    def validate_spatial(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        for prefix in ["first", "second"]:
            if (
                values.get(f"{prefix}-intersects")
                and values.get(f"{prefix}-bbox") is not None
            ):
                raise ValueError(
                    f"{prefix}-intersects and {prefix}-bbox parameters are mutually exclusive"
                )
            return values

    # @property
    # def spatial_filter(self) -> Optional[Intersection]:
    #     """Return a geojson-pydantic object representing the spatial filter for the search request.

    #     Check for both because the ``bbox`` and ``intersects`` parameters are mutually exclusive.
    #     """
    #     if self.bbox:
    #         return Polygon.from_bounds(*self.bbox)
    #     if self.intersects:
    #         return self.intersects
    #     else:
    #         return None


class PairSearchLinks(BaseLinks):
    """Create inferred links specific to collections."""

    def link_self(self) -> Dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.geojson.value,
            "href": self.resolve("pair-search"),
        }
