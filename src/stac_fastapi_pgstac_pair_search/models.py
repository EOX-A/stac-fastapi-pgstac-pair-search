import json
import logging
import os
import re
from typing import Dict, Optional, List, Annotated, Any, Literal, Tuple

import cql2
from fastapi import Query
from pydantic import Field, AfterValidator, BaseModel, model_validator, NonNegativeInt
from datetime import datetime as dt
from stac_fastapi.extensions.core.filter.request import FilterLang
from stac_fastapi.types.search import Limit, APIRequest, BaseSearchPostRequest
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

logger = logging.getLogger(__name__)

COLLECTION_SEARCH_LIMIT = 1_000


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

    offset: Optional[NonNegativeInt] = Field(
        0,
        description="Offset from the first record. Can be used to paginate results.",
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

    first_collections: Optional[List[str]] = Field(
        alias="first-collections", default=None
    )
    second_collections: Optional[List[str]] = Field(
        alias="second-collections", default=None
    )

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
    ] = Field(alias="filter", default=None)
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
    def filter_sql(self) -> Tuple[str, Dict[str, Any]]:
        """
        Converts a CQL2 filter expression into a buildpg-compatible SQL template
        and a dictionary of parameters.
        """
        if not self.filter_expr:
            return "", {}

        final_params: Dict[str, Any] = {}
        param_counter = 0

        expr = cql2.Expr(self.filter_expr)
        query_template = "AND " + expr.to_sql()

        def property_replacer(match: re.Match) -> str:
            root_properties = {"geometry", "id", "collection"}
            nonlocal param_counter
            param_counter += 1
            param_name = f"prop_{param_counter}"

            prefix = match.group(1)  # 'first' or 'second'
            prop_name = match.group(
                2
            )  # The property name, e.g., 'datetime' or 'grid:code'
            # namespaced fields with colons need special handling:
            if ":" in prop_name:
                # The parameter value is the property name string itself
                final_params[param_name] = prop_name
                if prop_name == "geometry":
                    return f"{prefix}.feature->'{param_name}'"
                elif prop_name in root_properties:
                    # Handle root properties like 'id'
                    return f"{prefix}.feature->>:{param_name}"
                else:
                    # Handle nested properties like 'datetime'
                    return f"{prefix}.feature->'properties'->>:{param_name}"
            else:
                if prop_name == "geometry":
                    return f"{prefix}.feature->'{prop_name}'"
                elif prop_name in root_properties:
                    # Handle root properties like 'id'
                    return f"{prefix}.feature->>'{prop_name}'"
                else:
                    # Handle nested properties like 'datetime'
                    return f"{prefix}.feature->'properties'->>'{prop_name}'"

        # This pattern finds 'first.' or 'second.' followed by a property name.
        pattern = r'"?\b(first|second)\.([\w:]+)"?'
        final_template = re.sub(pattern, property_replacer, query_template)
        logger.debug(f"Final template: {final_template}, params: {final_params}")
        return final_template, final_params

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
            bbox = values.get(f"{prefix}-bbox")
            intersects = values.get(f"{prefix}-intersects")
            collections = values.get(f"{prefix}-collections")
            ids = values.get(f"{prefix}-ids")
            if bbox and intersects:
                raise ValueError(
                    f"{prefix}-intersects and {prefix}-bbox parameters are mutually exclusive"
                )
            if bbox:
                if isinstance(bbox, str):
                    values[f"{prefix}-bbox"] = list(map(float, bbox.split(",")))
            if intersects:
                if isinstance(intersects, str):
                    values[f"{prefix}-intersects"] = json.loads(intersects)
            if isinstance(collections, str):
                values[f"{prefix}-collections"] = collections.split(",")
            if ids:
                if isinstance(ids, str):
                    values[f"{prefix}-ids"] = [ids]
        filter_expr = values.get("filter_expr")
        if filter_expr:
            try:
                cql2.Expr(filter_expr).validate()  # will raise if invalid
            except Exception as exc:
                raise ValueError(f"Invalid CQL2 filter expression: {exc}") from exc
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

    def first_search_params(self) -> BaseSearchPostRequest:
        return BaseSearchPostRequest(
            bbox=self.first_bbox,
            datetime=self.first_datetime,
            intersects=self.first_intersects,
            ids=self.first_ids,
            collections=self.first_collections,
            limit=int(
                os.getenv(
                    "PAIR_SEARCH_COLLECTION_SEARCH_LIMIT", COLLECTION_SEARCH_LIMIT
                )
            ),
        )

    def second_search_params(self) -> BaseSearchPostRequest:
        return BaseSearchPostRequest(
            bbox=self.second_bbox,
            datetime=self.second_datetime,
            intersects=self.second_intersects,
            ids=self.second_ids,
            collections=self.second_collections,
            limit=int(
                os.getenv(
                    "PAIR_SEARCH_COLLECTION_SEARCH_LIMIT", COLLECTION_SEARCH_LIMIT
                )
            ),
        )


class PairSearchLinks(BaseLinks):
    """Create inferred links specific to collections."""

    def link_self(self) -> Dict:
        """Return the self link."""
        return {
            "rel": Relations.self.value,
            "type": MimeTypes.geojson.value,
            "href": self.resolve("pair-search"),
        }
