from __future__ import annotations
import logging
from pathlib import Path
import re
from typing import Dict, Any, Optional, Set, List, Tuple

from asyncpg.exceptions import InvalidDatetimeFormatError
from buildpg import render
import cql2
from fastapi import Request
from pypgstac.hydration import hydrate
from stac_fastapi.api.routes import create_async_endpoint
from stac_fastapi.api.models import GeoJSONResponse, JSONResponse
from stac_fastapi.pgstac.app import StacApi
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.models.links import ItemLinks, PagingLinks
from stac_fastapi.pgstac.utils import filter_fields
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.stac import ItemCollection
from stac_pydantic.item import Item

from stac_fastapi_pgstac_pair_search.models import PairSearchRequest, PairSearchLinks


logger = logging.getLogger(__name__)


pair_search_sql = (Path(__file__).parent / "sql" / "pair_search.sql").read_text()


class PairSearchClient(CoreCrudClient):
    """A custom client for pair searching."""

    pgstac_search_model = PairSearchRequest

    async def get_pair_search(
        self,
        request: Request,
        **__: Any,
    ) -> ItemCollection:
        """Cross catalog search (GET).

        Called with `GET /pair-search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        item_collection = await self._pair_search_base(
            PairSearchRequest.model_validate(request.query_params._dict, by_alias=True),
            request=request,
        )
        links = await PairSearchLinks(request=request).get_links(
            extra_links=item_collection["links"]
        )
        item_collection["links"] = links
        return item_collection

    async def post_pair_search(
        self,
        request: Request,
        **__: Any,
    ) -> ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        body = await request.body()
        search_request = PairSearchRequest.model_validate_json(
            body.decode(), by_alias=True
        )
        item_collection = await self._pair_search_base(search_request, request=request)

        # If we have the `fields` extension enabled
        # we need to avoid Pydantic validation because the
        # Items might not be a valid STAC Item objects
        if fields := getattr(search_request, "fields", None):
            if fields.include or fields.exclude:
                return JSONResponse(item_collection)  # type: ignore

        links = await PairSearchLinks(request=request).get_links(
            extra_links=item_collection["links"]
        )
        item_collection["links"] = links

        return ItemCollection(**item_collection)

    async def _pair_search_base(
        self, search_request: PairSearchRequest, request: Request
    ) -> ItemCollection:
        """Cross catalog pair search (GET).

        Called with `GET /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        items: Dict[str, Any]

        settings: Settings = request.app.state.settings

        # TODO: don't know where this .conf is coming from
        # search_request.conf = search_request.conf or {}
        # search_request.conf["nohydrate"] = settings.use_api_hydrate

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                query, params = render_sql(search_request)
                items = await conn.fetchval(query, *params)
        except InvalidDatetimeFormatError as e:
            raise InvalidQueryParameter(
                f"Datetime parameter {search_request.datetime} is invalid."
            ) from e
        # Starting in pgstac 0.9.0, the `next` and `prev` tokens are returned in spec-compliant links with method GET
        next_from_link: Optional[str] = None
        prev_from_link: Optional[str] = None
        for link in items.get("links", []):
            if link.get("rel") == "next":
                next_from_link = link.get("href").split("token=next:")[1]
            if link.get("rel") == "prev":
                prev_from_link = link.get("href").split("token=prev:")[1]

        next: Optional[str] = items.pop("next", next_from_link)
        prev: Optional[str] = items.pop("prev", prev_from_link)
        collection = ItemCollection(**items)

        fields = getattr(search_request, "fields", None)
        include: Set[str] = fields.include if fields and fields.include else set()
        exclude: Set[str] = fields.exclude if fields and fields.exclude else set()

        async def _add_item_links(
            feature: Item,
            collection_id: Optional[str] = None,
            item_id: Optional[str] = None,
        ) -> None:
            """Add ItemLinks to the Item.

            If the fields extension is excluding links, then don't add them.
            Also skip links if the item doesn't provide collection and item ids.
            """
            collection_id = feature.get("collection") or collection_id
            item_id = feature.get("id") or item_id

            if not exclude or "links" not in exclude and all([collection_id, item_id]):
                feature["links"] = await ItemLinks(
                    collection_id=collection_id,  # type: ignore
                    item_id=item_id,  # type: ignore
                    request=request,
                ).get_links(extra_links=feature.get("links"))

        cleaned_features: List[Item] = []

        if settings.use_api_hydrate:

            async def _get_base_item(collection_id: str) -> Dict[str, Any]:
                return await self._get_base_item(collection_id, request=request)

            base_item_cache = settings.base_item_cache(
                fetch_base_item=_get_base_item, request=request
            )

            for feature in collection.get("features") or []:
                base_item = await base_item_cache.get(feature.get("collection"))
                # Exclude None values
                base_item = {k: v for k, v in base_item.items() if v is not None}

                feature = hydrate(base_item, feature)

                # Grab ids needed for links that may be removed by the fields extension.
                collection_id = feature.get("collection")
                item_id = feature.get("id")

                feature = filter_fields(feature, include, exclude)
                await _add_item_links(feature, collection_id, item_id)

                cleaned_features.append(feature)
        else:
            for feature in collection.get("features") or []:
                await _add_item_links(feature)
                cleaned_features.append(feature)

        collection["features"] = cleaned_features
        collection["links"] = await PagingLinks(
            request=request,
            next=next,
            prev=prev,
        ).get_links()
        return collection


def render_sql(pair_search_request: PairSearchRequest) -> Tuple[str, List[Any]]:
    return render(
        pair_search_sql.replace("{filter_expr}", cql2_to_sql(pair_search_request)),
        first_req=pair_search_request.first_search_params().model_dump_json(
            exclude_none=True, by_alias=True
        ),
        second_req=pair_search_request.second_search_params().model_dump_json(
            exclude_none=True, by_alias=True
        ),
        # filter=pair_search_request.filter_expr,
        limit=pair_search_request.limit or 10,
        response_type=pair_search_request.response_type,
    )


def cql2_to_sql(pair_search_request: PairSearchRequest) -> str:
    """
    Convert instances of first.<property> to first->>'<property>'.
    """
    if pair_search_request.filter_expr:
        # (first->>'datetime') > (second->>'datetime')
        raw_query = "AND " + cql2.Expr(pair_search_request.filter_expr).to_sql().query
        logger.debug(raw_query)
        cleaned_query = re.sub(
            r'"?(first|second)\.(\w+)"*', r"\1.feature->'properties'->>'\2'", raw_query
        )
        logger.debug(cleaned_query)
        return cleaned_query
    return ""


def register_pair_search(api: StacApi):
    # initialize the client
    pair_search_client = PairSearchClient(
        stac_version=api.stac_version,
        landing_page_id=api.settings.stac_fastapi_landing_id,
        title=api.settings.stac_fastapi_title,
        description=api.settings.stac_fastapi_description,
        extensions=api.extensions,
    )

    # POST /pair-search
    api.app.add_api_route(
        name="Pair Search",
        path="/pair-search",
        response_model=ItemCollection if api.settings.enable_response_models else None,
        responses={
            200: {
                "content": {
                    "application/geo+json": {},
                },
                "model": ItemCollection,
            },
        },
        response_class=GeoJSONResponse,
        response_model_exclude_unset=True,
        response_model_exclude_none=True,
        methods=["POST"],
        endpoint=create_async_endpoint(
            pair_search_client.post_pair_search, PairSearchRequest
        ),
    )

    # GET /pair-search
    api.app.add_api_route(
        name="Pair Search",
        path="/pair-search",
        response_model=ItemCollection if api.settings.enable_response_models else None,
        responses={
            200: {
                "content": {
                    "application/geo+json": {},
                },
                "model": ItemCollection,
            },
        },
        response_class=GeoJSONResponse,
        response_model_exclude_unset=True,
        response_model_exclude_none=True,
        methods=["GET"],
        endpoint=create_async_endpoint(
            pair_search_client.get_pair_search, PairSearchRequest
        ),
    )
