from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Set, List

from cql2 import Expr
from asyncpg.exceptions import InvalidDatetimeFormatError
from buildpg import render
from fastapi import Request
from pypgstac.hydration import hydrate
from stac_fastapi.api.routes import create_async_endpoint
from stac_fastapi.api.models import GeoJSONResponse, JSONResponse
from stac_fastapi.pgstac.app import StacApi
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.pgstac.core import CoreCrudClient
from stac_fastapi.pgstac.models.links import ItemLinks
from stac_fastapi.pgstac.utils import filter_fields
from stac_fastapi.types.errors import InvalidQueryParameter
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.stac import ItemCollection
from stac_pydantic.item import Item

from stac_fastapi_pgstac_pair_search.models import PairSearchRequest, PairSearchLinks
from stac_fastapi_pgstac_pair_search.extensions.pair_search import PairSearchExtension


logger = logging.getLogger(__name__)


PAIR_SEARCH_SQL = (Path(__file__).parent / "sql" / "pair_search.sql").read_text()


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

        # INFO: check the SQL code to see what configuration options are applied
        search_request.conf = search_request.conf or {}
        search_request.conf["nohydrate"] = settings.use_api_hydrate

        search_request_json = json.dumps(
            self._sanitize_pair_search_request(search_request.dict(by_alias=True))
        )

        try:
            async with request.app.state.get_connection(request, "r") as conn:
                query, params = render(
                    "SELECT * FROM pair_search_alt(:request::text::jsonb);",
                    request=search_request_json,
                )
                items = await conn.fetchval(query, *params)
        except InvalidDatetimeFormatError as e:
            raise InvalidQueryParameter(
                f"Datetime parameter {search_request.datetime} is invalid."
            ) from e

        # extract pagination information and reset the links
        link_parameters = {
            link.get("rel"): link.get("parameters") for link in items.get("links") or []
        }
        items["links"] = []

        collection = ItemCollection(**items)

        collection["features"] = await self._finalize_items(
            collection.get("features") or [],
            search_request=search_request,
            request=request,
        )

        collection["links"] = await self._get_search_links(
            link_parameters=link_parameters,
            search_request=search_request,
            request=request,
        )

        return collection

    def _sanitize_pair_search_request(
        self,
        query: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Fix pair search parameters to format expected by the SQL query."""

        def _fix_filter(query):
            """fix some issues introduced by the buggy cql2 v0.4.x parser"""
            if isinstance(query, float):
                if int(query) == query:
                    return int(query)
            if isinstance(query, dict):
                if "op" in query:
                    # negative number expressed as -1 * <positive number>
                    if (
                        query["op"] == "*"
                        and query["args"][0] == -1
                        and isinstance(query["args"][1], (float, int))
                    ):
                        return _fix_filter(-query["args"][1])
                    # fix nested arguments
                    query["args"] = [_fix_filter(item) for item in query["args"]]
            return query

        if query["filter"]:
            if query["filter_lang"] == "cql2-text":
                query["filter"] = Expr(query["filter"]).to_json()
                query["filter_lang"] = "cql2-json"
            query["filter"] = _fix_filter(query["filter"])
        else:
            query["filter"] = None

        if query.get("fields"):
            includes = set()
            excludes = set()
            for field in query["fields"]:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)

            query["fields"] = {"include": includes, "exclude": excludes}

        query = {
            key: value
            for key, value in query.items()
            if value is not None and value != []
        }

        return query

    async def _finalize_items(
        self,
        features: list[Item],
        search_request: PairSearchRequest,
        request: Request,
    ) -> list[Item]:
        settings: Settings = request.app.state.settings

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

            for feature in features:
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
            for feature in features or []:
                await _add_item_links(feature)
                cleaned_features.append(feature)

        return cleaned_features

    async def _get_search_links(
        self,
        link_parameters: dict[str, dict[str, Any]],
        search_request: PairSearchRequest,
        request: Request,
    ) -> List[Dict[str, str]]:
        """Take existing request and edit offset."""

        base_url = get_base_url(request)

        if request.method == "GET":
            query_params = dict(request.query_params.multi_items())

            def _get_link(rel: str, extra_params: dict[str, Any] | None = None):
                return {
                    "rel": rel,
                    "method": "GET",
                    "href": request.url.replace_query_params(
                        **{**query_params, **(extra_params or {})}
                    ),
                    "type": "application/geo+json",
                }

        elif request.method == "POST":
            query_params = await request.body()
            if isinstance(query_params, bytes):
                query_params = json.loads(query_params.decode("utf-8"))

            def _get_link(rel: str, extra_params: dict[str, Any] | None = None):
                return {
                    "rel": rel,
                    "method": "POST",
                    "href": request.url,
                    "body": {**query_params, **(extra_params or {})},
                    "type": "application/geo+json",
                }

        def _generate_links():
            yield {
                "rel": "root",
                "href": base_url,
                "type": "application/json",
            }
            yield {
                "rel": "self",
                "href": request.url,
                "type": "application/geo+json",
            }
            if "next" in link_parameters:
                yield _get_link("next", link_parameters["next"])
            if "prev" in link_parameters:
                yield _get_link("prev", link_parameters["prev"])

        return list(_generate_links())


def register_pair_search(api: StacApi):
    """Shoehorn pair-search on an instantiated StacApi."""

    # initialize the client
    pair_search_client = PairSearchClient(
        stac_version=api.stac_version,
        landing_page_id=api.settings.stac_fastapi_landing_id,
        title=api.settings.stac_fastapi_title,
        description=api.settings.stac_fastapi_description,
        extensions=api.extensions,
    )

    # this is required to add conformance classes:
    pair_search_extension = PairSearchExtension()
    api.extensions.append(pair_search_extension)
    api.client.extensions.append(pair_search_extension)

    # add api routes:
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

    logger.debug("Registered /pair-search endpoint")

    # logger.debug("Loading pair search SQL functions into database")
    # from pypgstac.db import PgstacDB
    # with PgstacDB(commit_on_exit=True).connect(
    # ) as conn:
    #     with conn.cursor() as cur:
    #         for file in [
    #             "n_diff.sql",
    #             "s_raoverlap.sql",
    #             "t_diff.sql",
    #             "t_end.sql",
    #             "t_start.sql",
    #         ]:
    #             load_functions_sql = (Path(__file__).parent / "sql" / file).read_text()
    #             cur.execute(load_functions_sql)
    #     conn.commit()
