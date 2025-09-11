import logging
import os

from stac_fastapi.api.routes import create_async_endpoint
from stac_fastapi.pgstac.app import api
from stac_fastapi.pgstac.config import Settings
from stac_fastapi.types.stac import ItemCollection
from stac_fastapi.api.models import GeoJSONResponse

from stac_fastapi_pgstac_pair_search.client import PairSearchClient
from stac_fastapi_pgstac_pair_search.models import PairSearchRequest

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

api.router.add_api_route(
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
        PairSearchClient().post_pair_search, PairSearchRequest
    ),
)
api.router.add_api_route(
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
        PairSearchClient().get_pair_search, PairSearchRequest
    ),
)
api.__attrs_post_init__()
app = api.app


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        settings = Settings()
        for key, value in settings.model_dump().items():
            logger.info(f"{key}: {value}")
        uvicorn.run(
            "stac_fastapi.pgstac.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="debug",
            reload=settings.reload,
            root_path=os.getenv("UVICORN_ROOT_PATH", ""),
        )
    except ImportError as e:
        raise RuntimeError("Uvicorn must be installed in order to use command") from e
    except Exception as exc:
        raise exc


if __name__ == "__main__":
    run()
