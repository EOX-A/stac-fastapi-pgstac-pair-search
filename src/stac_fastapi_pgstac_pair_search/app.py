import logging
import os

from stac_fastapi.pgstac.app import api
from stac_fastapi.pgstac.config import Settings

from stac_fastapi_pgstac_pair_search.client import register_pair_search

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

register_pair_search(api)

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
