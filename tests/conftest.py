import pytest

from fastapi.testclient import TestClient

from stac_fastapi_pgstac_pair_search.app import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client
