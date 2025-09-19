import json
from pathlib import Path

import pytest

from fastapi.testclient import TestClient

from stac_fastapi_pgstac_pair_search.app import app


def load_control_json(file_name: str):
    control_path = Path(__file__).parent / "testdata" / file_name
    with open(control_path, "r") as f:
        control = json.load(f)
    return control


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client


@pytest.fixture(scope="session")
def control_00():
    return load_control_json("control_00.json")


@pytest.fixture(scope="session")
def control_01():
    return load_control_json("control_01.json")


@pytest.fixture(scope="session")
def control_02():
    return load_control_json("control_02.json")


@pytest.fixture(scope="session")
def control_03():
    return load_control_json("control_03.json")


@pytest.fixture(scope="session")
def control_04():
    return load_control_json("control_04.json")


@pytest.fixture(scope="session")
def control_05():
    return load_control_json("control_05.json")


@pytest.fixture(scope="session")
def control_06():
    return load_control_json("control_06.json")
