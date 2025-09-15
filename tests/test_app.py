import json

import pytest


def test_app(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()


def test_search(client):
    response = client.get("/search")
    assert response.status_code == 200
    assert response.json()


@pytest.mark.parametrize(
    "params",
    [
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
        },
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "limit": 1,
        },
    ],
)
@pytest.mark.parametrize("method", ["get", "post"])
def test_pair_search(client, params: dict, method: str):
    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    # make sure we have features in the response
    if "limit" in params:
        assert len(response.json()["features"]) == params["limit"]
    else:
        assert len(response.json()["features"]) > 1


@pytest.mark.parametrize("response_type", ["pair", "first-only", "second-only"])
@pytest.mark.parametrize("method", ["get", "post"])
def test_pair_search_response_types(client, method: str, response_type: str):
    """
    response-type = "pair":
    {
        "type": “FeatureCollection”
        "featurePairs": [
            [<itemId#1>, <itemId#2>],
            [<itemId#3>, <itemId#2>]
        ],
        "features": [
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#1>,
            ...
            },
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#2>,
            ...
            },
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#3>,
            ...
            }
        ],
        "links": [
        ...
        ],
        "numberReturned": 3,
        "numberPairsReturned": 2,
        "numberPairsMatched": 100
    }

    response-type = "first-only" or "second-only":
    {
        "type": “FeatureCollection”
        "features": [
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#1>,
            ...
            },
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#3>,
            ...
            }
        ],
        "links": [
        ...
        ],
        "numberReturned": 2,
        "numberMatched": 2
    }
    """
    params = {
        "first-collections": ["ENVISAT.ASA.IMS_1P"],
        "second-collections": ["ENVISAT.ASA.IMS_1P"],
        "response-type": response_type,
    }
    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    response_json = response.json()

    if response_type == "pair":
        assert "featurePairs" in response_json
        feature_pairs = response_json["featurePairs"]
        assert isinstance(feature_pairs, list)
        assert feature_pairs
        assert all(isinstance(pair, list) and len(pair) == 2 for pair in feature_pairs)

    assert "numberPairsReturned" in response_json
    assert response_json["numberPairsReturned"] > 0

    assert "numberPairsMatched" in response_json
    assert response_json["numberPairsMatched"] > 0
