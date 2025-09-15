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


def test_pair_search_empty_get(client):
    response = client.get("/pair-search")
    assert response.status_code == 200
    # make sure we have features in the response
    assert response.json()["features"]


@pytest.mark.skip(reason="POST not yet working")
def test_pair_search_empty_post(client):
    response = client.post("/pair-search", json={})
    assert response.status_code == 200
    # make sure we have features in the response
    assert response.json()["features"]


def test_pair_search_limit_get(client):
    response = client.get("/pair-search?limit=1")
    assert response.status_code == 200
    # make sure we have exactly one feature in the response
    assert len(response.json()["features"]) == 1


@pytest.mark.skip(reason="POST not yet working")
def test_pair_search_limit_post(client):
    response = client.post("/pair-search", json=json.dumps({"limit": 1}))
    assert response.status_code == 200
    # make sure we have exactly one feature in the response
    assert len(response.json()["features"]) == 1


def test_pair_response(client):
    """
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
    """
    response = client.get(
        "/pair-search",
        params=dict(
            first_collections="ENVISAT.ASA.IMS_1P",
            second_collections="ENVISAT.ASA.IMS_1P",
            response_type="pair",
        ),
    )
    assert response.status_code == 200
    response_json = response.json()

    assert "featurePairs" in response_json
    feature_pairs = response_json["featurePairs"]
    assert isinstance(feature_pairs, list)
    assert feature_pairs
    assert all(isinstance(pair, list) and len(pair) == 2 for pair in feature_pairs)

    assert "numberPairsReturned" in response_json
    assert response_json["numberPairsReturned"] > 0

    assert "numberPairsMatched" in response_json
    assert response_json["numberPairsMatched"] > 0


def test_first_only_response(client):
    """
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
    response = client.get(
        "/pair-search",
        params=dict(
            first_collections="ENVISAT.ASA.IMS_1P",
            second_collections="ENVISAT.ASA.IMS_1P",
            response_type="first-only",
        ),
    )

    assert response.status_code == 200
    response_json = response.json()

    assert "numberReturned" in response_json
    assert response_json["numberReturned"] > 0

    assert "numberMatched" in response_json
    assert response_json["numberMatched"] > 0


def test_second_only_response(client):
    """
    {
        "type": “FeatureCollection”
        "features": [
            {
                "type": “Feature”,
                "stac_version": “1.1.0”,
                "id": <itemId#2>,
            ...
            }
        ],
        "links": [
        ...
        ],
        "numberReturned": 1,
        "numberMatched": 1
    }
    """
    response = client.get(
        "/pair-search",
        params=dict(
            first_collections="ENVISAT.ASA.IMS_1P",
            second_collections="ENVISAT.ASA.IMS_1P",
            response_type="second-only",
        ),
    )

    assert response.status_code == 200
    response_json = response.json()

    assert "numberReturned" in response_json
    assert response_json["numberReturned"] > 0

    assert "numberMatched" in response_json
    assert response_json["numberMatched"] > 0
