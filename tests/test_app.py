import json

import pytest


def test_app(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()


@pytest.mark.parametrize(
    "class_name",
    [
        "pair-search",
        "number-difference",
        "time-difference",
        "relative-geometry-overlap",
    ],
)
def test_conformance(client, class_name):
    """
    {
        "conformsTo": [
            "https://api.stacspec.org/v1.0.0/core",
            "https://api.stacspec.org/v1.0.0/item-search",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "...",
            "// Your new Pair Search classes //",
            "https://api.stacspec.org/v1.0.0/pair-search",
            "// Your custom CQL2 extension classes //",
            "https://eox.at/ext/cq12/1.0/conf/number-difference",
            "https://eox.at/ext/cq12/1.0/conf/time-difference",
            "https://eox.at/ext/cq12/1.0/conf/relative-geometry-overlap"
        ]
    }
    """

    response = client.get("/conformance")
    assert response.status_code == 200
    assert response.json()

    for conformance_class_uri in response.json()["conformsTo"]:
        if class_name in conformance_class_uri:
            break
    else:
        raise ValueError(f"{class_name} not found")


def test_search(client):
    response = client.get("/search")
    assert response.status_code == 200
    assert response.json()


def test_collection_queryables(client):
    response = client.get("/collections/ENVISAT.ASA.IMS_1P/queryables")
    assert response.status_code == 200
    assert response.json()
    assert "grid:code" in response.json()["properties"]


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
def test_pair_search_limit(client, params: dict, method: str):
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
        assert len(response.json()["featurePairs"]) == params["limit"]
    else:
        assert len(response.json()["featurePairs"]) == 10


@pytest.mark.parametrize("response_type", ["pair", "first-only", "second-only"])
@pytest.mark.parametrize("method", ["get", "post"])
def test_pair_search_response(client, method: str, response_type: str):
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
        assert "numberPairsReturned" in response_json
        assert response_json["numberPairsReturned"] > 0

        assert "numberPairsMatched" in response_json
        assert response_json["numberPairsMatched"] > 0

        assert "featurePairs" in response_json
        feature_pairs = response_json["featurePairs"]
        assert isinstance(feature_pairs, list)
        assert feature_pairs
        assert all(isinstance(pair, list) and len(pair) == 2 for pair in feature_pairs)

        assert len(feature_pairs) == response_json["numberPairsReturned"]

        # make sure all item ids in featurePairs are present in features
        pair_feature_ids = {item_id for pair in feature_pairs for item_id in pair}
        feature_ids = {feature["id"] for feature in response_json["features"]}
        assert pair_feature_ids == feature_ids

        # make sure pairs do not contain identical items
        for first, second in feature_pairs:
            assert first != second


# https://pair-search-demo.eox.at/catalogue/pair-search?
#   response-type=pair&
#   first-collection=ASA_IMS_1P&
#   second-collection=ASA_IMS_1P&
#   first-bbox=12.6,41.8,12.7,41.9&
#   first-datetime=2003-01-01T00:00Z/2004-01-01T00:00Z&
#   filter=((N_DIFF(second.oads:baseline_perpendicular_offset, first.oads:baseline_perpendicular_offset) BETWEEN-500 AND 500) AND (T_DIFF(T_START(first.datetime),T_START(seconds.datetime)) BETWEEN TimeDelta('0D') ANDTimeDelta('356D') AND (first.sat:orbit_state = second.sat:orbit_state)AND (first.sar:beam_id = second.sar:beam_id) AND(first.sar:polarization = second.sar:polarization) AND(first.oads:wrs_longitude_grid = second.oads:wrs_longitude_grid) AND(first.oads:wrs_latitude_grid = second.oads:wrs_latitude_grid) AND(first.oads:mission_phase = second.oads:mission_phase))
