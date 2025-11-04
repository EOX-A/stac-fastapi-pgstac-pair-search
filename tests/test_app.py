import json

import pytest
from shapely.geometry import box, mapping


def test_app(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()


@pytest.mark.parametrize(
    "class_name",
    [
        "pair-search",
        "n_diff",
        "t_diff",
        "t_start",
        "t_end",
        "s_raoverlap",
    ],
)
def test_conformance(client, class_name):
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

        # assert "numberPairsMatched" in response_json
        # assert response_json["numberPairsMatched"] > 0

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


# first-bbox, second-bbox
@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("first_bbox", [None, [6.21203, 43.36210, 6.35085, 43.48009]])
@pytest.mark.parametrize("second_bbox", [None, [6.21203, 43.36210, 6.35085, 43.48009]])
def test_bbox(client, method: str, first_bbox, second_bbox):
    # bbox only intersects with product ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000
    intersecting_id = "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"
    params = {
        "first-collections": ["ENVISAT.ASA.IMS_1P"],
        "second-collections": ["ENVISAT.ASA.IMS_1P"],
    }
    if first_bbox:
        params["first-bbox"] = ",".join(map(str, first_bbox))
    if second_bbox:
        params["second-bbox"] = ",".join(map(str, second_bbox))

    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    response_json = response.json()

    # assert "numberPairsReturned" in response_json
    # assert "numberPairsMatched" in response_json

    # special case: if both bboxes are set, no pairs are returned, because there is
    # only one product
    if first_bbox and second_bbox:
        assert response_json["numberPairsReturned"] == 0
        # assert response_json["numberPairsMatched"] == 0
        assert not response_json["featurePairs"]
    else:
        assert response_json["numberPairsReturned"] > 0
        # assert response_json["numberPairsMatched"] > 0
        assert response_json["featurePairs"]
        for first, second in response_json["featurePairs"]:
            if first_bbox:
                assert first == intersecting_id
            if second_bbox:
                assert second == intersecting_id


# first-intersects, second-intersects
@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize(
    "first_intersects", [None, mapping(box(6.21203, 43.36210, 6.35085, 43.48009))]
)
@pytest.mark.parametrize(
    "second_intersects", [None, mapping(box(6.21203, 43.36210, 6.35085, 43.48009))]
)
def test_intersects(client, method: str, first_intersects, second_intersects):
    # bbox only intersects with product ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000
    intersecting_id = "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"
    params = {
        "first-collections": ["ENVISAT.ASA.IMS_1P"],
        "second-collections": ["ENVISAT.ASA.IMS_1P"],
    }
    if first_intersects:
        params["first-intersects"] = json.dumps(first_intersects, separators=(",", ":"))
    if second_intersects:
        params["second-intersects"] = json.dumps(
            second_intersects, separators=(",", ":")
        )

    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    response_json = response.json()

    assert "numberPairsReturned" in response_json
    # assert "numberPairsMatched" in response_json

    # special case: if both bboxes are set, no pairs are returned, because there is
    # only one product
    if first_intersects and second_intersects:
        assert response_json["numberPairsReturned"] == 0
        # assert response_json["numberPairsMatched"] == 0
        assert not response_json["featurePairs"]
    else:
        assert response_json["numberPairsReturned"] > 0
        # assert response_json["numberPairsMatched"] > 0
        assert response_json["featurePairs"]
        for first, second in response_json["featurePairs"]:
            if first_intersects:
                assert first == intersecting_id
            if second_intersects:
                assert second == intersecting_id


# first-ids, second-ids
@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize(
    "first_ids",
    [
        None,
        "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000",
        ["ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"],
    ],
)
@pytest.mark.parametrize(
    "second_ids",
    [
        None,
        "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000",
        ["ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"],
    ],
)
def test_ids(client, method: str, first_ids, second_ids):
    intersecting_id = "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"
    params = {
        "first-collections": ["ENVISAT.ASA.IMS_1P"],
        "second-collections": ["ENVISAT.ASA.IMS_1P"],
    }
    if first_ids:
        params["first-ids"] = first_ids
    if second_ids:
        params["second-ids"] = second_ids

    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    response_json = response.json()

    assert "numberPairsReturned" in response_json
    # assert "numberPairsMatched" in response_json

    # special case: if both bboxes are set, no pairs are returned, because there is
    # only one product
    if first_ids and second_ids:
        assert response_json["numberPairsReturned"] == 0
        # assert response_json["numberPairsMatched"] == 0
        assert not response_json["featurePairs"]
    else:
        assert response_json["numberPairsReturned"] > 0
        # assert response_json["numberPairsMatched"] > 0
        assert response_json["featurePairs"]
        for first, second in response_json["featurePairs"]:
            if first_ids:
                assert first == intersecting_id
            if second_ids:
                assert second == intersecting_id


# first-datetime, second-datetime
@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize(
    "first_datetime",
    [None, "2010-06-02T09:49:53.149000Z", "2010-06-02T00:00:00Z/2010-06-02T23:59:59Z"],
)
@pytest.mark.parametrize(
    "second_datetime",
    [None, "2010-06-02T09:49:53.149000Z", "2010-06-02T00:00:00Z/2010-06-02T23:59:59Z"],
)
def test_datetime(client, method: str, first_datetime, second_datetime):
    # bbox only intersects with product ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000
    intersecting_id = "ASA_IMS_1PNESA20100602_094953_000000152090_00022_43162_0000"
    params = {
        "first-collections": ["ENVISAT.ASA.IMS_1P"],
        "second-collections": ["ENVISAT.ASA.IMS_1P"],
    }
    if first_datetime:
        params["first-datetime"] = first_datetime
    if second_datetime:
        params["second-datetime"] = second_datetime

    response = client.request(
        method=method,
        url="/pair-search",
        params=params if method == "get" else None,
        content=json.dumps(params) if method == "post" else None,
    )
    if response.status_code != 200:
        raise ValueError(f"Response: {response.status_code}, {response.text}")

    response_json = response.json()
    assert "numberPairsReturned" in response_json
    # assert "numberPairsMatched" in response_json

    # special case: if both bboxes are set, no pairs are returned, because there is
    # only one product
    if first_datetime and second_datetime:
        assert response_json["numberPairsReturned"] == 0
        # assert response_json["numberPairsMatched"] == 0
        assert not response_json["featurePairs"]
    else:
        assert response_json["numberPairsReturned"] > 0
        # assert response_json["numberPairsMatched"] > 0
        assert response_json["featurePairs"]
        for first, second in response_json["featurePairs"]:
            if first_datetime:
                assert first == intersecting_id
            if second_datetime:
                assert second == intersecting_id
