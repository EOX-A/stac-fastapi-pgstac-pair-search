import json

import pytest


def assert_pairs_match_control(
    client, method: str, response_type: str, params: dict, control: list
):
    params["response-type"] = response_type
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
        assert response_json["numberPairsReturned"] == len(control)
        feature_pairs = response_json["featurePairs"]
        assert len(feature_pairs) == len(control)
        assert set(map(tuple, feature_pairs)) == set(map(tuple, control))

    elif response_type == "first-only":
        features = response_json["features"]
        assert len(features) == len(control)
        feature_ids = {feature["id"] for feature in features}
        control_ids = {first for first, _ in control}
        assert feature_ids == control_ids

    elif response_type == "second-only":
        features = response_json["features"]
        assert len(features) == len(control)
        feature_ids = {feature["id"] for feature in features}
        control_ids = {second for _, second in control}
        assert feature_ids == control_ids


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("response_type", ["pair", "first-only", "second-only"])
def test_00_no_constraints(client, method: str, response_type: str, control_00: list):
    """
    Test 00: No constraints

    matched pairs: 90
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID
    parameters: []
    """
    assert_pairs_match_control(
        client,
        method,
        response_type,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "limit": 100,
        },
        control_00,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_01_pair_order(client, method: str, control_01: list):
    """
    Test 01: orderes pairs, first before second

    matched pairs: 45
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND second.beginAcquisition > first.beginAcquisition
    parameters: []
    """
    #   filter=((N_DIFF(second.oads:baseline_perpendicular_offset, first.oads:baseline_perpendicular_offset) BETWEEN-500 AND 500) AND (T_DIFF(T_START(first.datetime),T_START(seconds.datetime)) BETWEEN TimeDelta('0D') ANDTimeDelta('356D') AND (first.sat:orbit_state = second.sat:orbit_state)AND (first.sar:beam_id = second.sar:beam_id) AND(first.sar:polarization = second.sar:polarization) AND(first.oads:wrs_longitude_grid = second.oads:wrs_longitude_grid) AND(first.oads:wrs_latitude_grid = second.oads:wrs_latitude_grid) AND(first.oads:mission_phase = second.oads:mission_phase))
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": "second.datetime > first.datetime",
        },
        control_01,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_02_area_overlap(client, method: str, control_02: list):
    """
    Test 02: pairs with 75% area overlap

    matched pairs: 4
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND ST_Area(ST_Intersection(second.footprint, first.footprint))/MIN(ST_Area(second.footprint),ST_Area(first.footprint)) >= ?
    parameters: [0.75]
    """
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": None,  # TODO: add actual filter
        },
        control_02,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_03_wrs_grid(client, method: str, control_03: list):
    """
    Test 03: pairs sharing the same WRS grid

    matched pairs: 2
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND (second.wrsLongitudeGrid = first.wrsLongitudeGrid) AND (second.wrsLatitudeGrid = first.wrsLatitudeGrid)
    parameters: []
    """
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": None,  # TODO: add actual filter
        },
        control_03,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_04_timedelta(client, method: str, control_04: list):
    """
    Test 04: second product more than 35 days after first

    matched pairs: 26
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND second.beginAcquisition > (first.beginAcquisition + ?)
    parameters: [3024000000]
    Note: Time stored as number of milliseconds since 1970-01-01
    """
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": None,  # TODO: add actual filter
        },
        control_04,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_05_same_track(client, method: str, control_05: list):
    """
    Test 05: second after first, the same track/frame (grid code)

    matched pairs: 1
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND (second.wrsLongitudeGrid = first.wrsLongitudeGrid) AND (second.wrsLatitudeGrid = first.wrsLatitudeGrid) AND (second.beginAcquisition > first.beginAcquisition)
    parameters: []
    """
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": None,  # TODO: add actual filter
        },
        control_05,
    )


@pytest.mark.skip(reason="Skipped as filter query is not yet implemented")
@pytest.mark.parametrize("method", ["get", "post"])
def test_06_timedelta_overlap(client, method: str, control_06: list):
    """
    Test 06: second product more than 35 days after first and 75% area overlap

    matched pairs: 1
    query:      SELECT first.productId,second.productId FROM searchables AS first,searchables AS second WHERE first.ROWID != second.ROWID AND (second.beginAcquisition > (first.beginAcquisition + ?)) AND (ST_Area(ST_Intersection(second.footprint, first.footprint))/MIN(ST_Area(second.footprint),ST_Area(first.footprint)) >= ?)
    parameters: [3024000000, 0.75]
    Note: Time stored as number of milliseconds since 1970-01-01
    """
    assert_pairs_match_control(
        client,
        method,
        {
            "first-collections": ["ENVISAT.ASA.IMS_1P"],
            "second-collections": ["ENVISAT.ASA.IMS_1P"],
            "response-type": "pair",
            "limit": 100,
            "filter": None,  # TODO: add actual filter
        },
        control_06,
    )
