import json


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


def test_pair_search_limit_post(client):
    response = client.post("/pair-search", json=json.dumps({"limit": 1}))
    assert response.status_code == 200
    # make sure we have exactly one feature in the response
    assert len(response.json()["features"]) == 1
