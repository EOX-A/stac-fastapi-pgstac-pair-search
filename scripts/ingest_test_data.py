#!/usr/bin/env python3
"""Ingest sample data during docker-compose"""

import sys
from urllib.parse import urljoin

from pystac_client import Client
import requests
import tqdm

CATALOG_URL = "https://eocat.esa.int/eo-catalogue"
COLLECTION_NAME = "ENVISAT.ASA.IMS_1P"
BBOX = [7.5, 44, 10.5, 44]
DATETIME = "2010-01-01/2010-12-31"

try:
    app_host = sys.argv[1]
except IndexError:
    raise Exception("You must include full path/port to stac instance")


def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    response = requests.post(url, json=data)
    if response.status_code == 409:
        new_url = url + f"/{data['id']}"
        # Exists, so update
        response = requests.put(new_url, json=data)
        # Unchanged may throw a 404
        if not response.status_code == 404:
            response.raise_for_status()
    else:
        response.raise_for_status()


def ingest_data(
    app_host: str = app_host,
    catalog_url: str = CATALOG_URL,
    collection_name: str = COLLECTION_NAME,
    datetime: str = DATETIME,
    bbox: list = BBOX,
):
    """ingest data."""
    if not app_host.startswith("http"):
        app_host = f"http://{app_host}"

    # check if collection already exists
    with requests.get(
        urljoin(app_host, f"/collections/{collection_name}/items")
    ) as response:
        if response.json().get("features"):
            print(
                f"Collection {collection_name} already ingested and has items, skipping ingest"
            )
            return

    # get collection metadata from remote catalog
    with requests.get(
        urljoin(catalog_url, f"/collections/{collection_name}")
    ) as response:
        collection_json = response.json()
    post_or_put(urljoin(app_host, "/collections"), collection_json)

    # search for items in remote catalog and ingest
    client = Client.open(catalog_url)
    results = client.search(collections=[collection_name], bbox=bbox, datetime=datetime)
    for item in tqdm.tqdm(results.items(), desc="Ingesting items", unit=" items"):
        if item.collection_id == collection_name:
            post_or_put(
                urljoin(app_host, f"/collections/{collection_name}/items"),
                item.to_dict(),
            )


if __name__ == "__main__":
    print("Loading Test data")
    ingest_data()
    print("All Done")
