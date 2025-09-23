#!/usr/bin/env python3
"""Ingest sample data during docker-compose"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import requests
import tqdm
from pystac_client import Client
from urllib.parse import urljoin

# Config
CATALOG_URL = "https://eocat.esa.int/eo-catalogue"
COLLECTION_NAME = "ENVISAT.ASA.IMS_1P"
BBOX: List[float] = [7.5, 44, 10.5, 44]
DATETIME = "2010-01-01/2010-12-31"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

SCRIPT_DIR = Path(__file__).resolve().parent
TESTDATA_DIR = SCRIPT_DIR.parent / "tests" / "testdata"


def post_or_put(session: requests.Session, url: str, data: Dict) -> None:
    """Post or put data to STAC endpoint."""
    try:
        response = session.post(url, json=data)
        if response.status_code == 409:  # Already exists, update instead
            new_url = f"{url}/{data['id']}"
            response = session.put(new_url, json=data)
            if response.status_code != 404:  # Ignore unchanged update errors
                response.raise_for_status()
        else:
            response.raise_for_status()
    except requests.RequestException as e:
        logging.error("Failed to post/put data to %s: %s", url, e)
        raise


def collection_exists(session: requests.Session, app_host: str, collection_name: str) -> bool:
    """Check if collection already exists with items."""
    url = urljoin(app_host, f"/collections/{collection_name}/items")
    try:
        response = session.get(url)
        response.raise_for_status()
        return bool(response.json().get("features"))
    except requests.RequestException as e:
        logging.warning("Failed to check collection existence: %s", e)
        return False


def download_collection_metadata(
    session: requests.Session, catalog_url: str, collection_name: str, collection_json: Path
) -> None:
    """Download collection metadata from remote catalog."""
    url = urljoin(catalog_url, f"/collections/{collection_name}")
    response = session.get(url)
    response.raise_for_status()
    collection_json.write_text(response.text)


def download_items(
    catalog_url: str, collection_name: str, bbox: List[float], datetime: str, items_dir: Path
) -> None:
    """Download items for collection into local dir."""
    client = Client.open(catalog_url)
    results = client.search(collections=[collection_name], bbox=bbox, datetime=datetime)

    for item in tqdm.tqdm(results.items(), desc="Downloading items", unit="item"):
        if item.collection_id == collection_name:
            item_path = items_dir / f"{item.id}.json"
            item_path.parent.mkdir(parents=True, exist_ok=True)
            item_path.write_text(json.dumps(item.to_dict(), indent=2))


def ingest_data(
    app_host: str,
    catalog_url: str = CATALOG_URL,
    collection_name: str = COLLECTION_NAME,
    datetime: str = DATETIME,
    bbox: List[float] = BBOX,
) -> None:
    """Ingest data into local STAC instance."""
    if not app_host.startswith("http"):
        app_host = f"http://{app_host}"

    collection_dir = TESTDATA_DIR / collection_name
    collection_json = collection_dir / "collection.json"
    items_dir = collection_dir / "items"

    with requests.Session() as session:
        # Skip if collection already ingested
        if collection_exists(session, app_host, collection_name):
            logging.info("Collection %s already ingested, skipping.", collection_name)
            return

        # Ensure testdata dir exists and has data
        if not collection_dir.exists() or not any(collection_dir.iterdir()):
            collection_dir.mkdir(parents=True, exist_ok=True)
            items_dir.mkdir(parents=True, exist_ok=True)

            logging.info("Downloading collection metadata...")
            download_collection_metadata(session, catalog_url, collection_name, collection_json)

            logging.info("Downloading items from remote catalog...")
            download_items(catalog_url, collection_name, bbox, datetime, items_dir)

        # Ingest collection
        logging.info("Ingesting collection %s...", collection_name)
        post_or_put(session, urljoin(app_host, "/collections"), json.loads(collection_json.read_text()))

        # Ingest items
        for item_json in tqdm.tqdm(items_dir.iterdir(), desc="Ingesting items", unit="item"):
            post_or_put(
                session,
                urljoin(app_host, f"/collections/{collection_name}/items"),
                json.loads(item_json.read_text()),
            )


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: script.py <stac_host:port>")

    logging.info("Loading test data...")
    ingest_data(sys.argv[1])
    logging.info("All done!")
