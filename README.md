# STAC Pair Search Extension for stac-fastapi-pgstac

[![CI/CD](https://github.com/EOX-A/stac-fastapi-pgstac-pair-search/actions/workflows/ci.yml/badge.svg)](https://github.com/EOX-A/stac-fastapi-pgstac-pair-search/actions/workflows/ci.yml)
[![PyPI package](https://img.shields.io/badge/pypi-stac-fastapi-pgstac-pair-search-blue)](https://pypi.org/project/stac-fastapi-pgstac-pair-search/)
[![PyPI version](https://img.shields.io/pypi/v/stac-fastapi-pgstac-pair-search.svg)](https://pypi.org/project/stac-fastapi-pgstac-pair-search/)
[![GitHub license](https://img.shields.io/github/license/EOX-A/stac-fastapi-pgstac-pair-search)](https://github.com/EOX-A/stac-fastapi-pgstac-pair-search/blob/main/LICENSE)

An extension for `stac-fastapi` that enables powerful "pair searches" on a `pgstac` backend. This project is built on the powerful [stac-utils/stac-fastapi-pgstac](https://github.com/stac-utils/stac-fastapi-pgstac) framework.

This extension is ideal for applications that need to find related imagery for analysis, such as change detection, interferometry, or comparative studies.

---
## ✨ Features

* **`/pair-search` Endpoint**: A dedicated endpoint for finding pairs of STAC Items.
* **Relational Filtering**: Use a powerful filter syntax to define conditions between the two items in a pair, using `first.` and `second.` prefixes.
* **Standard CQL2 Support**: Leverage the full power of CQL2-JSON and CQL2-TEXT for filtering each set of items before pairing.
* **Custom Functions**: Integrates with custom PostgreSQL functions to enable advanced relational queries (e.g., `T_DIFF` for time difference, `S_RAOVERLAP` for spatial overlap).
* **Flexible Response Formats**: Retrieve results as a list of item pairs, or as a standard FeatureCollection containing all unique items found in the resulting pairs.
* **Asynchronous by Design**: Built on FastAPI and asyncpg for high performance.

---
## ✅ Prerequisites

This project consists of two main components: the FastAPI application and a specialized database.

1.  **The Application**: A Python FastAPI application that can be run as a standalone service.

2.  **A `pgstac` Database with Custom Functions**: A PostgreSQL database with PostGIS and `pgstac` installed. Crucially, this database **must** be extended with the following custom SQL functions:
    * `N_DIFF`
    * `S_RAOVERLAP`
    * `T_DIFF`
    * `T_END`
    * `T_START`

The SQL scripts to create these functions are available in the `sql/` directory of the source code.

---
## 🐳 Running with Docker Compose (Quickstart)

The included `docker-compose.yml` file provides the simplest way to run a complete setup, including the FastAPI application and a `pgstac` database with all the required custom functions pre-installed.

To start the services, run the following command from the root of the repository:

```bash
docker compose up --build
```

---
## 🚀 Running the Application Manually

This repository contains a fully functional FastAPI application that can be run directly.

After installing the Python dependencies, you can start the application with the following command:

```bash
python -m stac_fastapi_pgstac_pair_search.app
```

**Note**: This command starts the application only. It assumes you have a separate, running `pgstac` database (as described in the Prerequisites) and have set the environment variables for the database connection (e.g., `POSTGRES_HOST`, `POSTGRES_USER`, etc.).

---
## 📖 API Documentation

The extension adds a new endpoint: `POST /pair-search`.

The request body is a flattened structure of STAC search parameters prefixed with `first-` and `second-` to define the two item sets, along with parameters to control the pairing logic. All parameters are optional.

### Request Body

| Key | Type | Description |
| :--- | :--- | :--- |
| `first-collections` | `array[string]` | STAC `collections` parameter for the first search set. |
| `second-collections`| `array[string]` | STAC `collections` parameter for the second search set. |
| `first-bbox` | `array[number]` | STAC `bbox` parameter for the first search set. |
| `second-bbox` | `array[number]` | STAC `bbox` parameter for the second search set. |
| `first-datetime` | `string` | STAC `datetime` parameter for the first search set. |
| `second-datetime` | `string` | STAC `datetime` parameter for the second search set. |
| `first-intersects` | `string` | STAC `intersects` parameter (**WKT string**) for the first search set. |
| `second-intersects` | `string` | STAC `intersects` parameter (**WKT string**) for the second search set. |
| `first-ids` | `array[string]` | STAC `ids` parameter for the first search set. |
| `second-ids` | `array[string]` | STAC `ids` parameter for the second search set. |
| `filter` | `string` | A CQL2-based expression to filter pairs. Use `first.<prop>` and `second.<prop>`. |
| `response-type` | `string` | Defines the response format. Must be one of `'pair'`, `'first-only'`, or `'second-only'`. Defaults to `'pair'`. |
| `limit` | `integer`| The maximum number of *pairs* to return. Defaults to 100. |

### The `filter` Expression

The `filter` is the core of this extension. It allows you to express conditions *between* an item from the first search and an item from the second search.

* **Referencing Items**: Use the prefix `first.` to refer to properties of items from the first search set and `second.` for items from the second search set.
* **Property Names**: Properties can be standard STAC properties (`first.datetime`), custom properties (`first.eo:cloud_cover`), or properties with special characters like colons (`first.grid:code`).
* **Discovering Properties**: To see a list of all filterable properties for a collection, you can use the standard STAC API `/queryables` endpoint. For example: `/collections/ENVISAT.ASA.IMS_1P/queryables`.

---
## 🌐 Example Request

This example finds pairs of `ENVISAT.ASA.IMS_1P` items within a specific area and time window, where the time difference between the paired items is less than 35 days.

```bash
curl -X POST "http://localhost:8080/pair-search" \
-H "Content-Type: application/json" \
-d '{
      "first-collections": ["ENVISAT.ASA.IMS_1P"],
      "first-bbox": [13.0, 48.0, 14.5, 49.0],
      "first-datetime": "2010-01-01T00:00:00Z/2010-03-31T23:59:59Z",
      "second-collections": ["ENVISAT.ASA.IMS_1P"],
      "second-bbox": [13.0, 48.0, 14.5, 49.0],
      "limit": 50,
      "filter