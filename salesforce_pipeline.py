#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""

import os
from functools import cache

import dlt
import pyarrow as pa
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError

from iceberg.schema import create_iceberg_schema_from_table_schema
from salesforce import salesforce_source

CATALOG_URL = os.getenv("ICEBERG_CATALOG_URL", "http://localhost:8181/catalog")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "my-warehouse")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "database")
TOKEN = os.getenv("ICEBERG_TOKEN", "")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

DUMP_TO_DUCKDB = os.getenv("DUMP_TO_DUCKDB", "false") == "true"


@cache
def get_catalog() -> RestCatalog:
    return RestCatalog(
        name="rest_catalog",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE,
        token=TOKEN,
    )


@dlt.destination(batch_size=BATCH_SIZE)
def iceberg_rest_catalog(items: TDataItems, table: TTableSchema) -> None:
    if "name" not in table:
        raise ValueError("Table schema must have a 'name' field")
    table_name = table["name"]
    full_table_name = f"{NAMESPACE}.{table_name}"
    catalog = get_catalog()

    try:
        i_table = catalog.load_table(full_table_name)
    except NoSuchTableError:
        print(f"Table {full_table_name} not found, creating it...")
        iceberg_schema = create_iceberg_schema_from_table_schema(table)
        i_table = catalog.create_table(
            identifier=full_table_name,
            schema=iceberg_schema,
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy",
            },
        )
        print(f"Created table {full_table_name} with schema: {iceberg_schema}")

    print(f"Converting {len(items)} items to PyArrow table")
    pa_table = pa.Table.from_pylist(items)
    i_table.append(pa_table)


def load() -> None:
    """Execute a pipeline from Salesforce."""

    pipeline_name = "salesforce_duckdb" if DUMP_TO_DUCKDB else "salesforce_iceberg"
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb" if DUMP_TO_DUCKDB else iceberg_rest_catalog,
        dataset_name="salesforce_data",
    )
    load_info = pipeline.run(
        salesforce_source().with_resources(
            "account",
            "contact",
            "opportunity",
            "opportunity_contact_role",
        )
    )
    print(load_info)


if __name__ == "__main__":
    load()
