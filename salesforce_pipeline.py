#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""

import logging
import os
import shutil
import sys
from functools import cache
from typing import Sequence

import dlt
import pyarrow as pa
from dlt.common.pipeline import LoadInfo
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
WRITE_DISPOSITION = os.getenv(
    "WRITE_DISPOSITION", "force_replace"
)  # default or force_replace
SALESFORCE_RESOURCES = os.getenv(
    "SALESFORCE_RESOURCES", "account,contact,opportunity,opportunity_contact_role"
).split(",")

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
    write_disposition = table.get("write_disposition", "append")

    try:
        i_table = catalog.load_table(full_table_name)

        # If replace mode, delete all existing data first
        if write_disposition == "replace":
            print(f"Replace mode: deleting all data from {full_table_name}")
            from pyiceberg.expressions import AlwaysTrue
            i_table.delete(delete_filter=AlwaysTrue())

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

    print(f"Converting {len(items)} items to PyArrow table ({write_disposition} mode)")
    pa_table = pa.Table.from_pylist(items)
    i_table.append(pa_table)


def apply_write_disposition(
    source, resources: Sequence[str], write_disposition: str
) -> None:
    """Apply write disposition to resources based on configuration."""
    if write_disposition == "force_replace":
        print("Forcing all resources to use 'replace' disposition")
        for resource_name in resources:
            resource = getattr(source, resource_name, None)
            if resource and hasattr(resource, "write_disposition"):
                original = getattr(resource, "write_disposition", "unknown")
                resource.write_disposition = "replace"
                print(f"  - {resource_name}: {original} → replace")
    else:
        # Use default dispositions from salesforce/__init__.py
        print("Using default write dispositions:")
        for resource_name in resources:
            resource = getattr(source, resource_name, None)
            if resource and hasattr(resource, "write_disposition"):
                disposition = getattr(resource, "write_disposition", "unknown")
                print(f"  - {resource_name}: {disposition}")


def load() -> LoadInfo:
    """Execute a pipeline from Salesforce."""
    # In Airflow, warnings are sent to stderr which gets logged as ERROR
    # Redirect dlt warnings to stdout instead
    if "airflow" in sys.modules:
        dlt_logger = logging.getLogger("dlt")
        # Create a handler that outputs to stdout instead of stderr
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
        dlt_logger.handlers = [stdout_handler]

    pipeline_name = "salesforce_duckdb" if DUMP_TO_DUCKDB else "salesforce_iceberg"

    # Clear pipeline state for force_replace mode
    if WRITE_DISPOSITION == "force_replace":
        pipeline_dir = os.path.expanduser(f"~/.dlt/pipelines/{pipeline_name}")
        if os.path.exists(pipeline_dir):
            print(f"Clearing pipeline state at {pipeline_dir} for force_replace mode")
            shutil.rmtree(pipeline_dir)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb" if DUMP_TO_DUCKDB else iceberg_rest_catalog,
        dataset_name="salesforce_data",
    )

    resources = [r.strip() for r in SALESFORCE_RESOURCES if r.strip()]
    print(f"Loading Salesforce resources: {', '.join(resources)}")
    source = salesforce_source().with_resources(*resources)
    apply_write_disposition(source, resources, WRITE_DISPOSITION)

    return pipeline.run(source)


if __name__ == "__main__":
    print(load())
