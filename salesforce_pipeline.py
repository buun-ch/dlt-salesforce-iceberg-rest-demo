#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""

import os
import shutil
from functools import cache

import dlt
import pyarrow as pa
from dlt.common.pipeline import LoadInfo
from dlt.common.schema import TTableSchema
from dlt.common.typing import TDataItems
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    Or,
)

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


def get_primary_keys(table: TTableSchema) -> list[str]:
    """Extract primary keys from table schema."""
    primary_keys = []
    if "columns" in table:
        for column_name, column_info in table["columns"].items():
            if column_info.get("primary_key", False):
                primary_keys.append(column_name)
    return primary_keys


@dlt.destination(batch_size=BATCH_SIZE, loader_file_format="parquet")
def iceberg_rest_catalog(items: TDataItems, table: TTableSchema) -> None:
    if "name" not in table:
        raise ValueError("Table schema must have a 'name' field")
    table_name = table["name"]
    full_table_name = f"{NAMESPACE}.{table_name}"

    catalog = get_catalog()
    write_disposition = table.get("write_disposition", "append")

    try:
        i_table = catalog.load_table(full_table_name)
        print(
            f"Table {full_table_name} found, proceeding with {write_disposition} mode"
        )

        # Handle different write dispositions
        if write_disposition == "replace":
            print(f"Replace mode: deleting all data from {full_table_name}")
            i_table.delete(delete_filter=AlwaysTrue())

        elif write_disposition == "merge":
            primary_keys = get_primary_keys(table)
            print(f"Merge mode for {full_table_name}, primary keys: {primary_keys}")
            if primary_keys:
                print(f"Merge mode: updating/inserting based on keys {primary_keys}")

                # Convert to PyArrow Table if needed
                if isinstance(items, pa.RecordBatch):
                    pa_data = pa.Table.from_batches([items])
                elif isinstance(items, pa.Table):
                    pa_data = items
                else:
                    pa_data = pa.Table.from_pylist(items)

                # Check if primary keys exist in data
                if all(pk in pa_data.schema.names for pk in primary_keys):
                    # Build delete filter using PyArrow columns directly
                    conditions: list[BooleanExpression] = []

                    # Extract primary key columns
                    pk_columns = {
                        pk: pa_data.column(pk).to_pylist() for pk in primary_keys
                    }

                    # Create filter conditions for each row
                    for i in range(len(pa_data)):
                        pk_conditions: list[BooleanExpression] = []
                        for pk in primary_keys:
                            pk_value = pk_columns[pk][i]
                            if pk_value is not None:
                                pk_conditions.append(EqualTo(pk, pk_value))

                        if pk_conditions:
                            if len(pk_conditions) == 1:
                                conditions.append(pk_conditions[0])
                            else:
                                conditions.append(And(*pk_conditions))

                    # Delete existing records with matching primary keys
                    if conditions:
                        delete_filter: BooleanExpression
                        if len(conditions) == 1:
                            delete_filter = conditions[0]
                        else:
                            delete_filter = Or(*conditions)

                        print("Deleting existing records with matching primary keys...")
                        i_table.delete(delete_filter=delete_filter)
                else:
                    print(
                        f"Warning: Primary keys {primary_keys} not found in data, falling back to append"
                    )
            else:
                print(
                    "Warning: No primary keys defined for merge, falling back to append"
                )

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
        print(f"Created table {full_table_name}")

    # Get Iceberg table's Arrow schema and align data
    iceberg_arrow_schema = i_table.schema().as_arrow()
    if isinstance(items, (pa.Table, pa.RecordBatch)):
        if isinstance(items, pa.RecordBatch):
            pa_table = pa.Table.from_batches([items])
        else:
            pa_table = items

        for field in iceberg_arrow_schema:
            if field.name not in pa_table.schema.names:
                null_array = pa.array([None] * len(pa_table), type=field.type)
                pa_table = pa_table.append_column(field.name, null_array)

        pa_table = pa_table.select([field.name for field in iceberg_arrow_schema])
    else:
        iceberg_field_names = {field.name for field in iceberg_arrow_schema}
        for item in items:
            for field_name in iceberg_field_names:
                if field_name not in item:
                    item[field_name] = None

        pa_table = pa.Table.from_pylist(items, schema=iceberg_arrow_schema)

    i_table.append(pa_table)


def load() -> LoadInfo:
    """Execute a pipeline from Salesforce."""
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

    # Apply write disposition based on configuration
    if WRITE_DISPOSITION == "force_replace":
        print("Forcing all resources to use 'replace' disposition")
        return pipeline.run(source, write_disposition="replace")
    else:
        print("Using default write dispositions from resource definitions")
        return pipeline.run(source)


if __name__ == "__main__":
    print(load())
