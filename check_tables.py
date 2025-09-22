#!/usr/bin/env python3
"""Check Iceberg tables created by the pipeline."""

import os

from pyiceberg.catalog.rest import RestCatalog

CATALOG_URL = os.getenv("ICEBERG_CATALOG_URL", "http://localhost:8181/catalog")
WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "my-warehouse")
NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "database")
TOKEN = os.getenv("ICEBERG_TOKEN", "")


def main():
    # Connect to catalog
    catalog = RestCatalog(
        name="rest_catalog",
        uri=CATALOG_URL,
        warehouse=WAREHOUSE,
        token=TOKEN,
    )

    print(f"Connected to catalog: {CATALOG_URL}")
    print(f"Namespace: {NAMESPACE}")
    print("=" * 60)

    try:
        # List all tables in the namespace
        tables = catalog.list_tables(NAMESPACE)
        print(f"Tables in namespace '{NAMESPACE}':")
        for table_id in tables:
            print(f"  📊 {table_id}")

        print()

        # Show details for each table
        for table_id in tables:
            try:
                table = catalog.load_table(table_id)
                print(f"Table: {table_id}")
                print(f"  Location: {table.location()}")
                print(f"  Schema: {len(table.schema().fields)} fields")

                # Try to get record count
                try:
                    df = table.scan().to_pandas()
                    print(f"  Records: {len(df):,}")

                    # Show first few records
                    if len(df) > 0:
                        print("  Sample data (first 3 rows):")
                        for col in df.columns[:5]:  # Show first 5 columns
                            print(f"    {col}: {df[col].head(3).tolist()}")

                except Exception as e:
                    print(f"  Records: Unable to scan ({e})")

                print()

            except Exception as e:
                print(f"  ❌ Error loading table {table_id}: {e}")
                print()

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()

