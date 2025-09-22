#!/usr/bin/env python3
"""Pipeline to load Salesforce data."""

import dlt

from salesforce import salesforce_source


def load() -> None:
    """Execute a pipeline from Salesforce."""

    pipeline = dlt.pipeline(
        pipeline_name="salesforce", destination="duckdb", dataset_name="salesforce_data"
    )
    load_info = pipeline.run(salesforce_source().with_resources(
        "account",
        "contact",
        "opportunity",
        "opportunity_contact_role",
    ))
    print(load_info)


if __name__ == "__main__":
    load()
