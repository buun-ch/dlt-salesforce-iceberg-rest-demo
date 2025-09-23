#!/usr/bin/env python3
"""Airflow DAG for Salesforce to Iceberg pipeline using TaskFlow API."""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.models import Variable
from pyiceberg.catalog.rest import RestCatalog

dlt_dir = Path(__file__).parent / "dlt_salesforce"
sys.path.insert(0, str(dlt_dir))

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


@dag(
    dag_id="salesforce_iceberg_pipeline",
    description="Load Salesforce data into Iceberg REST Catalog using dlt",
    default_args=default_args,
    catchup=False,
    tags=["salesforce", "iceberg", "dlt"],
)
def salesforce_iceberg_pipeline():
    """DAG for Salesforce to Iceberg data pipeline."""

    @task
    def validate_configuration() -> Dict[str, Any]:
        """Validate required configuration and environment variables."""

        required_vars = [
            "SOURCES__SALESFORCE__CREDENTIALS__USER_NAME",
            "SOURCES__SALESFORCE__CREDENTIALS__PASSWORD",
            "SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN",
            "ICEBERG_CATALOG_URL",
            "ICEBERG_WAREHOUSE",
            "ICEBERG_NAMESPACE",
            "ICEBERG_TOKEN",
        ]
        missing_vars = []
        config = {}
        for var in required_vars:
            value = Variable.get(var, default_var=None)
            if not value:
                # Try environment variable as fallback
                value = os.getenv(var)

            if not value:
                missing_vars.append(var)
            else:
                config[var] = value
        if missing_vars:
            raise ValueError(f"Missing required variables: {missing_vars}")

        optional_vars = {
            "WRITE_DISPOSITION": "force_replace",
            "SALESFORCE_RESOURCES": "account,contact,opportunity,opportunity_contact_role",
            "BATCH_SIZE": "1000",
        }
        for var, default_value in optional_vars.items():
            value = Variable.get(var, default_var=None)
            if not value:
                value = os.getenv(var, default_value)
            config[var] = value

        return config

    @task
    def run_salesforce_pipeline(config) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
        """Execute the Salesforce to Iceberg pipeline."""

        for key, value in config.items():
            os.environ[key] = str(value)

        from salesforce_pipeline import load  # type: ignore[import]

        print(f"Starting Salesforce pipeline with config:\n{config}")
        print(load())

        return {
            "status": "success",
            "message": "Pipeline completed successfully",
            "config": config,
        }

    @task
    def verify_data_load(pipeline_result) -> Dict[str, Any]:  # type: ignore[no-untyped-def]
        """Verify that data was loaded successfully into Iceberg tables."""

        if pipeline_result["status"] != "success":
            raise ValueError(f"Pipeline failed: {pipeline_result['message']}")
        try:
            config = pipeline_result["config"]

            # Connect to catalog
            catalog = RestCatalog(
                name="rest_catalog",
                uri=config["ICEBERG_CATALOG_URL"],
                warehouse=config["ICEBERG_WAREHOUSE"],
                token=config["ICEBERG_TOKEN"],
            )

            namespace = config["ICEBERG_NAMESPACE"]
            resources = config["SALESFORCE_RESOURCES"].split(",")

            verification_results = {}

            for resource in resources:
                resource_name = resource.strip()
                table_id = f"{namespace}.{resource_name}"

                try:
                    table = catalog.load_table(table_id)
                    df = table.scan().to_pandas()
                    record_count = len(df)

                    verification_results[resource_name] = {
                        "status": "success",
                        "record_count": record_count,
                        "table_location": table.location(),
                    }

                    print(f"✅ {resource_name}: {record_count:,} records")

                except Exception as e:
                    verification_results[resource_name] = {
                        "status": "error",
                        "error": str(e),
                    }
                    print(f"❌ {resource_name}: {e}")

            return {
                "status": "success",
                "verification_results": verification_results,
                "total_tables": len(resources),
            }

        except Exception as e:
            return {"status": "error", "message": f"Verification failed: {str(e)}"}

    @task
    def send_notification(verification_result) -> None:  # type: ignore[no-untyped-def]
        """Send notification about pipeline execution results."""

        if verification_result["status"] == "success":
            results = verification_result["verification_results"]
            success_count = sum(1 for r in results.values() if r["status"] == "success")
            total_records = sum(
                r.get("record_count", 0)
                for r in results.values()
                if r["status"] == "success"
            )

            message = (
                f"✅ Salesforce pipeline completed successfully\n"
                f"Tables loaded: {success_count}/{verification_result['total_tables']}\n"
                f"Total records: {total_records:,}\n"
            )

            for table_name, result in results.items():
                if result["status"] == "success":
                    message += f"  - {table_name}: {result['record_count']:,} records\n"
                else:
                    message += f"  - {table_name}: ❌ {result['error']}\n"
        else:
            message = f"❌ Salesforce pipeline failed: {verification_result['message']}"

        print(message)

    config = validate_configuration()
    pipeline_result = run_salesforce_pipeline(config)
    verification_result = verify_data_load(pipeline_result)
    send_notification(verification_result)


salesforce_dag = salesforce_iceberg_pipeline()
