# dlt Salesforce to Iceberg REST Catalog Demo

A sample project demonstrating how to ingest data from Salesforce into an Iceberg REST Catalog using dlt (data load tool).

## Overview

This project showcases:

- **Salesforce data extraction** using dlt's built-in Salesforce source
- **Custom Iceberg destination** implementation for Iceberg REST Catalog
- **Automatic table creation** with schema conversion from dlt to Iceberg
- **PyArrow integration** for efficient data processing

## Background

The open-source version of dlt does not currently support Iceberg REST Catalog as a destination out of the box. This project implements a **Custom Destination** to bridge that gap, allowing you to:

1. Extract data from Salesforce using dlt's verified source
2. Automatically convert dlt table schemas to Iceberg schemas
3. Create Iceberg tables dynamically if they don't exist
4. Load data efficiently using PyArrow

## Features

- ✅ **Salesforce Integration**: Multiple authentication methods supported
- ✅ **Schema Conversion**: Automatic dlt → Iceberg schema mapping with timezone-aware timestamps
- ✅ **Table Auto-creation**: Creates Iceberg tables on-demand with proper schema
- ✅ **Write Disposition Support**: Replace, merge (upsert), and append modes
- ✅ **Parquet Format**: Efficient columnar format for Iceberg storage
- ✅ **Batch Processing**: Configurable batch sizes for optimal performance
- ✅ **Environment Variable Support**: Easy configuration via `.env` files
- ✅ **Fallback to DuckDB**: Can switch between Iceberg and DuckDB for testing
- ✅ **Orchestration**: Support for both Dagster and Airflow

## Architecture

```plain
Salesforce API → dlt Source → Custom Iceberg Destination → Iceberg REST Catalog → MinIO/S3
```

The pipeline:

1. **Extracts** data from Salesforce using dlt's source
2. **Transforms** data and infers schema automatically
3. **Converts** dlt schema to Iceberg schema using PyArrow as intermediate format
4. **Loads** data into Iceberg tables via REST Catalog

## Prerequisites

- Python 3.12+
- uv (package manager)
- Access to Salesforce with API credentials
- Iceberg REST Catalog (e.g., Lakekeeper)
- S3-compatible storage (e.g., MinIO)

## Installation

Setup the environment:

```bash
mise trust
uv venv
uv sync
source .venv/bin/activate
```

## Configuration

### Environment Variables

Create a `.env.local` file with the following variables. All configuration options are customizable via environment variables. If you are using 1Password, you can use the following format to reference your secrets:

```bash
# Salesforce Credentials (SecurityTokenAuth)
SOURCES__SALESFORCE__CREDENTIALS__USER_NAME="op://Personal/Salesforce/username"
SOURCES__SALESFORCE__CREDENTIALS__PASSWORD="op://Personal/Salesforce/password"
SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN="op://Personal/SalesforceToken/credential"

# Iceberg REST Catalog Configuration
ICEBERG_CATALOG_URL="http://lakekeeper.lakekeeper:8181/catalog"
ICEBERG_WAREHOUSE="demo1"
ICEBERG_NAMESPACE="salesforce"
ICEBERG_TOKEN="op://Personal/lakekeeper token/credential"

# Optional: Performance Tuning
BATCH_SIZE=1000

# Optional: Data Loading Configuration
WRITE_DISPOSITION=default  # default or force_replace
SALESFORCE_RESOURCES=account,contact,opportunity,opportunity_contact_role

# Optional: Fallback to DuckDB for testing
DUMP_TO_DUCKDB=false
```

### Salesforce Authentication

The project supports multiple Salesforce authentication methods. Use the environment variable dumper to see all available options:

```bash
uv run dump_env_vars.py salesforce
```

## Usage

### Run the Pipeline

```bash
# Using 1Password to retrieve secrets
op run --env-file=./.env.local -- python salesforce_pipeline.py

# Or source environment variables directly
source .env.local && python salesforce_pipeline.py
```

### Check Created Tables

```bash
# View tables and data in Iceberg catalog
op run --env-file=./.env.local -- python check_tables.py
```

### Test with DuckDB

```bash
# For local testing without Iceberg
export DUMP_TO_DUCKDB=true
op run --env-file=./.env.local -- python salesforce_pipeline.py
```

### Run with Dagster

```bash
# Prepare dependencies (required once)
cd dagster
just prepare

# Start Dagster UI
dagster dev

# Access UI at http://localhost:3000
# Materialize assets: salesforce/account, salesforce/contact, etc.
```

### Run with Airflow

```bash
# Prepare dependencies (required once)
cd airflow
just prepare

# Copy DAG to your Airflow DAGs folder (if using external Airflow)
cp -r dags/* $AIRFLOW_HOME/dags/

# Set Airflow Variables (via UI or CLI)
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__USER_NAME "your_username"
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__PASSWORD "your_password"
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN "your_token"
airflow variables set ICEBERG_CATALOG_URL "http://lakekeeper:8181/catalog"
airflow variables set ICEBERG_WAREHOUSE "demo1"
airflow variables set ICEBERG_NAMESPACE "salesforce"
airflow variables set ICEBERG_TOKEN "your_token"

# Optional: Configure write disposition and resources
airflow variables set WRITE_DISPOSITION "default"
airflow variables set SALESFORCE_RESOURCES "account,contact,opportunity,opportunity_contact_role"

# Trigger the DAG
airflow dags trigger salesforce_iceberg_pipeline
```

## Salesforce Sample Data

To populate your Salesforce org with realistic test data for this demo, see [data/README.md](data/README.md). This includes Snowfakery recipes for generating:

- Small-scale (50 accounts) for basic testing
- Medium-scale (200 accounts) - Recommended for demos
- Large-scale (1,000 accounts) for performance testing

## Project Structure

```plain
├── salesforce/                    # dlt-generated Salesforce source
│   ├── __init__.py                # Source configuration with resources
│   └── helpers/
│       ├── client.py              # Authentication classes
│       └── records.py             # Data extraction logic
├── iceberg/
│   └── schema.py                  # dlt → Iceberg schema conversion
├── dagster/                       # Dagster orchestration
│   └── dlt_salesforce/
│       ├── definitions.py         # Dagster definitions
│       └── assets.py              # Salesforce data assets
├── airflow/                       # Airflow orchestration
│   └── dags/
│       ├── salesforce_iceberg_dag.py  # DAG definition with TaskFlow API
│       └── dlt_salesforce/        # Shared pipeline code
├── salesforce_pipeline.py         # Main pipeline with custom destination
├── dump_env_vars.py               # Environment variable reference tool
├── check_tables.py                # Table inspection utility
└── README.md                      # This file
```

## Key Components

### Custom Iceberg Destination

The core innovation is the `iceberg_rest_catalog` custom destination function:

```python
@dlt.destination(batch_size=BATCH_SIZE, loader_file_format="parquet")
def iceberg_rest_catalog(items: TDataItems, table: TTableSchema) -> None:
    # Connect to REST catalog
    # Handle write dispositions (replace, merge, append)
    # Convert schema if table doesn't exist
    # Load data via PyArrow with Parquet format
```

### Write Dispositions

The destination supports three write dispositions:

- **`replace`**: Deletes all existing data before inserting new records
- **`merge`**: Upserts based on primary keys (deletes matching records, then inserts)
- **`append`**: Adds new records without deleting existing ones

Merge operations use PyArrow for efficient primary key extraction and PyIceberg expressions for filtering.

### Schema Conversion

The project uses dlt's built-in `columns_to_arrow()` function for reliable schema conversion:

```python
# Convert dlt schema → PyArrow → Iceberg
pa_schema = columns_to_arrow(columns, caps)
iceberg_schema = create_iceberg_schema_from_table_schema(table_schema)
```

## Orchestration

### Dagster

The Dagster integration provides a declarative approach with asset-based orchestration:

- **Multi-asset**: Core Salesforce resources as individual assets
- **Asset dependencies**: Summary asset depends on core assets
- **Configuration**: Via `SalesforceConfig` class
- **Metadata**: Tracks rows loaded, write disposition, destination type

**Setup**: Run `just prepare` in the `dagster/` directory to copy pipeline dependencies.

### Airflow

The Airflow DAG uses TaskFlow API with the following tasks:

1. **validate_configuration**: Validates required Airflow Variables
2. **run_salesforce_pipeline**: Executes the dlt pipeline
3. **verify_data_load**: Verifies data was loaded to Iceberg tables
4. **send_notification**: Sends execution summary

**Configuration**: Uses Airflow Variables for credentials and settings

**Setup**: Run `just prepare` in the `airflow/` directory to copy pipeline dependencies.

## Limitations & Considerations

### PyIceberg Considerations

This implementation is subject to PyIceberg's characteristics:

- **Performance**: Single-threaded processing may be slower than Spark for very large datasets
- **Concurrent Writes**: PyIceberg lacks automatic retry mechanisms for transaction conflicts, making concurrent writes more likely to fail compared to Spark
- **Ecosystem**: Fewer integrations compared to Spark, but rapidly improving

### Recommended Usage Patterns

- **Single Writer**: Ensure only one process writes to each table at a time
- **Batch Processing**: Use appropriate batch sizes (1000-10000 records)
- **Incremental Updates**: Use dlt's incremental loading for large tables
- **Monitoring**: Monitor table metadata and file sizes
- **Orchestration**: Use Airflow or Dagster for scheduling and monitoring

## Configuration Options

### Write Disposition

Control how data is loaded into tables using the `WRITE_DISPOSITION` environment variable:

- **`default`** - Uses the write disposition defined in `salesforce/__init__.py` for each resource:
    - `account`, `opportunity`, `opportunity_contact_role`: `merge` (upsert based on Id)
    - `contact`: `replace` (full table replacement)
- **`force_replace`** - Forces all resources to use `replace` disposition

```bash
# Use default dispositions (recommended for incremental loading)
WRITE_DISPOSITION=default

# Force all tables to be completely replaced
WRITE_DISPOSITION=force_replace
```

**Note**: `force_replace` mode automatically clears the dlt pipeline state to ensure clean table recreation.

### Salesforce Resources

Customize which Salesforce objects to load using the `SALESFORCE_RESOURCES` environment variable:

```bash
# Default configuration
SALESFORCE_RESOURCES=account,contact,opportunity,opportunity_contact_role

# Load only accounts and contacts
SALESFORCE_RESOURCES=account,contact

# Add leads and campaigns
SALESFORCE_RESOURCES=account,contact,lead,campaign,opportunity
```

## Supported Salesforce Objects

Default configuration loads:

- `account` - Customer accounts
- `contact` - Contact records
- `opportunity` - Sales opportunities
- `opportunity_contact_role` - Opportunity-contact relationships

Other commonly available objects include: `lead`, `campaign`, `case`, `task`, `event`, `user`, etc.

## Troubleshooting

### Common Issues

1. **S3/MinIO Authentication Errors**: Ensure proper credentials and endpoint configuration
2. **Schema Mismatch**: Delete `.dlt/` directory to reset pipeline state
3. **Token Expiration**: Refresh Iceberg REST catalog token
4. **PyArrow Type Errors**: Check timestamp timezone handling in schema conversion

### Debug Mode

Enable DuckDB mode for local debugging:

```bash
export DUMP_TO_DUCKDB=true
```

## References

- [dlt Documentation](https://dlthub.com/docs/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest/)
- [Salesforce API Documentation](https://developer.salesforce.com/docs/apis)
