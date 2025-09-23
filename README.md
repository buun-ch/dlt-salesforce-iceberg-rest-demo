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
- ✅ **Batch Processing**: Configurable batch sizes for optimal performance
- ✅ **Environment Variable Support**: Easy configuration via `.env` files
- ✅ **Fallback to DuckDB**: Can switch between Iceberg and DuckDB for testing

## Architecture

```
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
```

## Configuration

### Environment Variables

Create a `.env.local` file with the following variables. All configuration options are customizable via environment variables. If you are using 1Password, you can use the following format to reference your secrets:

```bash
# Salesforce Credentials (SecurityTokenAuth)
SOURCES__SALESFORCE__CREDENTIALS__USER_NAME="op://Personal/Salesforce Developer Edition/username"
SOURCES__SALESFORCE__CREDENTIALS__PASSWORD="op://Personal/Salesforce Developer Edition/password"
SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN="op://Personal/Salesforce token/credential"

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

## Project Structure

```
├── salesforce/                    # dlt-generated Salesforce source
│   ├── __init__.py                # Source configuration
│   └── helpers/
│       ├── client.py              # Authentication classes
│       └── records.py             # Data extraction logic
├── iceberg/
│   └── schema.py                  # dlt → Iceberg schema conversion
├── salesforce_pipeline.py         # Main pipeline with custom destination
├── dump_env_vars.py               # Environment variable reference tool
├── check_tables.py                # Table inspection utility
└── README.md                      # This file
```

## Key Components

### Custom Iceberg Destination

The core innovation is the `iceberg_rest_catalog` custom destination function:

```python
@dlt.destination(batch_size=BATCH_SIZE)
def iceberg_rest_catalog(items: TDataItems, table: TTableSchema) -> None:
    # Connect to REST catalog
    # Convert schema if table doesn't exist
    # Load data via PyArrow
```

### Schema Conversion

The project uses dlt's built-in `columns_to_arrow()` function for reliable schema conversion:

```python
# Convert dlt schema → PyArrow → Iceberg
pa_schema = columns_to_arrow(columns, caps)
iceberg_schema = create_iceberg_schema_from_table_schema(table_schema)
```

## Limitations & Considerations

### PyIceberg Constraints

This implementation is subject to PyIceberg's limitations:

- **Concurrent Writes**: PyIceberg doesn't support concurrent writes to the same table
- **Transaction Isolation**: Limited ACID transaction support compared to Spark
- **Performance**: Single-threaded writes may be slower for large datasets
- **Schema Evolution**: Limited schema evolution capabilities

### Recommended Usage Patterns

- **Single Writer**: Ensure only one process writes to each table at a time
- **Batch Processing**: Use appropriate batch sizes (1000-10000 records)
- **Incremental Updates**: Use dlt's incremental loading for large tables
- **Monitoring**: Monitor table metadata and file sizes

## Configuration Options

### Write Disposition

Control how data is loaded into tables using the `WRITE_DISPOSITION` environment variable:

- **`default`** - Uses the default write disposition defined in `salesforce/__init__.py` for each resource
- **`force_replace`** - Forces all resources to use 'replace' disposition, completely replacing table contents

```bash
# Use default dispositions (recommended for incremental loading)
WRITE_DISPOSITION=default

# Force all tables to be completely replaced
WRITE_DISPOSITION=force_replace
```

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

## Project Initialization

This project was initialized by:

```bash
uv init
source .venv/bin/activate
uv add dlt
dlt init salesforce dummy
```

## Contributing

This is a demonstration project. For production use:

1. Add comprehensive error handling
2. Implement retry logic for transient failures
3. Add schema evolution support
4. Consider using Spark for large-scale processing
5. Add monitoring and alerting

## References

- [dlt Documentation](https://dlthub.com/docs/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://iceberg.apache.org/docs/latest/rest/)
- [Salesforce API Documentation](https://developer.salesforce.com/docs/apis)
