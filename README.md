# dlt-salesforce-demo

## Installation

Setup the environment:

```bash
mise trust
uv venv
uv sync
```

## Running the Pipeline

Copy `env.local.example` to `.env.local` and fill in the credentials and the configurations. If you are using 1Password, you can use the following format to reference your secrets:

```bash
SOURCES__SALESFORCE__CREDENTIALS__USER_NAME="op://Personal/Salesforce Developer Edition/username"
SOURCES__SALESFORCE__CREDENTIALS__PASSWORD="op://Personal/Salesforce Developer Edition/password"
SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN="op://Personal/Salesforce token/credential"

ICEBERG_CATALOG_URL="http://lakekeeper.lakekeeper:8181/catalog"
ICEBERG_ICEBERG_WAREHOUSE="demo1"
ICEBERG_NAMESPACE="salesforce"
ICEBERG_TOKEN="op://Personal/lakekeeper token/credential"

S3_ENDPOINT="http://minio.minio.svc.cluster.local:9000"
AWS_ACCESS_KEY_ID="op://Personal/minio access key/username"
AWS_SECRET_ACCESS_KEY="op://Personal/minio access key/password"
```

Run the pipeline with retrieving credentials from 1Password:

```bash
op run --env-file=./env.local -- python salesforce_pipeline.py
```

If you are not using 1Password, you can simply source the `.env.local` and run the pipeline:

```bash
source .env.local && python salesforce_pipeline.py
```

### Project Initialization

This project was initialized by:

```bash
uv init
source .venv/bin/activate
uv add dlt
dlt init salesforce dummy
```
