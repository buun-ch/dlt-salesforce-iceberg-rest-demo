# dlt-salesforce-demo

## Installation

```bash
mise trust
```

## Running the Pipeline

```bash
SOURCES__SALESFORCE__CREDENTIALS__USER_NAME="op://Personal/Salesforce Developer Edition/username"
SOURCES__SALESFORCE__CREDENTIALS__PASSWORD="op://Personal/Salesforce Developer Edition/password"
SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN="op://Personal/Salesforce token/credential"
```

```bash
op run --env-file=./env.local
```

### Project Initialization

This project was initialized by:

```bash
uv init
source .venv/bin/activate
uv add dlt
dlt init salesforce dummy
```
