# Airflow Integration

This directory contains Apache Airflow DAGs for running the Salesforce to Iceberg pipeline in a scheduled, production environment.

## DAG Overview

### `salesforce_iceberg_dag.py`

A TaskFlow API-based DAG that:

1. **Validates Configuration** - Checks required Airflow Variables and environment variables
2. **Runs Pipeline** - Executes the Salesforce to Iceberg data pipeline
3. **Verifies Data Load** - Confirms data was loaded successfully into Iceberg tables
4. **Sends Notifications** - Reports pipeline status (extensible for Slack, email, etc.)

**Schedule**: Every 6 hours (configurable)
**Retries**: 1 retry with 5-minute delay
**Tags**: `salesforce`, `iceberg`, `dlt`

## Configuration

### Required Airflow Variables

Set these in the Airflow UI (`Admin > Variables`) or via CLI:

```bash
# Salesforce credentials
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__USER_NAME "your-salesforce-username"
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__PASSWORD "your-salesforce-password"
airflow variables set SOURCES__SALESFORCE__CREDENTIALS__SECURITY_TOKEN "your-security-token"

# Iceberg REST Catalog
airflow variables set ICEBERG_CATALOG_URL "http://lakekeeper.lakekeeper:8181/catalog"
airflow variables set ICEBERG_WAREHOUSE "demo1"
airflow variables set ICEBERG_NAMESPACE "salesforce"
airflow variables set ICEBERG_TOKEN "your-iceberg-token"
```

### Optional Variables (with defaults)

```bash
airflow variables set BATCH_SIZE "1000"
airflow variables set WRITE_DISPOSITION "default"  # or "force_replace"
airflow variables set SALESFORCE_RESOURCES "account,contact,opportunity,opportunity_contact_role"
airflow variables set DUMP_TO_DUCKDB "false"
```

## Installation

### 1. Prepare for Kubernetes Deployment

```bash
# Prepare dags directory for Kubernetes deployment
cd airflow
just prepare

# Check preparation status
just status

# Clean up when done
just clean
```

### 2. Copy to Local Airflow (Optional)

```bash
# For local development/testing
cd airflow
just copy
```

### 3. Install Dependencies

Ensure your Airflow environment has the required packages:

```bash
# In your Airflow environment
pip install dlt pyiceberg pyarrow simple-salesforce
```

### 3. Configure Variables

Use the Airflow UI or CLI to set the required variables listed above.

## Usage

### Manual Execution

```bash
# Trigger the DAG manually
airflow dags trigger salesforce_iceberg_pipeline

# Check DAG status
airflow dags state salesforce_iceberg_pipeline

# View task logs
airflow tasks logs salesforce_iceberg_pipeline validate_configuration <execution_date>
```

### Schedule Modification

To change the schedule, edit the `schedule_interval` parameter in the DAG:

```python
@dag(
    schedule_interval=timedelta(hours=12),  # Every 12 hours
    # or
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    # or
    schedule_interval=None,  # Manual only
)
```

## Monitoring

### Task Structure

1. **validate_configuration** - Ensures all required variables are set
2. **run_salesforce_pipeline** - Executes the main pipeline logic
3. **verify_data_load** - Checks that data was loaded into Iceberg tables
4. **send_notification** - Logs results (extend for external notifications)

### Logs and Alerts

- Check Airflow UI for task logs and execution history
- Pipeline status and record counts are logged in the notification task
- Extend `send_notification` task for Slack, email, or other alerting

### Data Quality Checks

The verification task checks:

- ✅ Table existence in Iceberg catalog
- ✅ Record counts for each Salesforce resource
- ✅ Table locations and accessibility

## Customization

### Adding Notifications

Extend the `send_notification` task:

```python
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

@task
def send_slack_notification(verification_result: Dict[str, Any]) -> None:
    # Your notification logic here
    pass
```

### Custom Data Quality Checks

Add more verification in `verify_data_load`:

```python
# Check for data freshness
latest_modified = df['SystemModstamp'].max()
if latest_modified < datetime.now() - timedelta(days=1):
    raise ValueError("Data appears stale")
```

### Error Handling

The DAG includes basic error handling and retries. For production:

1. Configure appropriate retry counts and delays
2. Set up proper alerting for failures
3. Consider implementing circuit breaker patterns for external dependencies

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure the project root is in Python path
2. **Missing Variables**: Check Airflow Variables are set correctly
3. **Connection Failures**: Verify network access to Salesforce and Iceberg catalog
4. **Permission Issues**: Ensure Airflow worker has necessary permissions

### Debug Mode

Enable debug logging by setting Airflow logging level or adding print statements to tasks.

