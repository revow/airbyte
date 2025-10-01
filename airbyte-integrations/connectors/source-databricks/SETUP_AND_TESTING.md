# Databricks Source Connector - Setup and Testing Guide

This guide will walk you through setting up and testing the Databricks source connector locally.

## Prerequisites

1. **Python 3.9+** installed on your system
2. **Poetry** for dependency management
3. **Databricks workspace** with SQL endpoint access
4. **Personal Access Token (PAT)** from your Databricks workspace
5. **SQL Warehouse ID** from your Databricks workspace

## Installation

1. **Clone or navigate to the connector directory:**
   ```bash
   cd source-databricks
   ```

2. **Install dependencies using Poetry:**
   ```bash
   poetry install
   ```

3. **Activate the virtual environment:**
   ```bash
   poetry shell
   ```

## Configuration

1. **Create a configuration file** (`config.json`):
   ```json
   {
     "workspace_url": "https://your-workspace.cloud.databricks.com",
     "personal_access_token": "your_pat_token_here",
     "catalog": "hive_metastore",
     "schema": "default",
     "table": "your_table_name",
     "cursor_field": "updated_at",
     "warehouse_id": "your_warehouse_id_here",
     "batch_size": 10000
   }
   ```

2. **Get your Databricks credentials:**
   - **Workspace URL**: Found in your browser when accessing Databricks
   - **Personal Access Token**: Generate in User Settings > Access Tokens
   - **Warehouse ID**: Found in SQL Warehouses section (copy from URL)

## Testing the Connector

### 1. Test Connection

```bash
poetry run source-databricks check --config config.json
```

### 2. Discover Streams

```bash
poetry run source-databricks discover --config config.json
```

### 3. Read Data

```bash
poetry run source-databricks read --config config.json --catalog catalog.json
```

### 4. Run Tests

```bash
# Unit tests
poetry run pytest unit_tests/

# Integration tests
poetry run pytest integration_tests/
```

## Using with Airbyte

### Local Airbyte Instance

1. **Build the connector image:**
   ```bash
   docker build -t airbyte/source-databricks:dev .
   ```

2. **Add to your local Airbyte instance:**
   - Copy the connector files to your Airbyte connectors directory
   - Restart Airbyte
   - Add the source connector in the UI

### Airbyte Cloud

1. **Package the connector:**
   ```bash
   poetry build
   ```

2. **Upload to Airbyte Cloud:**
   - Use the Airbyte CLI or web interface
   - Follow the custom connector setup process

## Troubleshooting

### Common Issues

1. **Connection Failed:**
   - Verify your workspace URL is correct
   - Ensure your PAT has the necessary permissions
   - Check if your SQL warehouse is running

2. **Authentication Errors:**
   - Verify your PAT is valid and not expired
   - Ensure your PAT has SQL endpoint access

3. **Schema Discovery Issues:**
   - Check if the table exists in the specified catalog/schema
   - Verify your user has access to the table

4. **Import Errors:**
   - Ensure all dependencies are installed: `poetry install`
   - Check Python version compatibility

### Debug Mode

Enable debug logging by setting the environment variable:
```bash
export AIRBYTE_DEBUG=1
```

## Example Usage

### Basic Configuration
```json
{
  "workspace_url": "https://mycompany.cloud.databricks.com",
  "personal_access_token": "dapi1234567890abcdef",
  "catalog": "hive_metastore",
  "schema": "analytics",
  "table": "user_events",
  "cursor_field": "event_timestamp"
}
```

### Full Refresh Sync
For full refresh sync, omit the `cursor_field`:
```json
{
  "workspace_url": "https://mycompany.cloud.databricks.com",
  "personal_access_token": "dapi1234567890abcdef",
  "catalog": "hive_metastore",
  "schema": "analytics",
  "table": "user_events"
}
```

## Performance Considerations

1. **Batch Size**: Adjust `batch_size` based on your data volume and memory constraints
2. **Cursor Field**: Choose an efficient cursor field (indexed timestamp columns work best)
3. **Warehouse Size**: Use appropriately sized SQL warehouses for your data volume
4. **Network Latency**: Consider the location of your Airbyte instance relative to Databricks

## Security Best Practices

1. **Token Management**: Rotate your PAT regularly
2. **Access Control**: Use the minimum required permissions for your PAT
3. **Network Security**: Ensure secure connections between Airbyte and Databricks
4. **Data Encryption**: Verify that data is encrypted in transit

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the Airbyte documentation
3. Check Databricks documentation for SQL endpoint issues
4. Open an issue in the connector repository

## Next Steps

After successful testing:
1. Configure your production Databricks workspace
2. Set up monitoring and alerting
3. Configure incremental sync schedules
4. Set up data validation and quality checks
