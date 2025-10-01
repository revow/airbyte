# Databricks Source Connector - Project Summary

## Overview

This is a complete Airbyte source connector for extracting data from Databricks using the Python CDK. The connector supports both full refresh and incremental sync modes, allowing users to efficiently extract data from Databricks tables.

## Architecture

### Core Components

1. **Source Class** (`source.py`)
   - Main connector class that implements `AbstractSource`
   - Handles connection testing and stream creation
   - Entry point for Airbyte integration

2. **Client Class** (`client.py`)
   - Manages Databricks SQL endpoint connections
   - Handles authentication via Personal Access Token
   - Executes SQL queries and retrieves data
   - Supports cursor-based filtering for incremental sync

3. **Stream Class** (`streams.py`)
   - Implements the data extraction logic
   - Supports both full refresh and incremental sync modes
   - Handles schema discovery and data type mapping
   - Manages stream state for incremental operations

4. **Configuration** (`spec.json`)
   - Defines the connector's configuration schema
   - Includes validation rules and field descriptions
   - Supports workspace URL, authentication, and table selection

## Features

### ✅ Implemented Features

- **Authentication**: Personal Access Token (PAT) support
- **Multi-Cloud Support**: AWS, Azure, and GCP Databricks workspaces
- **Flexible Configuration**: Catalog, schema, and table selection
- **Sync Modes**: Full refresh and incremental sync
- **Cursor Support**: Configurable cursor fields for incremental sync
- **Schema Discovery**: Automatic table schema detection
- **Error Handling**: Comprehensive error handling and logging
- **Testing**: Unit tests and integration test configurations

### 🔧 Configuration Options

- `workspace_url`: Databricks workspace URL
- `personal_access_token`: Authentication token
- `catalog`: Target catalog name
- `schema`: Target schema name
- `table`: Target table name
- `cursor_field`: Field for incremental sync (optional)
- `warehouse_id`: SQL warehouse ID
- `batch_size`: Data processing batch size

## Project Structure

```
source-databricks/
├── source_databricks/           # Main source code
│   ├── __init__.py             # Package initialization
│   ├── source.py               # Main source class
│   ├── client.py               # Databricks client wrapper
│   ├── streams.py              # Stream implementations
│   ├── run.py                  # Entry point
│   └── spec.json               # Configuration schema
├── integration_tests/           # Integration test configs
│   ├── __init__.py
│   ├── sample_config.json      # Sample configuration
│   ├── invalid_config.json     # Invalid config for testing
│   ├── configured_catalog.json # Test catalog
│   └── acceptance.py           # Acceptance tests
├── unit_tests/                  # Unit tests
│   ├── __init__.py
│   └── test_source.py          # Source class tests
├── pyproject.toml              # Dependencies and project config
├── metadata.yaml               # Connector metadata
├── Dockerfile                  # Container configuration
├── README.md                   # Project documentation
├── SETUP_AND_TESTING.md        # Setup and testing guide
├── example_config.json         # Example configuration
├── test_connector.py           # Local testing script
└── acceptance-test-config.yml  # Acceptance test configuration
```

## Implementation Details

### Connection Management

The connector uses the official `databricks-sql-connector` Python package to establish connections to Databricks SQL endpoints. Connections are managed through a client wrapper that handles:

- Authentication via Personal Access Token
- Connection pooling and lifecycle management
- Error handling and retry logic
- Query execution and result processing

### Data Extraction

Data extraction is implemented through a stream-based approach:

1. **Full Refresh Mode**: Extracts all data from the specified table
2. **Incremental Mode**: Uses cursor fields to extract only new/updated records
3. **Batch Processing**: Configurable batch sizes for memory-efficient processing
4. **Schema Mapping**: Automatic mapping of Databricks data types to JSON schema types

### State Management

For incremental sync, the connector maintains stream state that includes:

- Current cursor value for the specified field
- Timestamp of last successful sync
- Error state and retry information

## Testing Strategy

### Unit Tests

- Source class functionality testing
- Mock-based client testing
- Configuration validation testing
- Error handling testing

### Integration Tests

- Connection testing with sample configurations
- Stream discovery testing
- Data extraction testing
- Schema validation testing

### Acceptance Tests

- End-to-end connector testing
- Configuration validation
- Error scenario testing

## Usage Examples

### Basic Usage

```python
from source_databricks.source import SourceDatabricks

# Create source instance
source = SourceDatabricks()

# Test connection
success, error = source.check_connection(logger, config)

# Get streams
streams = source.streams(config)
```

### Configuration Example

```json
{
  "workspace_url": "https://mycompany.cloud.databricks.com",
  "personal_access_token": "dapi1234567890abcdef",
  "catalog": "hive_metastore",
  "schema": "analytics",
  "table": "user_events",
  "cursor_field": "event_timestamp",
  "warehouse_id": "1234567890abcdef",
  "batch_size": 10000
}
```

## Deployment

### Local Development

1. Install dependencies: `poetry install`
2. Activate environment: `poetry shell`
3. Test connector: `python test_connector.py`

### Docker Deployment

1. Build image: `docker build -t airbyte/source-databricks:dev .`
2. Run container: `docker run -it airbyte/source-databricks:dev spec`

### Airbyte Integration

1. Copy connector to Airbyte connectors directory
2. Restart Airbyte instance
3. Add source connector in UI
4. Configure with your Databricks credentials

## Performance Considerations

- **Batch Size**: Adjust based on data volume and memory constraints
- **Cursor Field Selection**: Choose indexed timestamp columns for efficiency
- **Warehouse Sizing**: Use appropriately sized SQL warehouses
- **Network Optimization**: Consider geographic proximity of Airbyte and Databricks

## Security Features

- **Token-based Authentication**: Secure Personal Access Token usage
- **Encrypted Connections**: HTTPS/TLS encryption for data in transit
- **Minimal Permissions**: Configurable access control via PAT permissions
- **Secure Configuration**: Environment variable support for sensitive data

## Future Enhancements

### Potential Improvements

1. **Multi-Table Support**: Extract from multiple tables in a single sync
2. **Advanced Filtering**: Support for WHERE clause conditions
3. **Parallel Processing**: Multi-threaded data extraction
4. **Data Validation**: Built-in data quality checks
5. **Monitoring**: Enhanced logging and metrics collection
6. **Caching**: Schema and metadata caching for performance

### Scalability Features

1. **Connection Pooling**: Efficient connection management
2. **Streaming Processing**: Memory-efficient large dataset handling
3. **Resumable Syncs**: Automatic retry and resume capabilities
4. **Rate Limiting**: Respect Databricks API limits

## Support and Maintenance

### Documentation

- Comprehensive README with setup instructions
- Detailed setup and testing guide
- Code documentation and examples
- Troubleshooting guide

### Testing

- Automated unit and integration tests
- Acceptance test configurations
- Local testing scripts
- Error scenario coverage

### Maintenance

- Regular dependency updates
- Security patch management
- Performance monitoring
- User feedback integration

## Conclusion

This Databricks source connector provides a robust, feature-rich solution for extracting data from Databricks using Airbyte. With support for both sync modes, comprehensive error handling, and extensive testing, it's ready for production use while maintaining the flexibility for future enhancements.

The connector follows Airbyte best practices and integrates seamlessly with the existing ecosystem, making it easy to deploy and maintain in both local and cloud environments.
