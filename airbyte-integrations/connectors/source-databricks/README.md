# Databricks Source Connector

This is the repository for the Databricks source connector, written in Python using the Airbyte CDK.

## Features

- **Full Refresh Sync**: Complete data extraction from specified tables
- **Incremental Sync**: Incremental data extraction using cursor columns (e.g., `updated_at`)
- **Flexible Configuration**: Support for workspace URL, catalog, schema, and table selection
- **Authentication**: Personal Access Token (PAT) authentication
- **Multiple Databricks Clouds**: Support for AWS, Azure, and GCP Databricks workspaces

## Local Development

### Prerequisites

- Python 3.9+
- Poetry
- Airbyte instance (local or cloud)

### Installation

1. Install dependencies:
```bash
poetry install
```

2. Activate the virtual environment:
```bash
poetry shell
```

### Running the Connector

#### Spec Command
```bash
poetry run source-databricks spec
```

#### Check Command
```bash
poetry run source-databricks check --config config.json
```

#### Discover Command
```bash
poetry run source-databricks discover --config config.json
```

#### Read Command
```bash
poetry run source-databricks read --config config.json --catalog catalog.json
```

### Configuration

Create a `config.json` file with your Databricks configuration:

```json
{
  "workspace_url": "https://your-workspace.cloud.databricks.com",
  "personal_access_token": "your_pat_token",
  "catalog": "your_catalog",
  "schema": "your_schema",
  "table": "your_table",
  "cursor_field": "updated_at"
}
```

### Testing

Run unit tests:
```bash
poetry run pytest
```

## Project Structure

```
source-databricks/
├── source_databricks/
│   ├── __init__.py
│   ├── source.py          # Main source class
│   ├── streams.py         # Stream implementations
│   ├── client.py          # Databricks client wrapper
│   └── run.py            # Entry point
├── integration_tests/     # Integration test configurations
├── unit_tests/           # Unit tests
├── pyproject.toml        # Dependencies and project config
├── metadata.yaml         # Connector metadata
└── README.md            # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License
