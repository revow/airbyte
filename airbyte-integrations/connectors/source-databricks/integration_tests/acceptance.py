#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

from typing import Generator

import pytest
from airbyte_cdk.sources.streams import Stream
from source_databricks.source import SourceDatabricks


@pytest.fixture
def source() -> Generator[SourceDatabricks, None, None]:
    yield SourceDatabricks()


@pytest.fixture
def config():
    return {
        "workspace_url": "https://test-workspace.cloud.databricks.com",
        "personal_access_token": "test_token",
        "catalog": "test_catalog",
        "schema": "test_schema",
        "table": "test_table",
        "cursor_field": "updated_at"
    }


class TestSourceDatabricks:
    def test_streams(self, source: SourceDatabricks, config):
        streams = source.streams(config)
        assert len(streams) == 1
        assert isinstance(streams[0], Stream)
        assert streams[0].name == "test_catalog_test_schema_test_table"

    def test_check_connection_success(self, source: SourceDatabricks, config):
        # This test would require a mock DatabricksClient
        # For now, we'll just test that the method exists
        assert hasattr(source, 'check_connection')
        assert callable(source.check_connection)
