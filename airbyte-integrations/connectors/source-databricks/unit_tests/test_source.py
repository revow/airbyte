#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import unittest
from unittest.mock import Mock, patch

from source_databricks.source import SourceDatabricks


class TestSourceDatabricks(unittest.TestCase):
    def setUp(self):
        self.source = SourceDatabricks()
        self.config = {
            "workspace_url": "https://test-workspace.cloud.databricks.com",
            "personal_access_token": "test_token",
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "test_table",
            "cursor_field": "updated_at"
        }

    def test_source_initialization(self):
        """Test that the source can be initialized."""
        self.assertIsInstance(self.source, SourceDatabricks)

    @patch('source_databricks.source.DatabricksClient')
    def test_check_connection_success(self, mock_client_class):
        """Test successful connection check."""
        mock_client = Mock()
        mock_client.check_connection.return_value = (True, None)
        mock_client_class.return_value = mock_client

        success, error = self.source.check_connection(Mock(), self.config)
        
        self.assertTrue(success)
        self.assertIsNone(error)
        mock_client.check_connection.assert_called_once()

    @patch('source_databricks.source.DatabricksClient')
    def test_check_connection_failure(self, mock_client_class):
        """Test failed connection check."""
        mock_client = Mock()
        mock_client.check_connection.return_value = (False, "Connection failed")
        mock_client_class.return_value = mock_client

        success, error = self.source.check_connection(Mock(), self.config)
        
        self.assertFalse(success)
        self.assertEqual(error, "Connection failed")

    @patch('source_databricks.source.DatabricksClient')
    def test_check_connection_exception(self, mock_client_class):
        """Test connection check with exception."""
        mock_client_class.side_effect = Exception("Test exception")

        success, error = self.source.check_connection(Mock(), self.config)
        
        self.assertFalse(success)
        self.assertEqual(error, "Test exception")

    @patch('source_databricks.source.DatabricksClient')
    def test_streams(self, mock_client_class):
        """Test that streams are created correctly."""
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        streams = self.source.streams(self.config)
        
        self.assertEqual(len(streams), 1)
        self.assertEqual(streams[0].name, "test_catalog_test_schema_test_table")


if __name__ == '__main__':
    unittest.main()
