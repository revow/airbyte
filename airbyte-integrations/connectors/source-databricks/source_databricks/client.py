#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pandas as pd
from databricks import sql
from databricks.sql.client import Connection, Cursor


class DatabricksClient:
    """Client for interacting with Databricks SQL endpoint."""
    
    def __init__(self, config: Mapping[str, Any]):
        """
        Initialize the Databricks client.
        
        :param config: Configuration dictionary containing connection parameters
        """
        self.config = config
        self.workspace_url = config["workspace_url"]
        self.personal_access_token = config["personal_access_token"]
        self.catalog = config.get("catalog")
        self.schema = config.get("schema")
        self.table = config.get("table")
        self.logger = logging.getLogger(__name__)
        
        # Remove trailing slash from workspace URL if present
        if self.workspace_url.endswith('/'):
            self.workspace_url = self.workspace_url[:-1]
    
    def _get_connection(self) -> Connection:
        """Create and return a connection to Databricks."""
        # Get warehouse ID from config or use default
        warehouse_id = self.config.get("warehouse_id", "your_warehouse_id")
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
        
        # Extract hostname from workspace URL (remove https:// prefix)
        hostname = self.workspace_url
        if hostname.startswith("https://"):
            hostname = hostname[8:]  # Remove "https://" prefix
        
        return sql.connect(
            server_hostname=hostname,
            http_path=http_path,
            access_token=self.personal_access_token
        )
    
    def check_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test the connection to Databricks.
        
        :return: Tuple of (success, error_message)
        """
        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    # Test with a simple query
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result and len(result) > 0 and result[0] == 1:
                        return True, None
                    else:
                        return False, "Connection test query failed"
        except Exception as e:
            self.logger.error(f"Connection check failed: {str(e)}")
            return False, str(e)
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        :param query: SQL query to execute
        :return: List of dictionaries representing the query results
        """
        try:
            with self._get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                    
                    # Get column names
                    columns = [desc[0] for desc in cursor.description]
                    
                    # Fetch all rows and convert to list of dictionaries
                    rows = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_table_schema(self) -> List[Dict[str, Any]]:
        """
        Get the schema of the specified table.
        
        :return: List of dictionaries containing table schema information
        """
        query = f"""
        DESCRIBE TABLE {self.catalog}.{self.schema}.{self.table}
        """
        return self.execute_query(query)
    
    def get_table_data(self, limit: Optional[int] = None, cursor_field: Optional[str] = None, cursor_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get data from the specified table with optional filtering.
        
        :param limit: Maximum number of rows to return
        :param cursor_field: Field to use for cursor-based filtering
        :param cursor_value: Value to filter on for cursor field
        :return: List of dictionaries representing the table data
        """
        query = f"SELECT * FROM {self.catalog}.{self.schema}.{self.table}"
        
        # Add cursor-based filtering if provided
        if cursor_field and cursor_value:
            query += f" WHERE {cursor_field} > '{cursor_value}'"
        
        # Add ordering by cursor field if provided
        if cursor_field:
            query += f" ORDER BY {cursor_field}"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
    
    def get_table_count(self, cursor_field: Optional[str] = None, cursor_value: Optional[str] = None) -> int:
        """
        Get the count of rows in the specified table.
        
        :param cursor_field: Field to use for cursor-based filtering
        :param cursor_value: Value to filter on for cursor field
        :return: Number of rows in the table
        """
        query = f"SELECT COUNT(*) as count FROM {self.catalog}.{self.schema}.{self.table}"
        
        if cursor_field and cursor_value:
            query += f" WHERE {cursor_field} > '{cursor_value}'"
        
        result = self.execute_query(query)
        if result and len(result) > 0 and result[0] and "count" in result[0]:
            return int(result[0]["count"])
        return 0
