#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import StreamData
from airbyte_cdk.sources.utils.schema_helpers import ResourceSchemaLoader

from .client import DatabricksClient


class DatabricksStream(Stream):
    """Stream for reading data from Databricks tables."""
    
    primary_key = None  # No primary key for this stream
    cursor_field = "updated_at"  # Default cursor field for incremental sync
    
    def __init__(self, config: Mapping[str, Any]):
        """
        Initialize the Databricks stream.
        
        :param config: Configuration dictionary
        """
        super().__init__()
        self.config = config
        self.client = DatabricksClient(config)
        self.logger = logging.getLogger(__name__)
        
        # Get cursor field from config or use default
        self.cursor_field = config.get("cursor_field", self.cursor_field)
        
        # Set sync mode based on whether cursor field is provided
        self.sync_mode = "incremental" if self.cursor_field else "full_refresh"
    
    @property
    def name(self) -> str:
        """Return the name of the stream."""
        return f"{self.config.get('catalog', 'default')}_{self.config.get('schema', 'default')}_{self.config.get('table', 'default')}"
    
    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> MutableMapping[str, Any]:
        """
        Update the stream state based on the latest record.
        
        :param current_stream_state: Current state of the stream
        :param latest_record: Latest record processed
        :return: Updated state
        """
        if self.sync_mode == "incremental" and self.cursor_field:
            current_cursor_value = current_stream_state.get(self.cursor_field, "")
            latest_cursor_value = latest_record.get(self.cursor_field, "")
            
            if latest_cursor_value and latest_cursor_value > current_cursor_value:
                current_stream_state[self.cursor_field] = latest_cursor_value
        
        return current_stream_state
    
    def read_records(
        self,
        sync_mode: str,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[StreamData]:
        """
        Read records from the Databricks table.
        
        :param sync_mode: Sync mode (full_refresh or incremental)
        :param cursor_field: Field to use for cursor-based filtering
        :param stream_slice: Stream slice information
        :param stream_state: Current state of the stream
        :return: Iterable of records
        """
        try:
            # Get cursor value from stream state for incremental sync
            cursor_value = None
            if sync_mode == "incremental" and stream_state and self.cursor_field:
                cursor_value = stream_state.get(self.cursor_field)
            
            # Get data from Databricks
            records = self.client.get_table_data(
                cursor_field=self.cursor_field if sync_mode == "incremental" else None,
                cursor_value=cursor_value
            )
            
            # Yield each record
            for record in records:
                # Convert any non-serializable types
                processed_record = self._process_record(record)
                yield processed_record
                
        except Exception as e:
            self.logger.error(f"Error reading records: {str(e)}")
            raise
    
    def _process_record(self, record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Process a record to ensure it's serializable.
        
        :param record: Raw record from Databricks
        :return: Processed record
        """
        processed = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                processed[key] = value.isoformat()
            elif hasattr(value, 'isoformat'):  # Handle other datetime-like objects
                processed[key] = value.isoformat()
            else:
                processed[key] = value
        return processed
    
    def get_json_schema(self) -> Dict[str, Any]:
        """
        Get the JSON schema for this stream.
        
        :return: JSON schema dictionary
        """
        try:
            # Try to get schema from Databricks
            schema_data = self.client.get_table_schema()
            
            # Convert Databricks schema to JSON schema format
            properties = {}
            required = []
            
            for column in schema_data:
                column_name = column.get("col_name", "")
                column_type = column.get("data_type", "string")
                
                if column_name and column_name != "# col_name":
                    # Map Databricks types to JSON schema types
                    json_type = self._map_databricks_type_to_json(column_type)
                    properties[column_name] = {"type": json_type}
                    
                    # Add to required fields if not nullable
                    if "nullable" not in column.get("comment", "").lower():
                        required.append(column_name)
            
            return {
                "type": "object",
                "properties": properties,
                "required": required
            }
            
        except Exception as e:
            self.logger.warning(f"Could not retrieve schema from Databricks: {str(e)}")
            # Return a basic schema as fallback
            return {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "data": {"type": "string"}
                },
                "required": ["id"]
            }
    
    def _map_databricks_type_to_json(self, databricks_type: str) -> str:
        """
        Map Databricks data types to JSON schema types.
        
        :param databricks_type: Databricks data type
        :return: JSON schema type
        """
        type_mapping = {
            "string": "string",
            "varchar": "string",
            "char": "string",
            "text": "string",
            "int": "integer",
            "integer": "integer",
            "bigint": "integer",
            "long": "integer",
            "double": "number",
            "float": "number",
            "decimal": "number",
            "boolean": "boolean",
            "bool": "boolean",
            "date": "string",
            "timestamp": "string",
            "datetime": "string",
            "binary": "string",
            "array": "array",
            "map": "object",
            "struct": "object"
        }
        
        # Extract base type (remove precision/scale info)
        base_type = databricks_type.split("(")[0].lower()
        return type_mapping.get(base_type, "string")
    
    def supports_incremental(self) -> bool:
        """Check if this stream supports incremental sync."""
        return self.cursor_field is not None
    
    def get_cursor_field(self) -> Optional[List[str]]:
        """Get the cursor field for incremental sync."""
        return [self.cursor_field] if self.cursor_field else None
