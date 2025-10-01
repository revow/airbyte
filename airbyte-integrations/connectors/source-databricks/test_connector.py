#!/usr/bin/env python3
"""
Test script for the Databricks source connector.
This script demonstrates how to test the connector locally.
"""

import json
import sys
from pathlib import Path

# Add the source directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / "source_databricks"))

from source_databricks.source import SourceDatabricks


def test_connector():
    """Test the connector with sample configuration."""
    
    # Sample configuration - replace with your actual values
    config = {
        "workspace_url": "https://your-workspace.cloud.databricks.com",
        "personal_access_token": "your_pat_token",
        "catalog": "hive_metastore",
        "schema": "default",
        "table": "users",
        "cursor_field": "updated_at"
    }
    
    # Create source instance
    source = SourceDatabricks()
    
    print("Testing Databricks Source Connector")
    print("=" * 40)
    
    # Test 1: Check connection
    print("\n1. Testing connection...")
    try:
        success, error = source.check_connection(None, config)
        if success:
            print("✅ Connection successful!")
        else:
            print(f"❌ Connection failed: {error}")
    except Exception as e:
        print(f"❌ Connection test error: {str(e)}")
    
    # Test 2: Get streams
    print("\n2. Testing stream discovery...")
    try:
        streams = source.streams(config)
        print(f"✅ Found {len(streams)} stream(s)")
        for stream in streams:
            print(f"   - Stream: {stream.name}")
            print(f"     Sync mode: {getattr(stream, 'sync_mode', 'unknown')}")
            print(f"     Cursor field: {getattr(stream, 'cursor_field', 'none')}")
    except Exception as e:
        print(f"❌ Stream discovery error: {str(e)}")
    
    # Test 3: Get schema
    print("\n3. Testing schema retrieval...")
    try:
        for stream in streams:
            schema = stream.get_json_schema()
            print(f"✅ Schema for {stream.name}:")
            print(f"   Properties: {list(schema.get('properties', {}).keys())}")
            print(f"   Required: {schema.get('required', [])}")
    except Exception as e:
        print(f"❌ Schema retrieval error: {str(e)}")
    
    print("\n" + "=" * 40)
    print("Testing complete!")


def load_config_from_file(config_path: str):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Configuration file not found: {config_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in configuration file: {e}")
        return None


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Load config from file if provided
        config_file = sys.argv[1]
        config = load_config_from_file(config_file)
        if config:
            # Override the test function to use the loaded config
            def test_connector():
                source = SourceDatabricks()
                print(f"Testing connector with config from: {config_file}")
                print("=" * 40)
                
                # Test connection
                print("\n1. Testing connection...")
                try:
                    success, error = source.check_connection(None, config)
                    if success:
                        print("✅ Connection successful!")
                    else:
                        print(f"❌ Connection failed: {error}")
                except Exception as e:
                    print(f"❌ Connection test error: {str(e)}")
                
                # Test streams
                print("\n2. Testing stream discovery...")
                try:
                    streams = source.streams(config)
                    print(f"✅ Found {len(streams)} stream(s)")
                    for stream in streams:
                        print(f"   - Stream: {stream.name}")
                except Exception as e:
                    print(f"❌ Stream discovery error: {str(e)}")
                
                print("\n" + "=" * 40)
                print("Testing complete!")
    
    test_connector()
