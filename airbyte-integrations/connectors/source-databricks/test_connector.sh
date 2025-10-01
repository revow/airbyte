#!/bin/bash

echo "🚀 Testing Source-Databricks Connector"
echo "========================================"

# Check if config.json exists
if [ ! -f "config.json" ]; then
    echo "❌ config.json not found. Please create it first."
    exit 1
fi

echo "📋 Testing connection..."
docker run --rm -v $(pwd)/config.json:/config.json airbyte/source-databricks:dev check --config /config.json

echo ""
echo "🔍 Discovering streams..."
docker run --rm -v $(pwd)/config.json:/config.json airbyte/source-databricks:dev discover --config /config.json

echo ""
echo "✅ Test complete!"

