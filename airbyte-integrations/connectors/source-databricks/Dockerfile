FROM python:3.10-slim

# Set working directory
WORKDIR /airbyte/integration_code

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --only=main --no-root

# Copy source code
COPY source_databricks ./source_databricks
COPY main.py ./

# Set the entrypoint
ENTRYPOINT ["python", "main.py"]
