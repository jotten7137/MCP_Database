# Dockerfile for MCP Server

FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libsndfile1 \
    ffmpeg \
    pkg-config \
    git \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements files
COPY mcp-requirements.txt .
COPY database-requirements.txt .
COPY .env .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r mcp-requirements.txt && \
    pip install --no-cache-dir -r database-requirements.txt

# Copy the project files
COPY . .

# Copy database files
COPY iris.sqlite ./iris.sqlite

# Create necessary directories
RUN mkdir -p ./cache ./models

ENV PYTHONPATH=/app

# Expose port
EXPOSE 8000

# Run the server with the correct module path
CMD ["uvicorn", "mcp_server.main:app", "--host", "0.0.0.0", "--port", "8000"]