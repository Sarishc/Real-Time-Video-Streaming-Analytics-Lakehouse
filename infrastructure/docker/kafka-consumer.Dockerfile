# Kafka Consumer Service Dockerfile
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY kafka-setup/ ./kafka-setup/
COPY config/ ./config/
COPY data-generation/ ./data-generation/

# Create logs directory
RUN mkdir -p logs

# Set Python path
ENV PYTHONPATH=/app

# Run Kafka consumer
CMD ["python", "kafka-setup/consumer.py"]
