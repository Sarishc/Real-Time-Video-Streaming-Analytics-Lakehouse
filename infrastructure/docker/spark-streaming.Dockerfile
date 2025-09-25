# Spark Streaming Service Dockerfile
FROM bitnami/spark:3.5.0

# Switch to root for package installation
USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    kafka-python==2.0.2 \
    boto3==1.34.0 \
    pydantic==2.5.2 \
    prometheus-client==0.19.0

# Copy application code
COPY spark-jobs/ /app/spark-jobs/
COPY delta-lake/ /app/delta-lake/
COPY config/ /app/config/
COPY data-generation/ /app/data-generation/

# Create logs directory
RUN mkdir -p /app/logs

# Set working directory
WORKDIR /app

# Set Python path
ENV PYTHONPATH=/app

# Switch back to spark user
USER 1001

# Run Spark streaming job
CMD ["spark-submit", \
     "--packages", "io.delta:delta-core_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension", \
     "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog", \
     "spark-jobs/real_time_streaming.py"]
