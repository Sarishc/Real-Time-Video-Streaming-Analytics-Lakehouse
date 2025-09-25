# 🚀 Quick Start Guide - Video Streaming Analytics Lakehouse

## Prerequisites
- Docker Desktop installed and running
- 8GB+ RAM available
- 10GB+ disk space available

## Option 1: Automated Startup (Recommended)

```bash
# 1. Start Docker Desktop first (manual step)
# 2. Run the automated startup script
./scripts/start_pipeline.sh
```

## Option 2: Manual Step-by-Step Startup

### Step 1: Start Core Infrastructure
```bash
# Start Zookeeper and Kafka
docker-compose up -d zookeeper kafka

# Wait for Kafka to be ready (60 seconds)
sleep 60

# Start MinIO (S3-compatible storage)
docker-compose up -d minio

# Start Spark cluster
docker-compose up -d spark-master spark-worker
```

### Step 2: Create Kafka Topics
```bash
# Create optimized Kafka topics
python3 kafka-setup/topics.py --action create --all
```

### Step 3: Start Data Processing Services
```bash
# Start data generator (creates realistic streaming events)
docker-compose up -d data-generator

# Start Kafka consumer (processes events to Delta Lake)
docker-compose up -d kafka-consumer

# Start Spark streaming (real-time analytics)
docker-compose up -d spark-streaming
```

### Step 4: Start Monitoring and Tools
```bash
# Start monitoring stack
docker-compose up -d prometheus grafana redis postgres

# Start development tools
docker-compose up -d jupyter kafka-ui
```

## 🌐 Access URLs (After Startup)

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | None |
| **Spark Master** | http://localhost:8081 | None |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin123 |
| **Grafana** | http://localhost:3000 | admin/admin123 |
| **Prometheus** | http://localhost:9090 | None |
| **Jupyter** | http://localhost:8888 | None |

## 🔍 Verify Everything is Working

### Check Service Status
```bash
# View all running services
docker-compose ps

# Check logs for any service
docker-compose logs -f [service-name]
```

### Monitor Data Flow
1. **Kafka UI** (http://localhost:8080): See messages flowing through topics
2. **Spark UI** (http://localhost:8081): Monitor real-time processing jobs
3. **Grafana** (http://localhost:3000): View system metrics and dashboards

### View Generated Data
```bash
# Check Delta Lake tables
python3 delta-lake/delta_setup.py --action health

# Monitor Kafka topics
python3 kafka-setup/topics.py --action health
```

## 📊 What's Running

Once started, your lakehouse processes:
- **10 events/second** of realistic video streaming data
- **Real-time analytics** with 5-minute windows
- **ACID transactions** in Delta Lake
- **Comprehensive monitoring** of all components

## 🛑 Stop the Pipeline

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## 🔧 Troubleshooting

### Common Issues

1. **"Port already in use"**
   ```bash
   # Find and kill process using port (e.g., 9092)
   lsof -ti:9092 | xargs kill -9
   ```

2. **"Docker daemon not running"**
   - Start Docker Desktop application
   - Wait for it to fully initialize

3. **"Out of memory"**
   - Increase Docker memory limit to 8GB+
   - Close other applications

4. **Services not starting**
   ```bash
   # Check individual service logs
   docker-compose logs kafka
   docker-compose logs spark-master
   ```

### Health Checks
```bash
# Check Kafka connectivity
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check MinIO
curl http://localhost:9000/minio/health/live

# Check Spark
curl http://localhost:8081
```

## 🚀 Next Steps

After the system is running:

1. **Explore Data**: Open Jupyter (http://localhost:8888) and check the notebooks
2. **View Analytics**: Open Grafana (http://localhost:3000) for real-time dashboards  
3. **Monitor System**: Use Kafka UI (http://localhost:8080) to see message flow
4. **Scale Up**: Add more Spark workers with `docker-compose up -d --scale spark-worker=3`

## 📈 Sample Queries

Once data is flowing, you can query Delta Lake tables:

```python
# In Jupyter notebook
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Analytics").getOrCreate()

# Read video events
events_df = spark.read.format("delta").load("./data/delta-tables/bronze/video_events")
events_df.count()  # See how many events processed

# Daily active users
events_df.select("user_id", "event_timestamp") \
    .filter("event_timestamp >= current_date() - 1") \
    .select("user_id").distinct().count()
```

Your Video Streaming Analytics Lakehouse is now ready! 🎉
