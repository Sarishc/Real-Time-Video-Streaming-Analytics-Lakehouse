#!/bin/bash

# Video Streaming Analytics Lakehouse - Quick Start Script
# This script starts the complete data pipeline infrastructure

set -e

echo "🚀 Starting Video Streaming Analytics Lakehouse Pipeline"
echo "========================================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

print_header "Step 1: Creating necessary directories"
mkdir -p logs
mkdir -p data/checkpoints
mkdir -p data/delta-tables/{raw,bronze,silver,gold}
mkdir -p notebooks
print_status "Directories created"

print_header "Step 2: Starting infrastructure services"
print_status "Starting Zookeeper, Kafka, MinIO, Spark, Monitoring..."

# Start core infrastructure
docker-compose up -d zookeeper kafka minio spark-master spark-worker

print_status "Waiting for services to be ready..."
sleep 30

# Check if Kafka is ready
print_status "Checking Kafka readiness..."
for i in {1..30}; do
    if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; then
        print_status "Kafka is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_error "Kafka failed to start properly"
        exit 1
    fi
    sleep 2
done

# Check if MinIO is ready
print_status "Checking MinIO readiness..."
for i in {1..20}; do
    if curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        print_status "MinIO is ready"
        break
    fi
    if [ $i -eq 20 ]; then
        print_error "MinIO failed to start properly"
        exit 1
    fi
    sleep 2
done

print_header "Step 3: Setting up Kafka topics"
# Create Kafka topics using the topics management script
python3 kafka-setup/topics.py --action create --all
print_status "Kafka topics created"

print_header "Step 4: Starting monitoring services"
docker-compose up -d prometheus grafana redis postgres
print_status "Monitoring services started"

print_header "Step 5: Starting data processing services"
# Start data generator
docker-compose up -d data-generator
print_status "Data generator started"

# Start Kafka consumer
docker-compose up -d kafka-consumer  
print_status "Kafka consumer started"

# Start Spark streaming
docker-compose up -d spark-streaming
print_status "Spark streaming started"

print_header "Step 6: Starting additional services"
# Start Jupyter for data exploration
docker-compose up -d jupyter
print_status "Jupyter notebook started"

# Start Kafka UI for monitoring
docker-compose up -d kafka-ui
print_status "Kafka UI started"

print_header "✅ Pipeline Started Successfully!"
echo "========================================"
echo ""
echo "🌐 Access URLs:"
echo "   Kafka UI:         http://localhost:8080"
echo "   Spark Master UI:  http://localhost:8081" 
echo "   MinIO Console:    http://localhost:9001"
echo "   Grafana:          http://localhost:3000 (admin/admin123)"
echo "   Prometheus:       http://localhost:9090"
echo "   Jupyter:          http://localhost:8888"
echo ""
echo "📊 Key Features Running:"
echo "   ✓ Real-time event generation (10 events/sec)"
echo "   ✓ Kafka streaming ingestion"
echo "   ✓ Delta Lake ACID storage"
echo "   ✓ Spark real-time analytics"
echo "   ✓ Comprehensive monitoring"
echo ""
echo "📁 Data Locations:"
echo "   Raw Events:       ./data/delta-tables/raw/"
echo "   Processed Data:   ./data/delta-tables/bronze/"
echo "   Analytics:        ./data/delta-tables/silver/"
echo "   Aggregations:     ./data/delta-tables/gold/"
echo ""
echo "🔧 Management Commands:"
echo "   View logs:        docker-compose logs -f [service-name]"
echo "   Stop pipeline:    docker-compose down"
echo "   Restart service:  docker-compose restart [service-name]"
echo ""
echo "📈 Monitor Progress:"
echo "   • Check Kafka UI for message throughput"
echo "   • Monitor Spark UI for job progress"
echo "   • View Grafana dashboards for system metrics"
echo ""

# Optional: Run initial data quality checks
if command -v python3 &> /dev/null; then
    print_header "Step 7: Running initial health checks"
    echo "Waiting 60 seconds for data to flow through pipeline..."
    sleep 60
    
    print_status "Checking Kafka topic health..."
    python3 kafka-setup/topics.py --action health
    
    print_status "Checking Delta Lake tables..."
    python3 delta-lake/delta_setup.py --action health
    
    print_status "Pipeline health check completed"
fi

print_header "🎉 Video Streaming Analytics Lakehouse is Ready!"
echo ""
echo "The system is now processing real-time video streaming events and"
echo "building analytics on top of a modern data lakehouse architecture."
echo ""
echo "Next steps:"
echo "1. Explore data in Jupyter notebooks"
echo "2. View real-time analytics in Grafana"
echo "3. Monitor system health via Prometheus"
echo "4. Scale processing by adding Spark workers"
echo ""
echo "For troubleshooting, check logs with: docker-compose logs -f"
