"""
Real-Time Streaming Analytics for Video Streaming Events

This module implements real-time streaming analytics using Spark Structured Streaming
with Delta Lake integration for ACID compliance and exactly-once processing.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from pathlib import Path
import json
import signal
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, when, lit, current_timestamp,
    window, count, sum as spark_sum, avg, max as spark_max,
    year, month, dayofmonth, hour, split, regexp_extract
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta import configure_spark_with_delta_pip

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config
from delta_lake.delta_setup import DeltaLakeManager
from spark_jobs.etl_framework import DataQualityValidator, TransformationEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealTimeStreamingProcessor:
    """
    Real-time streaming processor for video streaming events with:
    - Kafka integration for event ingestion
    - Delta Lake for ACID transactions
    - Real-time aggregations and analytics
    - Schema evolution and data quality monitoring
    """
    
    def __init__(self):
        """Initialize streaming processor"""
        self.config = get_config()
        self.spark = self._create_spark_session()
        self.delta_manager = DeltaLakeManager(self.spark)
        self.data_quality_validator = DataQualityValidator(self.spark)
        self.transformation_engine = TransformationEngine(self.spark)
        
        # Streaming queries
        self.active_queries = {}
        self.is_running = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Real-time streaming processor initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session optimized for streaming"""
        spark_config = self.config.get_spark_config()
        
        # Add streaming-specific configurations
        streaming_configs = {
            'spark.sql.streaming.metricsEnabled': 'true',
            'spark.sql.streaming.forceDeleteTempCheckpointLocation': 'true',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.streaming.kafka.useDeprecatedOffsetFetching': 'false'
        }
        
        spark_config.update(streaming_configs)
        
        builder = SparkSession.builder
        builder = configure_spark_with_delta_pip(builder)
        
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Spark session created for streaming")
        return spark
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop_all_streams()
        sys.exit(0)
    
    def create_kafka_stream(self, topics: list, consumer_group: str = "spark-streaming") -> DataFrame:
        """Create Kafka streaming DataFrame"""
        try:
            kafka_config = {
                'kafka.bootstrap.servers': ','.join(self.config.kafka.bootstrap_servers),
                'subscribe': ','.join(topics),
                'kafka.group.id': consumer_group,
                'startingOffsets': 'latest',
                'failOnDataLoss': 'false',
                'maxOffsetsPerTrigger': '1000',
                'kafka.session.timeout.ms': '30000',
                'kafka.request.timeout.ms': '40000'
            }
            
            # Add authentication if configured
            if self.config.kafka.security_protocol != 'PLAINTEXT':
                kafka_config.update({
                    'kafka.security.protocol': self.config.kafka.security_protocol,
                    'kafka.sasl.mechanism': self.config.kafka.sasl_mechanism,
                    'kafka.sasl.jaas.config': f"""org.apache.kafka.common.security.scram.ScramLoginModule required
                        username="{self.config.kafka.sasl_username}"
                        password="{self.config.kafka.sasl_password}";"""
                })
            
            df = self.spark.readStream \
                .format("kafka") \
                .options(**kafka_config) \
                .load()
            
            logger.info(f"Created Kafka stream for topics: {topics}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to create Kafka stream: {e}")
            raise
    
    def parse_kafka_events(self, kafka_df: DataFrame) -> DataFrame:
        """Parse Kafka messages and extract event data"""
        try:
            # Define schema for event parsing
            event_schema = StructType([
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("event_timestamp", StringType(), False),
                StructField("user_id", StringType(), True),
                StructField("session_id", StringType(), False),
                StructField("device_id", StringType(), False),
                StructField("video_id", StringType(), True),
                StructField("content_title", StringType(), True),
                StructField("content_type", StringType(), True),
                StructField("playback_position", IntegerType(), True),
                StructField("device_type", StringType(), True),
                StructField("platform", StringType(), True),
                StructField("country", StringType(), True)
            ])
            
            # Parse JSON messages
            parsed_df = kafka_df.select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_timestamp"),
                from_json(col("value").cast("string"), event_schema).alias("event_data")
            ).select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("kafka_timestamp"),
                col("event_data.*")
            )
            
            # Convert string timestamp to timestamp type
            parsed_df = parsed_df.withColumn(
                "event_timestamp",
                col("event_timestamp").cast(TimestampType())
            ).withColumn(
                "ingestion_timestamp",
                current_timestamp()
            )
            
            # Add partition columns for Delta Lake
            parsed_df = self.transformation_engine.add_partition_columns(parsed_df, "event_timestamp")
            
            logger.info("Parsed Kafka events successfully")
            return parsed_df
            
        except Exception as e:
            logger.error(f"Failed to parse Kafka events: {e}")
            raise
    
    def start_raw_ingestion_stream(self) -> None:
        """Start raw data ingestion stream from Kafka to Delta Lake"""
        try:
            # Create Kafka stream for all topics
            topics = ["video_events", "user_interactions", "ad_events", "session_events"]
            kafka_stream = self.create_kafka_stream(topics, "raw-ingestion-consumer")
            
            # Parse events
            parsed_stream = self.parse_kafka_events(kafka_stream)
            
            # Define output path
            output_path = f"{self.config.delta_lake.raw_table_path}raw_events"
            checkpoint_path = f"{self.config.spark.streaming_checkpoint_location}/raw_ingestion"
            
            # Start streaming query
            query = parsed_stream \
                .withWatermark("event_timestamp", "10 minutes") \
                .writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .partitionBy("year", "month", "day") \
                .trigger(processingTime=self.config.spark.streaming_trigger_interval) \
                .start(output_path)
            
            self.active_queries["raw_ingestion"] = query
            logger.info(f"Started raw ingestion stream to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to start raw ingestion stream: {e}")
            raise
    
    def start_video_events_stream(self) -> None:
        """Start video events processing stream"""
        try:
            # Read from raw events table
            raw_path = f"{self.config.delta_lake.raw_table_path}raw_events"
            
            video_stream = self.spark.readStream \
                .format("delta") \
                .option("maxFilesPerTrigger", "100") \
                .load(raw_path) \
                .filter(col("topic") == "video_events")
            
            # Apply transformations
            transformed_stream = video_stream \
                .withColumn("is_mobile", col("device_type").isin(["mobile", "tablet"])) \
                .withColumn("is_premium", col("subscription_tier").isin(["premium", "enterprise"])) \
                .withColumn("watch_duration_minutes", col("playback_position") / 60) \
                .withColumn("processing_timestamp", current_timestamp())
            
            # Define output path
            output_path = f"{self.config.delta_lake.bronze_table_path}video_events"
            checkpoint_path = f"{self.config.spark.streaming_checkpoint_location}/video_events"
            
            # Start streaming query
            query = transformed_stream \
                .withWatermark("event_timestamp", "5 minutes") \
                .writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .partitionBy("year", "month", "day") \
                .trigger(processingTime="30 seconds") \
                .start(output_path)
            
            self.active_queries["video_events"] = query
            logger.info(f"Started video events stream to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to start video events stream: {e}")
            raise
    
    def start_real_time_aggregations(self) -> None:
        """Start real-time aggregation streams"""
        try:
            # Read video events stream
            video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
            
            video_stream = self.spark.readStream \
                .format("delta") \
                .option("maxFilesPerTrigger", "50") \
                .load(video_events_path)
            
            # Real-time user activity aggregations (5-minute windows)
            user_activity = video_stream \
                .withWatermark("event_timestamp", "10 minutes") \
                .groupBy(
                    window(col("event_timestamp"), "5 minutes"),
                    col("user_id"),
                    col("device_type"),
                    col("country")
                ).agg(
                    count("*").alias("event_count"),
                    countDistinct("video_id").alias("unique_videos"),
                    countDistinct("session_id").alias("unique_sessions"),
                    avg("playback_position").alias("avg_watch_position"),
                    spark_sum(when(col("event_type") == "video_complete", 1).otherwise(0)).alias("completions")
                ).select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("user_id"),
                    col("device_type"),
                    col("country"),
                    col("event_count"),
                    col("unique_videos"),
                    col("unique_sessions"),
                    col("avg_watch_position"),
                    col("completions"),
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Write user activity aggregations
            user_activity_path = f"{self.config.delta_lake.silver_table_path}user_activity_5min"
            user_activity_checkpoint = f"{self.config.spark.streaming_checkpoint_location}/user_activity_5min"
            
            user_activity_query = user_activity \
                .writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", user_activity_checkpoint) \
                .trigger(processingTime="1 minute") \
                .start(user_activity_path)
            
            self.active_queries["user_activity_5min"] = user_activity_query
            
            # Real-time content popularity (10-minute windows)
            content_popularity = video_stream \
                .withWatermark("event_timestamp", "15 minutes") \
                .groupBy(
                    window(col("event_timestamp"), "10 minutes"),
                    col("video_id"),
                    col("content_type"),
                    col("content_genre")
                ).agg(
                    count("*").alias("total_events"),
                    countDistinct("user_id").alias("unique_viewers"),
                    countDistinct("session_id").alias("unique_sessions"),
                    avg("playback_position").alias("avg_watch_position"),
                    spark_max("playback_position").alias("max_watch_position"),
                    spark_sum(when(col("event_type") == "video_play", 1).otherwise(0)).alias("play_events"),
                    spark_sum(when(col("event_type") == "video_complete", 1).otherwise(0)).alias("completion_events")
                ).select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("video_id"),
                    col("content_type"),
                    col("content_genre"),
                    col("total_events"),
                    col("unique_viewers"),
                    col("unique_sessions"),
                    col("avg_watch_position"),
                    col("max_watch_position"),
                    col("play_events"),
                    col("completion_events"),
                    (col("completion_events") / col("play_events")).alias("completion_rate"),
                    current_timestamp().alias("aggregation_timestamp")
                )
            
            # Write content popularity aggregations
            content_popularity_path = f"{self.config.delta_lake.silver_table_path}content_popularity_10min"
            content_popularity_checkpoint = f"{self.config.spark.streaming_checkpoint_location}/content_popularity_10min"
            
            content_popularity_query = content_popularity \
                .writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", content_popularity_checkpoint) \
                .trigger(processingTime="2 minutes") \
                .start(content_popularity_path)
            
            self.active_queries["content_popularity_10min"] = content_popularity_query
            
            logger.info("Started real-time aggregation streams")
            
        except Exception as e:
            logger.error(f"Failed to start real-time aggregations: {e}")
            raise
    
    def start_anomaly_detection_stream(self) -> None:
        """Start anomaly detection stream for monitoring unusual patterns"""
        try:
            # Read video events stream
            video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
            
            video_stream = self.spark.readStream \
                .format("delta") \
                .option("maxFilesPerTrigger", "20") \
                .load(video_events_path)
            
            # Detect potential anomalies (simplified rules-based approach)
            anomaly_stream = video_stream \
                .withWatermark("event_timestamp", "5 minutes") \
                .groupBy(
                    window(col("event_timestamp"), "1 minute"),
                    col("user_id"),
                    col("device_type")
                ).agg(
                    count("*").alias("event_count"),
                    countDistinct("video_id").alias("unique_videos"),
                    spark_sum(when(col("event_type") == "video_error", 1).otherwise(0)).alias("error_count")
                ).select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("user_id"),
                    col("device_type"),
                    col("event_count"),
                    col("unique_videos"),
                    col("error_count"),
                    # Define anomaly conditions
                    when(col("event_count") > 100, "high_activity")
                    .when(col("error_count") > 5, "high_errors")
                    .when(col("unique_videos") > 20, "rapid_browsing")
                    .otherwise("normal").alias("anomaly_type"),
                    current_timestamp().alias("detection_timestamp")
                ).filter(col("anomaly_type") != "normal")
            
            # Write anomalies
            anomaly_path = f"{self.config.delta_lake.silver_table_path}anomalies"
            anomaly_checkpoint = f"{self.config.spark.streaming_checkpoint_location}/anomalies"
            
            anomaly_query = anomaly_stream \
                .writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", anomaly_checkpoint) \
                .trigger(processingTime="30 seconds") \
                .start(anomaly_path)
            
            self.active_queries["anomaly_detection"] = anomaly_query
            logger.info("Started anomaly detection stream")
            
        except Exception as e:
            logger.error(f"Failed to start anomaly detection stream: {e}")
            raise
    
    def start_all_streams(self) -> None:
        """Start all streaming processes"""
        try:
            self.is_running = True
            
            logger.info("Starting all streaming processes...")
            
            # Start streams in order
            self.start_raw_ingestion_stream()
            self.start_video_events_stream()
            self.start_real_time_aggregations()
            self.start_anomaly_detection_stream()
            
            logger.info(f"Started {len(self.active_queries)} streaming queries")
            
            # Monitor streams
            self.monitor_streams()
            
        except Exception as e:
            logger.error(f"Failed to start streaming processes: {e}")
            self.stop_all_streams()
            raise
    
    def stop_all_streams(self) -> None:
        """Stop all streaming queries gracefully"""
        logger.info("Stopping all streaming queries...")
        
        for name, query in self.active_queries.items():
            try:
                logger.info(f"Stopping stream: {name}")
                query.stop()
                logger.info(f"Stopped stream: {name}")
            except Exception as e:
                logger.error(f"Error stopping stream {name}: {e}")
        
        self.active_queries.clear()
        self.is_running = False
        logger.info("All streaming queries stopped")
    
    def monitor_streams(self) -> None:
        """Monitor streaming queries and handle failures"""
        import time
        
        logger.info("Starting stream monitoring...")
        
        while self.is_running and self.active_queries:
            try:
                # Check status of all queries
                active_count = 0
                failed_queries = []
                
                for name, query in self.active_queries.items():
                    if query.isActive:
                        active_count += 1
                        
                        # Log progress periodically
                        progress = query.lastProgress
                        if progress:
                            input_rows = progress.get("inputRowsPerSecond", 0)
                            processed_rows = progress.get("numInputRows", 0)
                            logger.info(f"Stream {name}: {input_rows:.1f} rows/sec, "
                                      f"processed {processed_rows} rows")
                    else:
                        failed_queries.append(name)
                        if query.exception():
                            logger.error(f"Stream {name} failed: {query.exception()}")
                
                # Remove failed queries
                for failed_name in failed_queries:
                    del self.active_queries[failed_name]
                
                logger.info(f"Active streams: {active_count}/{len(self.active_queries) + len(failed_queries)}")
                
                # Stop monitoring if no active queries
                if active_count == 0:
                    logger.warning("No active streams remaining, stopping monitor")
                    break
                
                # Wait before next check
                time.sleep(30)
                
            except KeyboardInterrupt:
                logger.info("Received interrupt, stopping streams...")
                break
            except Exception as e:
                logger.error(f"Error in stream monitoring: {e}")
                time.sleep(10)
    
    def get_stream_status(self) -> Dict[str, Any]:
        """Get status of all streaming queries"""
        status = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_queries": len(self.active_queries),
            "active_queries": 0,
            "queries": {}
        }
        
        for name, query in self.active_queries.items():
            query_status = {
                "is_active": query.isActive,
                "last_progress": query.lastProgress,
                "exception": str(query.exception()) if query.exception() else None
            }
            
            if query.isActive:
                status["active_queries"] += 1
            
            status["queries"][name] = query_status
        
        return status


def main():
    """Main function for running real-time streaming"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-Time Streaming Analytics")
    parser.add_argument('--streams', nargs='+', 
                       choices=['raw_ingestion', 'video_events', 'aggregations', 'anomalies', 'all'],
                       default=['all'], help='Streams to start')
    parser.add_argument('--monitor-only', action='store_true',
                       help='Only monitor existing streams')
    
    args = parser.parse_args()
    
    processor = RealTimeStreamingProcessor()
    
    try:
        if args.monitor_only:
            # Just monitor existing streams
            processor.monitor_streams()
        elif 'all' in args.streams:
            # Start all streams
            processor.start_all_streams()
        else:
            # Start specific streams
            processor.is_running = True
            
            if 'raw_ingestion' in args.streams:
                processor.start_raw_ingestion_stream()
            if 'video_events' in args.streams:
                processor.start_video_events_stream()
            if 'aggregations' in args.streams:
                processor.start_real_time_aggregations()
            if 'anomalies' in args.streams:
                processor.start_anomaly_detection_stream()
            
            processor.monitor_streams()
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
    finally:
        processor.stop_all_streams()


if __name__ == "__main__":
    main()
