"""
Delta Lake Setup and Table Management

This module provides comprehensive Delta Lake table management including:
- Table creation with optimized schemas
- ACID transaction support
- Schema evolution and versioning
- Z-ordering and optimization
- Data lifecycle management
- Time travel capabilities
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType, BooleanType, ArrayType, MapType
)
from pyspark.sql.functions import col, expr, current_timestamp, lit
from delta import DeltaTable, configure_spark_with_delta_pip
from delta.tables import DeltaTable

# Add parent directory to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeltaLakeManager:
    """
    Comprehensive Delta Lake management system with:
    - Automatic table creation and schema management
    - ACID transaction support
    - Schema evolution capabilities
    - Performance optimization (Z-ordering, compaction)
    - Data lifecycle management
    - Time travel and versioning
    """
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """Initialize Delta Lake manager"""
        self.config = get_config()
        self.spark = spark_session or self._create_spark_session()
        
        # Enable Delta Lake optimizations
        self._configure_delta_optimizations()
        
        # Define table schemas
        self.table_schemas = self._define_table_schemas()
        
        logger.info("Delta Lake manager initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create optimized Spark session with Delta Lake support"""
        try:
            # Get Spark configuration
            spark_config = self.config.get_spark_config()
            
            # Build Spark session
            builder = SparkSession.builder
            
            # Set application name
            builder = builder.appName(spark_config['spark.app.name'])
            
            # Configure for Delta Lake
            builder = configure_spark_with_delta_pip(builder)
            
            # Add all configurations
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            # Additional Delta Lake configurations
            delta_configs = {
                'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
                'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
                'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
                'spark.databricks.delta.schema.autoMerge.enabled': 'true',
                'spark.databricks.delta.optimizeWrite.enabled': 'true',
                'spark.databricks.delta.autoCompact.enabled': 'true'
            }
            
            for key, value in delta_configs.items():
                builder = builder.config(key, value)
            
            spark = builder.getOrCreate()
            
            # Set log level
            spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session created with Delta Lake support")
            return spark
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            raise
    
    def _configure_delta_optimizations(self):
        """Configure Delta Lake optimizations"""
        try:
            # Set Delta Lake configurations
            self.spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
            self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
            self.spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", "true")
            
            logger.info("Delta Lake optimizations configured")
            
        except Exception as e:
            logger.error(f"Failed to configure Delta Lake optimizations: {e}")
    
    def _define_table_schemas(self) -> Dict[str, StructType]:
        """Define schemas for all Delta tables"""
        schemas = {}
        
        # Raw events table schema - flexible schema for ingestion
        raw_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            StructField("source_topic", StringType(), True),
            StructField("source_partition", IntegerType(), True),
            StructField("source_offset", LongType(), True),
            StructField("raw_data", StringType(), False),  # JSON string of original event
            StructField("processing_metadata", MapType(StringType(), StringType()), True)
        ])
        schemas['raw_events'] = raw_schema
        
        # Bronze layer - video events
        video_events_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            
            # User and session info
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), False),
            StructField("device_id", StringType(), False),
            
            # Video content info
            StructField("video_id", StringType(), False),
            StructField("content_title", StringType(), True),
            StructField("content_type", StringType(), True),
            StructField("content_duration", IntegerType(), True),
            StructField("content_genre", StringType(), True),
            StructField("content_rating", StringType(), True),
            StructField("content_language", StringType(), True),
            StructField("content_creator_id", StringType(), True),
            
            # Playback info
            StructField("playback_position", IntegerType(), True),
            StructField("video_quality", StringType(), True),
            StructField("audio_language", StringType(), True),
            StructField("subtitle_language", StringType(), True),
            
            # Performance metrics
            StructField("buffer_duration", DoubleType(), True),
            StructField("startup_time", DoubleType(), True),
            StructField("bitrate", IntegerType(), True),
            StructField("dropped_frames", IntegerType(), True),
            
            # Network info
            StructField("connection_type", StringType(), True),
            StructField("bandwidth", DoubleType(), True),
            
            # Device and location
            StructField("device_type", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            
            # Error info (for error events)
            StructField("error_code", StringType(), True),
            StructField("error_message", StringType(), True),
            
            # Seek info (for seek events)
            StructField("seek_from_position", IntegerType(), True),
            StructField("seek_to_position", IntegerType(), True),
            
            # Partitioning columns
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False)
        ])
        schemas['bronze_video_events'] = video_events_schema
        
        # Bronze layer - user interactions
        user_interactions_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            
            # User and session info
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), False),
            StructField("device_id", StringType(), False),
            
            # Content info
            StructField("content_id", StringType(), False),
            StructField("content_type", StringType(), True),
            StructField("content_creator_id", StringType(), True),
            
            # Interaction details
            StructField("interaction_context", StringType(), True),
            StructField("comment_text", StringType(), True),
            StructField("share_platform", StringType(), True),
            StructField("share_url", StringType(), True),
            
            # User info
            StructField("subscription_tier", StringType(), True),
            
            # Device and location
            StructField("device_type", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("app_version", StringType(), True),
            StructField("ip_address", StringType(), True),
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True),
            
            # Partitioning columns
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False)
        ])
        schemas['bronze_user_interactions'] = user_interactions_schema
        
        # Bronze layer - ad events
        ad_events_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            
            # User and session info
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), False),
            StructField("device_id", StringType(), False),
            
            # Ad info
            StructField("ad_id", StringType(), False),
            StructField("ad_type", StringType(), False),
            StructField("ad_duration", IntegerType(), False),
            StructField("ad_position", IntegerType(), False),
            StructField("ad_creative_id", StringType(), True),
            StructField("ad_campaign_id", StringType(), True),
            StructField("advertiser_id", StringType(), True),
            
            # Content context
            StructField("content_id", StringType(), False),
            StructField("content_type", StringType(), True),
            
            # Ad performance
            StructField("view_duration", IntegerType(), True),
            StructField("skip_position", IntegerType(), True),
            StructField("click_position", IntegerType(), True),
            
            # Revenue info
            StructField("ad_price", DoubleType(), True),
            StructField("currency", StringType(), True),
            
            # Error info
            StructField("error_code", StringType(), True),
            StructField("error_message", StringType(), True),
            
            # Device and location
            StructField("device_type", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("country", StringType(), True),
            
            # Partitioning columns
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False)
        ])
        schemas['bronze_ad_events'] = ad_events_schema
        
        # Bronze layer - session events
        session_events_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("ingestion_timestamp", TimestampType(), False),
            
            # User and session info
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), False),
            StructField("device_id", StringType(), False),
            
            # Session info
            StructField("session_duration", IntegerType(), True),
            StructField("page_views", IntegerType(), True),
            StructField("videos_watched", IntegerType(), True),
            
            # User info
            StructField("subscription_tier", StringType(), True),
            StructField("user_signup_date", TimestampType(), True),
            StructField("user_tier_change_date", TimestampType(), True),
            
            # Device change info
            StructField("previous_device_id", StringType(), True),
            StructField("previous_device_type", StringType(), True),
            
            # Performance metrics
            StructField("app_crashes", IntegerType(), True),
            StructField("network_errors", IntegerType(), True),
            
            # Device and location
            StructField("device_type", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("country", StringType(), True),
            
            # Partitioning columns
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False),
            StructField("hour", IntegerType(), False)
        ])
        schemas['bronze_session_events'] = session_events_schema
        
        # Silver layer - enriched video sessions
        video_sessions_schema = StructType([
            StructField("session_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("video_id", StringType(), False),
            StructField("session_start", TimestampType(), False),
            StructField("session_end", TimestampType(), True),
            StructField("session_duration", IntegerType(), True),
            StructField("total_watch_time", IntegerType(), True),
            StructField("completion_rate", DoubleType(), True),
            StructField("buffer_events", IntegerType(), True),
            StructField("seek_events", IntegerType(), True),
            StructField("quality_changes", IntegerType(), True),
            StructField("avg_bitrate", DoubleType(), True),
            StructField("device_type", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("country", StringType(), True),
            StructField("content_type", StringType(), True),
            StructField("content_genre", StringType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
        schemas['silver_video_sessions'] = video_sessions_schema
        
        # Gold layer - daily user metrics
        daily_user_metrics_schema = StructType([
            StructField("date", TimestampType(), False),
            StructField("user_id", StringType(), False),
            StructField("total_watch_time", IntegerType(), False),
            StructField("videos_watched", IntegerType(), False),
            StructField("unique_content_types", IntegerType(), False),
            StructField("sessions_count", IntegerType(), False),
            StructField("avg_session_duration", DoubleType(), False),
            StructField("completion_rate", DoubleType(), False),
            StructField("interaction_events", IntegerType(), False),
            StructField("ad_impressions", IntegerType(), False),
            StructField("ad_clicks", IntegerType(), False),
            StructField("revenue", DoubleType(), False),
            StructField("device_types", ArrayType(StringType()), False),
            StructField("countries", ArrayType(StringType()), False),
            StructField("subscription_tier", StringType(), True),
            StructField("year", IntegerType(), False),
            StructField("month", IntegerType(), False),
            StructField("day", IntegerType(), False)
        ])
        schemas['gold_daily_user_metrics'] = daily_user_metrics_schema
        
        return schemas
    
    def create_all_tables(self) -> Dict[str, bool]:
        """Create all Delta tables"""
        results = {}
        
        # Define table configurations
        table_configs = {
            'raw_events': {
                'path': f"{self.config.delta_lake.raw_table_path}raw_events",
                'partition_columns': ['year', 'month', 'day'],
                'z_order_columns': ['event_timestamp', 'event_type']
            },
            'bronze_video_events': {
                'path': f"{self.config.delta_lake.bronze_table_path}video_events",
                'partition_columns': ['year', 'month', 'day'],
                'z_order_columns': ['user_id', 'video_id', 'event_timestamp']
            },
            'bronze_user_interactions': {
                'path': f"{self.config.delta_lake.bronze_table_path}user_interactions",
                'partition_columns': ['year', 'month', 'day'],
                'z_order_columns': ['user_id', 'content_id', 'event_timestamp']
            },
            'bronze_ad_events': {
                'path': f"{self.config.delta_lake.bronze_table_path}ad_events",
                'partition_columns': ['year', 'month', 'day'],
                'z_order_columns': ['user_id', 'ad_id', 'event_timestamp']
            },
            'bronze_session_events': {
                'path': f"{self.config.delta_lake.bronze_table_path}session_events",
                'partition_columns': ['year', 'month', 'day'],
                'z_order_columns': ['user_id', 'session_id', 'event_timestamp']
            },
            'silver_video_sessions': {
                'path': f"{self.config.delta_lake.silver_table_path}video_sessions",
                'partition_columns': ['year', 'month'],
                'z_order_columns': ['user_id', 'video_id', 'session_start']
            },
            'gold_daily_user_metrics': {
                'path': f"{self.config.delta_lake.gold_table_path}daily_user_metrics",
                'partition_columns': ['year', 'month'],
                'z_order_columns': ['user_id', 'date']
            }
        }
        
        for table_name, config in table_configs.items():
            try:
                success = self.create_table(
                    table_name=table_name,
                    table_path=config['path'],
                    schema=self.table_schemas[table_name],
                    partition_columns=config['partition_columns'],
                    z_order_columns=config.get('z_order_columns', [])
                )
                results[table_name] = success
                
            except Exception as e:
                logger.error(f"Failed to create table {table_name}: {e}")
                results[table_name] = False
        
        return results
    
    def create_table(self, table_name: str, table_path: str, schema: StructType,
                    partition_columns: List[str] = None, z_order_columns: List[str] = None,
                    table_properties: Dict[str, str] = None) -> bool:
        """Create a Delta table with optimized configuration"""
        try:
            # Check if table already exists
            if self._table_exists(table_path):
                logger.info(f"Table {table_name} already exists at {table_path}")
                return True
            
            # Default table properties
            default_properties = {
                'delta.autoOptimize.optimizeWrite': 'true',
                'delta.autoOptimize.autoCompact': 'true',
                'delta.enableChangeDataFeed': 'true',
                'delta.deletedFileRetentionDuration': 'interval 7 days',
                'delta.logRetentionDuration': 'interval 30 days'
            }
            
            if table_properties:
                default_properties.update(table_properties)
            
            # Create empty DataFrame with schema
            empty_df = self.spark.createDataFrame([], schema)
            
            # Write to create table
            writer = empty_df.write.format("delta")
            
            # Add partitioning
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            # Add table properties
            for key, value in default_properties.items():
                writer = writer.option(key, value)
            
            # Create table
            writer.save(table_path)
            
            # Create table in catalog
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{table_path}'
            """)
            
            # Apply Z-ordering if specified
            if z_order_columns:
                self.optimize_table(table_path, z_order_columns)
            
            logger.info(f"Created Delta table: {table_name} at {table_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            return False
    
    def _table_exists(self, table_path: str) -> bool:
        """Check if Delta table exists at path"""
        try:
            DeltaTable.forPath(self.spark, table_path)
            return True
        except:
            return False
    
    def get_table(self, table_path: str) -> Optional[DeltaTable]:
        """Get Delta table instance"""
        try:
            return DeltaTable.forPath(self.spark, table_path)
        except Exception as e:
            logger.error(f"Failed to get table at {table_path}: {e}")
            return None
    
    def optimize_table(self, table_path: str, z_order_columns: List[str] = None) -> bool:
        """Optimize Delta table with compaction and Z-ordering"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            # Basic optimization (compaction)
            optimize_cmd = delta_table.optimize()
            
            # Add Z-ordering if specified
            if z_order_columns:
                optimize_cmd = optimize_cmd.executeZOrderBy(*z_order_columns)
                logger.info(f"Optimized table with Z-ordering on columns: {z_order_columns}")
            else:
                optimize_cmd.executeCompaction()
                logger.info("Optimized table with compaction")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to optimize table at {table_path}: {e}")
            return False
    
    def vacuum_table(self, table_path: str, retention_hours: int = None) -> bool:
        """Vacuum Delta table to remove old files"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            # Use configured retention or default
            retention = retention_hours or self.config.delta_lake.vacuum_retention_hours
            
            delta_table.vacuum(retentionHours=retention)
            
            logger.info(f"Vacuumed table at {table_path} with {retention} hours retention")
            return True
            
        except Exception as e:
            logger.error(f"Failed to vacuum table at {table_path}: {e}")
            return False
    
    def get_table_history(self, table_path: str, limit: int = 20) -> Optional[DataFrame]:
        """Get table history for time travel"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                return None
            
            return delta_table.history(limit)
            
        except Exception as e:
            logger.error(f"Failed to get history for table at {table_path}: {e}")
            return None
    
    def restore_table(self, table_path: str, version: int) -> bool:
        """Restore table to a specific version"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            delta_table.restoreToVersion(version)
            
            logger.info(f"Restored table at {table_path} to version {version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to restore table at {table_path}: {e}")
            return False
    
    def get_table_details(self, table_path: str) -> Optional[Dict[str, Any]]:
        """Get detailed table information"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                return None
            
            # Get table details
            detail_df = delta_table.detail()
            details = detail_df.collect()[0].asDict()
            
            # Get latest history
            history_df = delta_table.history(1)
            latest_history = history_df.collect()[0].asDict() if history_df.count() > 0 else {}
            
            # Combine information
            table_info = {
                'name': details.get('name'),
                'location': details.get('location'),
                'created_at': details.get('createdAt'),
                'last_modified': details.get('lastModified'),
                'size_in_bytes': details.get('sizeInBytes'),
                'num_files': details.get('numFiles'),
                'min_reader_version': details.get('minReaderVersion'),
                'min_writer_version': details.get('minWriterVersion'),
                'partition_columns': details.get('partitionColumns'),
                'table_features': details.get('tableFeatures', []),
                'latest_version': latest_history.get('version'),
                'latest_operation': latest_history.get('operation'),
                'latest_timestamp': latest_history.get('timestamp')
            }
            
            return table_info
            
        except Exception as e:
            logger.error(f"Failed to get details for table at {table_path}: {e}")
            return None
    
    def merge_data(self, table_path: str, source_df: DataFrame, 
                  merge_condition: str, when_matched_update: Dict[str, str] = None,
                  when_not_matched_insert: Dict[str, str] = None) -> bool:
        """Perform merge operation on Delta table"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            # Start merge operation
            merge_builder = delta_table.alias("target").merge(
                source_df.alias("source"),
                merge_condition
            )
            
            # Add when matched clause
            if when_matched_update:
                merge_builder = merge_builder.whenMatchedUpdate(set=when_matched_update)
            
            # Add when not matched clause
            if when_not_matched_insert:
                merge_builder = merge_builder.whenNotMatchedInsert(values=when_not_matched_insert)
            
            # Execute merge
            merge_builder.execute()
            
            logger.info(f"Successfully merged data into table at {table_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to merge data into table at {table_path}: {e}")
            return False
    
    def delete_data(self, table_path: str, condition: str) -> bool:
        """Delete data from Delta table"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            delta_table.delete(condition)
            
            logger.info(f"Deleted data from table at {table_path} with condition: {condition}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete data from table at {table_path}: {e}")
            return False
    
    def update_data(self, table_path: str, condition: str, set_values: Dict[str, str]) -> bool:
        """Update data in Delta table"""
        try:
            delta_table = self.get_table(table_path)
            if not delta_table:
                logger.error(f"Table not found at {table_path}")
                return False
            
            delta_table.update(condition, set_values)
            
            logger.info(f"Updated data in table at {table_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update data in table at {table_path}: {e}")
            return False
    
    def get_table_metrics(self, table_path: str) -> Dict[str, Any]:
        """Get comprehensive table metrics"""
        try:
            details = self.get_table_details(table_path)
            if not details:
                return {}
            
            # Read table to get row count (for small tables)
            try:
                df = self.spark.read.format("delta").load(table_path)
                row_count = df.count()
            except:
                row_count = None
            
            metrics = {
                'size_in_bytes': details.get('sizeInBytes', 0),
                'size_in_mb': (details.get('sizeInBytes', 0) / 1024 / 1024),
                'num_files': details.get('numFiles', 0),
                'row_count': row_count,
                'latest_version': details.get('latest_version'),
                'created_at': details.get('created_at'),
                'last_modified': details.get('last_modified'),
                'partition_columns': details.get('partition_columns', []),
                'avg_file_size_mb': (
                    (details.get('sizeInBytes', 0) / 1024 / 1024) / max(1, details.get('numFiles', 1))
                )
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics for table at {table_path}: {e}")
            return {}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Delta Lake setup"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'spark_session_active': self.spark is not None,
            'tables': {},
            'total_tables': 0,
            'healthy_tables': 0,
            'total_size_gb': 0
        }
        
        try:
            # Check each table
            table_paths = {
                'raw_events': f"{self.config.delta_lake.raw_table_path}raw_events",
                'bronze_video_events': f"{self.config.delta_lake.bronze_table_path}video_events",
                'bronze_user_interactions': f"{self.config.delta_lake.bronze_table_path}user_interactions",
                'bronze_ad_events': f"{self.config.delta_lake.bronze_table_path}ad_events",
                'bronze_session_events': f"{self.config.delta_lake.bronze_table_path}session_events",
                'silver_video_sessions': f"{self.config.delta_lake.silver_table_path}video_sessions",
                'gold_daily_user_metrics': f"{self.config.delta_lake.gold_table_path}daily_user_metrics"
            }
            
            for table_name, table_path in table_paths.items():
                table_health = {
                    'exists': self._table_exists(table_path),
                    'path': table_path,
                    'metrics': {}
                }
                
                if table_health['exists']:
                    metrics = self.get_table_metrics(table_path)
                    table_health['metrics'] = metrics
                    health['total_size_gb'] += metrics.get('size_in_mb', 0) / 1024
                    health['healthy_tables'] += 1
                
                health['tables'][table_name] = table_health
                health['total_tables'] += 1
            
            # Determine overall status
            if health['healthy_tables'] == 0:
                health['status'] = 'unhealthy'
            elif health['healthy_tables'] < health['total_tables']:
                health['status'] = 'degraded'
            
            # Check Spark session
            if not self.spark:
                health['status'] = 'unhealthy'
                health['error'] = 'Spark session not available'
        
        except Exception as e:
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        return health
    
    def close(self):
        """Close Spark session and cleanup resources"""
        try:
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")


def main():
    """Main function for Delta Lake setup operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Delta Lake Setup and Management")
    parser.add_argument('--action', choices=['create', 'optimize', 'vacuum', 'health', 'details'],
                       default='create', help='Action to perform')
    parser.add_argument('--table', type=str, help='Specific table to operate on')
    parser.add_argument('--all', action='store_true', help='Apply to all tables')
    parser.add_argument('--retention-hours', type=int, default=168,
                       help='Retention hours for vacuum operation')
    
    args = parser.parse_args()
    
    # Initialize Delta Lake manager
    manager = DeltaLakeManager()
    
    try:
        if args.action == 'create':
            if args.all:
                results = manager.create_all_tables()
                for table_name, success in results.items():
                    status = "✓" if success else "✗"
                    print(f"{status} {table_name}")
            else:
                print("Use --all to create all tables")
        
        elif args.action == 'health':
            health = manager.health_check()
            print(f"Status: {health['status']}")
            print(f"Total tables: {health['total_tables']}")
            print(f"Healthy tables: {health['healthy_tables']}")
            print(f"Total size: {health['total_size_gb']:.2f} GB")
            
            for table_name, info in health['tables'].items():
                status = "✓" if info['exists'] else "✗"
                size_mb = info['metrics'].get('size_in_mb', 0)
                print(f"  {status} {table_name}: {size_mb:.1f} MB")
        
        elif args.action == 'details':
            if args.table:
                table_paths = {
                    'raw_events': f"{manager.config.delta_lake.raw_table_path}raw_events",
                    'bronze_video_events': f"{manager.config.delta_lake.bronze_table_path}video_events",
                    # Add other table paths...
                }
                
                if args.table in table_paths:
                    details = manager.get_table_details(table_paths[args.table])
                    if details:
                        print(f"Table: {args.table}")
                        print(f"Location: {details['location']}")
                        print(f"Size: {details['size_in_bytes']} bytes")
                        print(f"Files: {details['num_files']}")
                        print(f"Latest version: {details['latest_version']}")
                        print(f"Last modified: {details['last_modified']}")
                    else:
                        print(f"Table {args.table} not found")
                else:
                    print(f"Unknown table: {args.table}")
            else:
                print("Specify --table")
        
        else:
            print(f"Action {args.action} not implemented yet")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        manager.close()


if __name__ == "__main__":
    main()
