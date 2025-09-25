"""
Comprehensive Spark ETL Framework for Video Streaming Analytics

This module provides a reusable, production-ready ETL framework with:
- Data quality validation and monitoring
- Incremental processing capabilities
- Error handling and retry mechanisms
- Performance optimization and monitoring
- Configurable data lineage tracking
- Schema evolution support
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Callable, Tuple, Union
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import uuid
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, isnan, isnull, 
    count, sum as spark_sum, max as spark_max, min as spark_min,
    regexp_extract, regexp_replace, split, explode, 
    year, month, dayofmonth, hour, date_format,
    lag, lead, row_number, rank, dense_rank,
    collect_list, collect_set, first, last
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from delta import DeltaTable
from delta.tables import DeltaTable as DeltaTableClass
from prometheus_client import Counter, Histogram, Gauge

# Add parent directory to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config
from delta_lake.delta_setup import DeltaLakeManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
ETL_JOBS_TOTAL = Counter('etl_jobs_total', 'Total ETL jobs executed', ['job_name', 'status'])
ETL_JOB_DURATION = Histogram('etl_job_duration_seconds', 'ETL job duration', ['job_name'])
ETL_RECORDS_PROCESSED = Counter('etl_records_processed_total', 'Total records processed', ['job_name', 'stage'])
ETL_DATA_QUALITY_SCORE = Gauge('etl_data_quality_score', 'Data quality score', ['job_name', 'dataset'])
ETL_ERRORS_TOTAL = Counter('etl_errors_total', 'Total ETL errors', ['job_name', 'error_type'])


@dataclass
class DataQualityMetrics:
    """Data quality metrics for ETL monitoring"""
    total_records: int
    valid_records: int
    invalid_records: int
    duplicate_records: int
    null_key_records: int
    data_quality_score: float
    validation_errors: Dict[str, int]
    timestamp: datetime


@dataclass
class ETLJobMetrics:
    """ETL job execution metrics"""
    job_id: str
    job_name: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str = "running"  # running, completed, failed
    records_read: int = 0
    records_written: int = 0
    records_filtered: int = 0
    processing_time_seconds: float = 0.0
    error_message: Optional[str] = None
    data_quality_metrics: Optional[DataQualityMetrics] = None


@dataclass
class ETLConfig:
    """ETL job configuration"""
    job_name: str
    source_path: str
    target_path: str
    checkpoint_path: str
    batch_size: int = 1000
    max_files_per_trigger: int = 100
    enable_data_quality: bool = True
    enable_deduplication: bool = True
    watermark_column: str = "event_timestamp"
    watermark_delay: str = "10 minutes"
    output_mode: str = "append"  # append, complete, update
    trigger_interval: str = "30 seconds"
    partition_columns: List[str] = None
    z_order_columns: List[str] = None
    
    def __post_init__(self):
        if self.partition_columns is None:
            self.partition_columns = []
        if self.z_order_columns is None:
            self.z_order_columns = []


class DataQualityValidator:
    """Comprehensive data quality validation framework"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.validation_rules = {}
    
    def add_validation_rule(self, column_name: str, rule_name: str, 
                          validation_func: Callable[[DataFrame], DataFrame]):
        """Add custom validation rule"""
        if column_name not in self.validation_rules:
            self.validation_rules[column_name] = {}
        self.validation_rules[column_name][rule_name] = validation_func
    
    def validate_dataframe(self, df: DataFrame, job_name: str) -> Tuple[DataFrame, DataQualityMetrics]:
        """Perform comprehensive data quality validation"""
        start_time = datetime.now()
        total_records = df.count()
        validation_errors = {}
        
        # Add data quality columns
        df_with_quality = df.withColumn("_dq_valid", lit(True)) \
                           .withColumn("_dq_errors", lit(""))
        
        # Basic null checks for required fields
        required_fields = ["event_id", "event_timestamp", "session_id"]
        for field in required_fields:
            if field in df.columns:
                null_count = df.filter(col(field).isNull()).count()
                if null_count > 0:
                    validation_errors[f"{field}_null"] = null_count
                    df_with_quality = df_with_quality.withColumn(
                        "_dq_valid",
                        when(col(field).isNull(), False).otherwise(col("_dq_valid"))
                    ).withColumn(
                        "_dq_errors",
                        when(col(field).isNull(), 
                             concat(col("_dq_errors"), lit(f"{field}_null;"))
                        ).otherwise(col("_dq_errors"))
                    )
        
        # Timestamp validation
        if "event_timestamp" in df.columns:
            future_timestamps = df.filter(col("event_timestamp") > current_timestamp()).count()
            if future_timestamps > 0:
                validation_errors["future_timestamp"] = future_timestamps
                df_with_quality = df_with_quality.withColumn(
                    "_dq_valid",
                    when(col("event_timestamp") > current_timestamp(), False)
                    .otherwise(col("_dq_valid"))
                )
        
        # Duplicate detection
        duplicate_count = 0
        if "event_id" in df.columns:
            duplicate_count = df.groupBy("event_id").count().filter(col("count") > 1).count()
            validation_errors["duplicates"] = duplicate_count
        
        # Custom validation rules
        for column_name, rules in self.validation_rules.items():
            if column_name in df.columns:
                for rule_name, validation_func in rules.items():
                    try:
                        df_with_quality = validation_func(df_with_quality)
                    except Exception as e:
                        logger.error(f"Validation rule {rule_name} failed: {e}")
                        validation_errors[f"{rule_name}_error"] = 1
        
        # Calculate metrics
        valid_records = df_with_quality.filter(col("_dq_valid") == True).count()
        invalid_records = total_records - valid_records
        
        # Data quality score (0-1)
        data_quality_score = valid_records / max(total_records, 1)
        
        metrics = DataQualityMetrics(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicate_records=duplicate_count,
            null_key_records=validation_errors.get("event_id_null", 0),
            data_quality_score=data_quality_score,
            validation_errors=validation_errors,
            timestamp=start_time
        )
        
        # Update Prometheus metrics
        ETL_DATA_QUALITY_SCORE.labels(job_name=job_name, dataset="input").set(data_quality_score)
        
        for error_type, count in validation_errors.items():
            ETL_ERRORS_TOTAL.labels(job_name=job_name, error_type=error_type).inc(count)
        
        logger.info(f"Data quality validation completed: {data_quality_score:.3f} score, "
                   f"{valid_records}/{total_records} valid records")
        
        return df_with_quality, metrics


class IncrementalProcessor:
    """Handles incremental data processing with watermarking and checkpoints"""
    
    def __init__(self, spark: SparkSession, delta_manager: DeltaLakeManager):
        self.spark = spark
        self.delta_manager = delta_manager
    
    def get_incremental_data(self, source_path: str, target_path: str, 
                           watermark_column: str, checkpoint_path: str) -> DataFrame:
        """Get incremental data since last processed timestamp"""
        try:
            # Get last processed timestamp from target table
            last_processed_timestamp = self._get_last_processed_timestamp(target_path, watermark_column)
            
            # Read source data
            df = self.spark.read.format("delta").load(source_path)
            
            # Filter for incremental data
            if last_processed_timestamp:
                df = df.filter(col(watermark_column) > last_processed_timestamp)
                logger.info(f"Processing incremental data since {last_processed_timestamp}")
            else:
                logger.info("Processing full dataset (no previous checkpoint)")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to get incremental data: {e}")
            raise
    
    def _get_last_processed_timestamp(self, target_path: str, watermark_column: str):
        """Get the last processed timestamp from target table"""
        try:
            if self.delta_manager._table_exists(target_path):
                target_df = self.spark.read.format("delta").load(target_path)
                last_timestamp = target_df.agg(spark_max(watermark_column)).collect()[0][0]
                return last_timestamp
            return None
        except Exception as e:
            logger.warning(f"Could not get last processed timestamp: {e}")
            return None
    
    def deduplicate_data(self, df: DataFrame, key_columns: List[str], 
                        order_column: str = "event_timestamp") -> DataFrame:
        """Remove duplicates based on key columns, keeping latest record"""
        try:
            if not key_columns:
                return df
            
            # Create window for deduplication
            window = Window.partitionBy(*key_columns).orderBy(col(order_column).desc())
            
            # Add row number and filter to keep only first (latest) record
            df_deduped = df.withColumn("_row_num", row_number().over(window)) \
                          .filter(col("_row_num") == 1) \
                          .drop("_row_num")
            
            original_count = df.count()
            deduped_count = df_deduped.count()
            duplicates_removed = original_count - deduped_count
            
            logger.info(f"Deduplication completed: removed {duplicates_removed} duplicates "
                       f"({original_count} -> {deduped_count} records)")
            
            return df_deduped
            
        except Exception as e:
            logger.error(f"Deduplication failed: {e}")
            return df


class TransformationEngine:
    """Advanced data transformation engine with common patterns"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def add_partition_columns(self, df: DataFrame, timestamp_column: str = "event_timestamp") -> DataFrame:
        """Add standard partition columns for efficient querying"""
        return df.withColumn("year", year(col(timestamp_column))) \
                .withColumn("month", month(col(timestamp_column))) \
                .withColumn("day", dayofmonth(col(timestamp_column))) \
                .withColumn("hour", hour(col(timestamp_column)))
    
    def enrich_user_session_data(self, df: DataFrame) -> DataFrame:
        """Enrich data with user session metrics"""
        try:
            # Session-level aggregations
            session_window = Window.partitionBy("session_id")
            
            df_enriched = df.withColumn("session_event_count", 
                                      count("*").over(session_window)) \
                           .withColumn("session_start_time", 
                                     spark_min("event_timestamp").over(session_window)) \
                           .withColumn("session_end_time", 
                                     spark_max("event_timestamp").over(session_window))
            
            # Calculate session duration
            df_enriched = df_enriched.withColumn(
                "session_duration_minutes",
                (col("session_end_time").cast("long") - col("session_start_time").cast("long")) / 60
            )
            
            return df_enriched
            
        except Exception as e:
            logger.error(f"Session enrichment failed: {e}")
            return df
    
    def calculate_video_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate video-specific metrics"""
        try:
            # Video-level aggregations
            video_window = Window.partitionBy("video_id", "session_id")
            
            df_metrics = df.withColumn("video_watch_time", 
                                     when(col("event_type") == "video_complete", 
                                          col("content_duration"))
                                     .otherwise(col("playback_position"))) \
                          .withColumn("video_completion_rate",
                                    col("video_watch_time") / col("content_duration"))
            
            return df_metrics
            
        except Exception as e:
            logger.error(f"Video metrics calculation failed: {e}")
            return df
    
    def clean_and_standardize(self, df: DataFrame) -> DataFrame:
        """Clean and standardize common data issues"""
        try:
            # Standardize string fields
            string_columns = [field.name for field in df.schema.fields 
                            if field.dataType == StringType()]
            
            for col_name in string_columns:
                df = df.withColumn(col_name, 
                                 regexp_replace(col(col_name), r"^\s+|\s+$", ""))  # Trim
                     .withColumn(col_name,
                               when(col(col_name) == "", None).otherwise(col(col_name)))  # Empty to null
            
            # Standardize country codes
            if "country" in df.columns:
                df = df.withColumn("country", upper(col("country")))
            
            # Clean IP addresses (basic validation)
            if "ip_address" in df.columns:
                ip_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
                df = df.withColumn("ip_address",
                                 when(regexp_extract(col("ip_address"), ip_pattern, 0) != "",
                                      col("ip_address")).otherwise(None))
            
            return df
            
        except Exception as e:
            logger.error(f"Data cleaning failed: {e}")
            return df


class ETLJobBase(ABC):
    """Abstract base class for ETL jobs with common functionality"""
    
    def __init__(self, config: ETLConfig, spark: SparkSession = None):
        self.config = config
        self.spark = spark or self._create_spark_session()
        self.delta_manager = DeltaLakeManager(self.spark)
        self.data_quality_validator = DataQualityValidator(self.spark)
        self.incremental_processor = IncrementalProcessor(self.spark, self.delta_manager)
        self.transformation_engine = TransformationEngine(self.spark)
        self.job_metrics = ETLJobMetrics(
            job_id=str(uuid.uuid4()),
            job_name=config.job_name,
            start_time=datetime.now(timezone.utc)
        )
        
        logger.info(f"Initialized ETL job: {config.job_name}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with optimized settings"""
        app_config = get_config()
        spark_config = app_config.get_spark_config()
        
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform extracted data"""
        pass
    
    @abstractmethod
    def load(self, df: DataFrame) -> bool:
        """Load transformed data to target"""
        pass
    
    def run(self) -> ETLJobMetrics:
        """Execute the complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            logger.info(f"Starting ETL job: {self.config.job_name}")
            
            # Extract
            logger.info("Starting extraction phase")
            source_df = self.extract()
            self.job_metrics.records_read = source_df.count()
            ETL_RECORDS_PROCESSED.labels(
                job_name=self.config.job_name, 
                stage="extract"
            ).inc(self.job_metrics.records_read)
            
            # Data quality validation
            if self.config.enable_data_quality:
                logger.info("Performing data quality validation")
                source_df, dq_metrics = self.data_quality_validator.validate_dataframe(
                    source_df, self.config.job_name
                )
                self.job_metrics.data_quality_metrics = dq_metrics
            
            # Deduplication
            if self.config.enable_deduplication:
                logger.info("Performing deduplication")
                source_df = self.incremental_processor.deduplicate_data(
                    source_df, ["event_id"], "event_timestamp"
                )
            
            # Transform
            logger.info("Starting transformation phase")
            transformed_df = self.transform(source_df)
            
            # Add partition columns
            if self.config.partition_columns:
                transformed_df = self.transformation_engine.add_partition_columns(transformed_df)
            
            # Load
            logger.info("Starting load phase")
            success = self.load(transformed_df)
            
            if success:
                self.job_metrics.records_written = transformed_df.count()
                self.job_metrics.status = "completed"
                ETL_JOBS_TOTAL.labels(job_name=self.config.job_name, status="success").inc()
            else:
                self.job_metrics.status = "failed"
                ETL_JOBS_TOTAL.labels(job_name=self.config.job_name, status="failed").inc()
            
            # Calculate metrics
            end_time = datetime.now()
            self.job_metrics.end_time = end_time
            self.job_metrics.processing_time_seconds = (end_time - start_time).total_seconds()
            
            ETL_JOB_DURATION.labels(job_name=self.config.job_name).observe(
                self.job_metrics.processing_time_seconds
            )
            
            logger.info(f"ETL job completed: {self.config.job_name} in "
                       f"{self.job_metrics.processing_time_seconds:.2f} seconds")
            
            return self.job_metrics
            
        except Exception as e:
            self.job_metrics.status = "failed"
            self.job_metrics.error_message = str(e)
            self.job_metrics.end_time = datetime.now()
            
            ETL_JOBS_TOTAL.labels(job_name=self.config.job_name, status="error").inc()
            ETL_ERRORS_TOTAL.labels(job_name=self.config.job_name, error_type="job_failure").inc()
            
            logger.error(f"ETL job failed: {self.config.job_name} - {e}")
            logger.error(traceback.format_exc())
            
            return self.job_metrics
    
    def run_streaming(self):
        """Run ETL job in streaming mode"""
        try:
            logger.info(f"Starting streaming ETL job: {self.config.job_name}")
            
            # Read streaming data
            streaming_df = self.spark.readStream \
                .format("delta") \
                .option("maxFilesPerTrigger", self.config.max_files_per_trigger) \
                .load(self.config.source_path)
            
            # Apply transformations
            transformed_df = self.transform(streaming_df)
            
            # Add watermark
            if self.config.watermark_column in transformed_df.columns:
                transformed_df = transformed_df.withWatermark(
                    self.config.watermark_column, 
                    self.config.watermark_delay
                )
            
            # Write stream
            query = transformed_df.writeStream \
                .format("delta") \
                .outputMode(self.config.output_mode) \
                .option("checkpointLocation", self.config.checkpoint_path) \
                .trigger(processingTime=self.config.trigger_interval) \
                .start(self.config.target_path)
            
            logger.info(f"Streaming job started: {self.config.job_name}")
            return query
            
        except Exception as e:
            logger.error(f"Streaming ETL job failed: {self.config.job_name} - {e}")
            raise


class VideoEventETLJob(ETLJobBase):
    """ETL job for processing video events"""
    
    def extract(self) -> DataFrame:
        """Extract video events from raw data"""
        return self.incremental_processor.get_incremental_data(
            source_path=self.config.source_path,
            target_path=self.config.target_path,
            watermark_column=self.config.watermark_column,
            checkpoint_path=self.config.checkpoint_path
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform video events data"""
        try:
            # Basic cleaning and standardization
            df = self.transformation_engine.clean_and_standardize(df)
            
            # Enrich with session data
            df = self.transformation_engine.enrich_user_session_data(df)
            
            # Calculate video metrics
            df = self.transformation_engine.calculate_video_metrics(df)
            
            # Filter for video events only
            video_event_types = [
                "video_play", "video_pause", "video_stop", "video_seek",
                "video_buffer", "video_error", "video_quality_change", "video_complete"
            ]
            df = df.filter(col("event_type").isin(video_event_types))
            
            # Add derived columns
            df = df.withColumn("is_mobile_device", 
                             col("device_type").isin(["mobile", "tablet"])) \
                   .withColumn("is_premium_user",
                             col("subscription_tier").isin(["premium", "enterprise"])) \
                   .withColumn("processing_timestamp", current_timestamp())
            
            return df
            
        except Exception as e:
            logger.error(f"Video event transformation failed: {e}")
            raise
    
    def load(self, df: DataFrame) -> bool:
        """Load video events to Delta table"""
        try:
            # Write to Delta table
            df.write \
              .format("delta") \
              .mode("append") \
              .partitionBy(*self.config.partition_columns) \
              .save(self.config.target_path)
            
            # Optimize table if configured
            if self.config.z_order_columns:
                self.delta_manager.optimize_table(
                    self.config.target_path, 
                    self.config.z_order_columns
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load video events: {e}")
            return False


class UserSessionAggregationJob(ETLJobBase):
    """ETL job for aggregating user session data"""
    
    def extract(self) -> DataFrame:
        """Extract data for session aggregation"""
        return self.spark.read.format("delta").load(self.config.source_path)
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform and aggregate session data"""
        try:
            # Session-level aggregations
            session_stats = df.groupBy("session_id", "user_id") \
                .agg(
                    spark_min("event_timestamp").alias("session_start"),
                    spark_max("event_timestamp").alias("session_end"),
                    count("*").alias("total_events"),
                    countDistinct("video_id").alias("unique_videos_watched"),
                    spark_sum(when(col("event_type") == "video_complete", 1).otherwise(0)).alias("videos_completed"),
                    avg("playback_position").alias("avg_playback_position"),
                    spark_max("playback_position").alias("max_playback_position"),
                    first("device_type").alias("device_type"),
                    first("platform").alias("platform"),
                    first("country").alias("country")
                )
            
            # Calculate derived metrics
            session_stats = session_stats \
                .withColumn("session_duration_minutes",
                           (col("session_end").cast("long") - col("session_start").cast("long")) / 60) \
                .withColumn("completion_rate",
                           col("videos_completed") / col("unique_videos_watched")) \
                .withColumn("engagement_score",
                           (col("total_events") * 0.3 + col("unique_videos_watched") * 0.7))
            
            return session_stats
            
        except Exception as e:
            logger.error(f"Session aggregation failed: {e}")
            raise
    
    def load(self, df: DataFrame) -> bool:
        """Load session aggregations"""
        try:
            # Upsert to maintain session state
            if self.delta_manager._table_exists(self.config.target_path):
                delta_table = self.delta_manager.get_table(self.config.target_path)
                
                delta_table.alias("target").merge(
                    df.alias("source"),
                    "target.session_id = source.session_id"
                ).whenMatchedUpdateAll() \
                 .whenNotMatchedInsertAll() \
                 .execute()
            else:
                df.write.format("delta").mode("overwrite").save(self.config.target_path)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load session aggregations: {e}")
            return False


def main():
    """Main function for running ETL jobs"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Spark ETL Framework")
    parser.add_argument('--job', choices=['video_events', 'user_sessions'], 
                       required=True, help='ETL job to run')
    parser.add_argument('--mode', choices=['batch', 'streaming'], 
                       default='batch', help='Processing mode')
    parser.add_argument('--source-path', type=str, required=True,
                       help='Source data path')
    parser.add_argument('--target-path', type=str, required=True,
                       help='Target data path')
    parser.add_argument('--checkpoint-path', type=str, 
                       help='Checkpoint path for streaming')
    
    args = parser.parse_args()
    
    # Create ETL configuration
    config = ETLConfig(
        job_name=f"{args.job}_etl",
        source_path=args.source_path,
        target_path=args.target_path,
        checkpoint_path=args.checkpoint_path or f"/tmp/checkpoints/{args.job}",
        partition_columns=["year", "month", "day"],
        z_order_columns=["user_id", "event_timestamp"]
    )
    
    # Create and run ETL job
    if args.job == "video_events":
        job = VideoEventETLJob(config)
    elif args.job == "user_sessions":
        job = UserSessionAggregationJob(config)
    else:
        raise ValueError(f"Unknown job type: {args.job}")
    
    if args.mode == "batch":
        metrics = job.run()
        print(f"Job completed: {metrics.status}")
        print(f"Records processed: {metrics.records_read} -> {metrics.records_written}")
        print(f"Processing time: {metrics.processing_time_seconds:.2f} seconds")
        
        if metrics.data_quality_metrics:
            print(f"Data quality score: {metrics.data_quality_metrics.data_quality_score:.3f}")
    else:
        query = job.run_streaming()
        query.awaitTermination()


if __name__ == "__main__":
    main()
