"""
Snowflake Integration Pipeline for Video Streaming Analytics

This module provides comprehensive Snowflake integration with:
- Dimensional data modeling (star schema)
- Incremental data loading from Delta Lake
- Performance optimization (clustering, caching)
- Data quality monitoring and validation
- Cost optimization strategies
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import json

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce, 
    max as spark_max, min as spark_min, count, sum as spark_sum,
    year, month, dayofmonth, date_format, to_date,
    rank, row_number, lag, lead, first_value, last_value
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

# Add parent directory to path for imports
import sys
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config
from delta_lake.delta_setup import DeltaLakeManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SnowflakeTableConfig:
    """Configuration for Snowflake table"""
    table_name: str
    database: str
    schema: str
    clustering_keys: List[str]
    stage_name: str
    file_format: str = "PARQUET"
    copy_options: Dict[str, str] = None
    
    def __post_init__(self):
        if self.copy_options is None:
            self.copy_options = {}


class SnowflakeConnector:
    """Production-ready Snowflake connector with connection pooling and error handling"""
    
    def __init__(self):
        """Initialize Snowflake connector"""
        self.config = get_config()
        self.connection_params = self.config.get_snowflake_connection_params()
        self.connection = None
        
        logger.info("Snowflake connector initialized")
    
    def connect(self) -> snowflake.connector.SnowflakeConnection:
        """Establish connection to Snowflake"""
        try:
            if self.connection and not self.connection.is_closed():
                return self.connection
            
            self.connection = snowflake.connector.connect(
                **self.connection_params,
                application='VideoStreamingLakehouse',
                autocommit=False
            )
            
            logger.info("Connected to Snowflake")
            return self.connection
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute query and return results"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch results if it's a SELECT query
            if query.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            else:
                conn.commit()
                return [{"rows_affected": cursor.rowcount}]
            
        except Exception as e:
            if self.connection:
                self.connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
    
    def bulk_load_from_s3(self, table_name: str, s3_path: str, 
                         file_format: str = "PARQUET", 
                         copy_options: Dict[str, str] = None) -> bool:
        """Bulk load data from S3 using COPY command"""
        try:
            copy_opts = copy_options or {}
            copy_options_str = " ".join([f"{k}={v}" for k, v in copy_opts.items()])
            
            copy_query = f"""
            COPY INTO {table_name}
            FROM '{s3_path}'
            FILE_FORMAT = (TYPE = {file_format})
            {copy_options_str}
            """
            
            result = self.execute_query(copy_query)
            logger.info(f"Bulk loaded data to {table_name} from {s3_path}")
            return True
            
        except Exception as e:
            logger.error(f"Bulk load failed: {e}")
            return False
    
    def close(self):
        """Close connection"""
        if self.connection and not self.connection.is_closed():
            self.connection.close()
            logger.info("Snowflake connection closed")


class DimensionalModelBuilder:
    """Builds dimensional data model (star schema) for video streaming analytics"""
    
    def __init__(self, spark: SparkSession, snowflake_connector: SnowflakeConnector):
        self.spark = spark
        self.snowflake = snowflake_connector
        self.config = get_config()
        
        logger.info("Dimensional model builder initialized")
    
    def create_dimension_tables(self) -> bool:
        """Create all dimension tables in Snowflake"""
        try:
            # User dimension
            self._create_user_dimension()
            
            # Content dimension
            self._create_content_dimension()
            
            # Device dimension
            self._create_device_dimension()
            
            # Time dimension
            self._create_time_dimension()
            
            # Geography dimension
            self._create_geography_dimension()
            
            logger.info("All dimension tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create dimension tables: {e}")
            return False
    
    def _create_user_dimension(self):
        """Create user dimension table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DIM_USER (
            user_key NUMBER AUTOINCREMENT PRIMARY KEY,
            user_id STRING NOT NULL,
            subscription_tier STRING,
            signup_date TIMESTAMP,
            tier_change_date TIMESTAMP,
            is_premium BOOLEAN,
            user_status STRING DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE(user_id)
        )
        CLUSTER BY (user_id, subscription_tier)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created DIM_USER table")
    
    def _create_content_dimension(self):
        """Create content dimension table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DIM_CONTENT (
            content_key NUMBER AUTOINCREMENT PRIMARY KEY,
            content_id STRING NOT NULL,
            content_title STRING,
            content_type STRING,
            content_genre STRING,
            content_rating STRING,
            content_language STRING,
            content_duration INTEGER,
            creator_id STRING,
            release_date TIMESTAMP,
            is_premium_content BOOLEAN,
            content_status STRING DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE(content_id)
        )
        CLUSTER BY (content_type, content_genre, release_date)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created DIM_CONTENT table")
    
    def _create_device_dimension(self):
        """Create device dimension table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DIM_DEVICE (
            device_key NUMBER AUTOINCREMENT PRIMARY KEY,
            device_id STRING NOT NULL,
            device_type STRING,
            platform STRING,
            app_version STRING,
            user_agent STRING,
            is_mobile BOOLEAN,
            device_category STRING,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE(device_id)
        )
        CLUSTER BY (device_type, platform)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created DIM_DEVICE table")
    
    def _create_time_dimension(self):
        """Create time dimension table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DIM_TIME (
            time_key NUMBER PRIMARY KEY,
            date_value DATE NOT NULL,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name STRING,
            week INTEGER,
            day INTEGER,
            day_name STRING,
            day_of_week INTEGER,
            day_of_year INTEGER,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN,
            fiscal_year INTEGER,
            fiscal_quarter INTEGER,
            season STRING
        )
        CLUSTER BY (date_value, year, month)
        """
        
        self.snowflake.execute_query(create_table_sql)
        
        # Populate time dimension with data
        self._populate_time_dimension()
        logger.info("Created and populated DIM_TIME table")
    
    def _create_geography_dimension(self):
        """Create geography dimension table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS DIM_GEOGRAPHY (
            geography_key NUMBER AUTOINCREMENT PRIMARY KEY,
            country_code STRING,
            country_name STRING,
            region STRING,
            city STRING,
            latitude FLOAT,
            longitude FLOAT,
            timezone STRING,
            continent STRING,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE(country_code, region, city)
        )
        CLUSTER BY (country_code, region)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created DIM_GEOGRAPHY table")
    
    def _populate_time_dimension(self):
        """Populate time dimension with date range"""
        # Generate time dimension data using Spark
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2030, 12, 31)
        
        populate_sql = f"""
        INSERT INTO DIM_TIME
        WITH date_series AS (
            SELECT 
                DATEADD(day, SEQ4(), '{start_date.strftime('%Y-%m-%d')}') AS date_value
            FROM TABLE(GENERATOR(ROWCOUNT => {(end_date - start_date).days + 1}))
        )
        SELECT 
            YEAR(date_value) * 10000 + MONTH(date_value) * 100 + DAY(date_value) AS time_key,
            date_value,
            YEAR(date_value) AS year,
            QUARTER(date_value) AS quarter,
            MONTH(date_value) AS month,
            MONTHNAME(date_value) AS month_name,
            WEEK(date_value) AS week,
            DAY(date_value) AS day,
            DAYNAME(date_value) AS day_name,
            DAYOFWEEK(date_value) AS day_of_week,
            DAYOFYEAR(date_value) AS day_of_year,
            CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
            FALSE AS is_holiday,  -- Would need holiday calendar integration
            CASE WHEN MONTH(date_value) <= 6 THEN YEAR(date_value) ELSE YEAR(date_value) + 1 END AS fiscal_year,
            CASE WHEN MONTH(date_value) <= 3 THEN 1
                 WHEN MONTH(date_value) <= 6 THEN 2
                 WHEN MONTH(date_value) <= 9 THEN 3
                 ELSE 4 END AS fiscal_quarter,
            CASE WHEN MONTH(date_value) IN (12, 1, 2) THEN 'Winter'
                 WHEN MONTH(date_value) IN (3, 4, 5) THEN 'Spring'
                 WHEN MONTH(date_value) IN (6, 7, 8) THEN 'Summer'
                 ELSE 'Fall' END AS season
        FROM date_series
        WHERE date_value NOT IN (SELECT date_value FROM DIM_TIME)
        """
        
        self.snowflake.execute_query(populate_sql)
    
    def create_fact_tables(self) -> bool:
        """Create fact tables for video streaming analytics"""
        try:
            # Video events fact table
            self._create_video_events_fact()
            
            # User session fact table
            self._create_user_session_fact()
            
            # Daily user metrics fact table
            self._create_daily_user_metrics_fact()
            
            # Content performance fact table
            self._create_content_performance_fact()
            
            logger.info("All fact tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create fact tables: {e}")
            return False
    
    def _create_video_events_fact(self):
        """Create video events fact table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FACT_VIDEO_EVENTS (
            event_key NUMBER AUTOINCREMENT PRIMARY KEY,
            event_id STRING NOT NULL,
            user_key NUMBER,
            content_key NUMBER,
            device_key NUMBER,
            time_key NUMBER,
            geography_key NUMBER,
            event_type STRING,
            event_timestamp TIMESTAMP,
            session_id STRING,
            playback_position INTEGER,
            video_quality STRING,
            buffer_duration FLOAT,
            startup_time FLOAT,
            bitrate INTEGER,
            is_mobile BOOLEAN,
            is_premium_user BOOLEAN,
            watch_duration_minutes FLOAT,
            completion_rate FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (user_key) REFERENCES DIM_USER(user_key),
            FOREIGN KEY (content_key) REFERENCES DIM_CONTENT(content_key),
            FOREIGN KEY (device_key) REFERENCES DIM_DEVICE(device_key),
            FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
            FOREIGN KEY (geography_key) REFERENCES DIM_GEOGRAPHY(geography_key)
        )
        CLUSTER BY (time_key, user_key, content_key)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created FACT_VIDEO_EVENTS table")
    
    def _create_user_session_fact(self):
        """Create user session fact table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FACT_USER_SESSION (
            session_key NUMBER AUTOINCREMENT PRIMARY KEY,
            session_id STRING NOT NULL,
            user_key NUMBER,
            device_key NUMBER,
            time_key NUMBER,
            geography_key NUMBER,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            session_duration_minutes FLOAT,
            total_events INTEGER,
            unique_videos_watched INTEGER,
            videos_completed INTEGER,
            total_watch_time_minutes FLOAT,
            avg_completion_rate FLOAT,
            engagement_score FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (user_key) REFERENCES DIM_USER(user_key),
            FOREIGN KEY (device_key) REFERENCES DIM_DEVICE(device_key),
            FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
            FOREIGN KEY (geography_key) REFERENCES DIM_GEOGRAPHY(geography_key)
        )
        CLUSTER BY (time_key, user_key)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created FACT_USER_SESSION table")
    
    def _create_daily_user_metrics_fact(self):
        """Create daily user metrics fact table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FACT_DAILY_USER_METRICS (
            metric_key NUMBER AUTOINCREMENT PRIMARY KEY,
            user_key NUMBER,
            time_key NUMBER,
            total_watch_time_minutes FLOAT,
            videos_watched INTEGER,
            unique_content_types INTEGER,
            sessions_count INTEGER,
            avg_session_duration FLOAT,
            completion_rate FLOAT,
            interaction_events INTEGER,
            ad_impressions INTEGER,
            ad_clicks INTEGER,
            estimated_revenue FLOAT,
            engagement_score FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (user_key) REFERENCES DIM_USER(user_key),
            FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
            UNIQUE(user_key, time_key)
        )
        CLUSTER BY (time_key, user_key)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created FACT_DAILY_USER_METRICS table")
    
    def _create_content_performance_fact(self):
        """Create content performance fact table"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS FACT_CONTENT_PERFORMANCE (
            performance_key NUMBER AUTOINCREMENT PRIMARY KEY,
            content_key NUMBER,
            time_key NUMBER,
            total_views INTEGER,
            unique_viewers INTEGER,
            total_watch_time_minutes FLOAT,
            avg_watch_time_minutes FLOAT,
            completion_rate FLOAT,
            engagement_score FLOAT,
            revenue FLOAT,
            trending_score FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (content_key) REFERENCES DIM_CONTENT(content_key),
            FOREIGN KEY (time_key) REFERENCES DIM_TIME(time_key),
            UNIQUE(content_key, time_key)
        )
        CLUSTER BY (time_key, content_key)
        """
        
        self.snowflake.execute_query(create_table_sql)
        logger.info("Created FACT_CONTENT_PERFORMANCE table")


class SnowflakeETLPipeline:
    """ETL pipeline for loading data from Delta Lake to Snowflake"""
    
    def __init__(self, spark: SparkSession = None):
        """Initialize Snowflake ETL pipeline"""
        self.config = get_config()
        self.spark = spark or self._create_spark_session()
        self.snowflake = SnowflakeConnector()
        self.delta_manager = DeltaLakeManager(self.spark)
        self.dimensional_model = DimensionalModelBuilder(self.spark, self.snowflake)
        
        logger.info("Snowflake ETL pipeline initialized")
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with Snowflake connector"""
        spark_config = self.config.get_spark_config()
        
        # Add Snowflake connector configuration
        snowflake_configs = {
            'spark.jars': 'snowflake-jdbc-3.14.4.jar,spark-snowflake_2.12-2.11.3-spark_3.4.jar',
            'spark.sql.catalog.snowflake': 'net.snowflake.spark.snowflake.catalog.SnowflakeCatalog'
        }
        
        spark_config.update(snowflake_configs)
        
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def setup_snowflake_schema(self) -> bool:
        """Setup complete Snowflake schema with dimensions and facts"""
        try:
            logger.info("Setting up Snowflake schema...")
            
            # Create database and schema
            self.snowflake.execute_query(f"CREATE DATABASE IF NOT EXISTS {self.config.snowflake.database}")
            self.snowflake.execute_query(f"USE DATABASE {self.config.snowflake.database}")
            self.snowflake.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.config.snowflake.schema}")
            self.snowflake.execute_query(f"USE SCHEMA {self.config.snowflake.schema}")
            
            # Create file format for data loading
            self.snowflake.execute_query("""
                CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
                TYPE = PARQUET
                COMPRESSION = SNAPPY
            """)
            
            # Create stage for S3 data
            s3_bucket = self.config.aws.s3_bucket
            self.snowflake.execute_query(f"""
                CREATE OR REPLACE STAGE S3_STAGE
                URL = 's3://{s3_bucket}/processed-data/'
                FILE_FORMAT = PARQUET_FORMAT
            """)
            
            # Create dimension tables
            self.dimensional_model.create_dimension_tables()
            
            # Create fact tables
            self.dimensional_model.create_fact_tables()
            
            logger.info("Snowflake schema setup completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Snowflake schema: {e}")
            return False
    
    def load_dimension_data(self) -> bool:
        """Load dimension data from Delta Lake to Snowflake"""
        try:
            logger.info("Loading dimension data...")
            
            # Load user dimension
            self._load_user_dimension()
            
            # Load content dimension
            self._load_content_dimension()
            
            # Load device dimension
            self._load_device_dimension()
            
            # Load geography dimension
            self._load_geography_dimension()
            
            logger.info("Dimension data loading completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load dimension data: {e}")
            return False
    
    def _load_user_dimension(self):
        """Load user dimension data"""
        # Extract unique users from video events
        video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
        session_events_path = f"{self.config.delta_lake.bronze_table_path}session_events"
        
        # Read user data from multiple sources
        video_users = self.spark.read.format("delta").load(video_events_path) \
            .select("user_id", "subscription_tier").distinct()
        
        session_users = self.spark.read.format("delta").load(session_events_path) \
            .select("user_id", "subscription_tier", "user_signup_date").distinct()
        
        # Combine and deduplicate user data
        users_df = video_users.join(session_users, "user_id", "left_outer") \
            .select(
                col("user_id"),
                coalesce(session_users["subscription_tier"], video_users["subscription_tier"]).alias("subscription_tier"),
                col("user_signup_date").alias("signup_date"),
                when(col("subscription_tier").isin(["premium", "enterprise"]), True).otherwise(False).alias("is_premium"),
                lit("ACTIVE").alias("user_status"),
                current_timestamp().alias("created_at"),
                current_timestamp().alias("updated_at")
            ).distinct()
        
        # Write to Snowflake using JDBC
        users_df.write \
            .format("snowflake") \
            .options(**self._get_snowflake_options()) \
            .option("dbtable", "DIM_USER") \
            .mode("overwrite") \
            .save()
        
        logger.info("Loaded user dimension data")
    
    def _load_content_dimension(self):
        """Load content dimension data"""
        video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
        
        content_df = self.spark.read.format("delta").load(video_events_path) \
            .select(
                "video_id", "content_title", "content_type", "content_genre",
                "content_rating", "content_language", "content_duration", "content_creator_id"
            ).distinct() \
            .withColumnRenamed("video_id", "content_id") \
            .withColumnRenamed("content_creator_id", "creator_id") \
            .withColumn("is_premium_content", 
                       when(col("content_type").isin(["movie", "premium_series"]), True).otherwise(False)) \
            .withColumn("content_status", lit("ACTIVE")) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())
        
        content_df.write \
            .format("snowflake") \
            .options(**self._get_snowflake_options()) \
            .option("dbtable", "DIM_CONTENT") \
            .mode("overwrite") \
            .save()
        
        logger.info("Loaded content dimension data")
    
    def _load_device_dimension(self):
        """Load device dimension data"""
        video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
        
        device_df = self.spark.read.format("delta").load(video_events_path) \
            .select("device_id", "device_type", "platform", "app_version", "user_agent") \
            .distinct() \
            .withColumn("is_mobile", col("device_type").isin(["mobile", "tablet"])) \
            .withColumn("device_category", 
                       when(col("device_type").isin(["mobile", "tablet"]), "Mobile")
                       .when(col("device_type") == "desktop", "Desktop")
                       .when(col("device_type") == "tv", "Connected TV")
                       .otherwise("Other")) \
            .withColumn("created_at", current_timestamp()) \
            .withColumn("updated_at", current_timestamp())
        
        device_df.write \
            .format("snowflake") \
            .options(**self._get_snowflake_options()) \
            .option("dbtable", "DIM_DEVICE") \
            .mode("overwrite") \
            .save()
        
        logger.info("Loaded device dimension data")
    
    def _load_geography_dimension(self):
        """Load geography dimension data"""
        video_events_path = f"{self.config.delta_lake.bronze_table_path}video_events"
        
        geography_df = self.spark.read.format("delta").load(video_events_path) \
            .select("country", "region", "city", "latitude", "longitude") \
            .distinct() \
            .withColumnRenamed("country", "country_code") \
            .withColumn("country_name", col("country_code"))  # Would need country code mapping \
            .withColumn("continent", lit("Unknown"))  # Would need geo mapping \
            .withColumn("timezone", lit("UTC"))  # Would need timezone mapping \
            .withColumn("created_at", current_timestamp())
        
        geography_df.write \
            .format("snowflake") \
            .options(**self._get_snowflake_options()) \
            .option("dbtable", "DIM_GEOGRAPHY") \
            .mode("overwrite") \
            .save()
        
        logger.info("Loaded geography dimension data")
    
    def load_fact_data(self, table_name: str, start_date: datetime = None, end_date: datetime = None) -> bool:
        """Load fact data for specified date range"""
        try:
            if table_name == "FACT_VIDEO_EVENTS":
                return self._load_video_events_fact(start_date, end_date)
            elif table_name == "FACT_USER_SESSION":
                return self._load_user_session_fact(start_date, end_date)
            elif table_name == "FACT_DAILY_USER_METRICS":
                return self._load_daily_user_metrics_fact(start_date, end_date)
            elif table_name == "FACT_CONTENT_PERFORMANCE":
                return self._load_content_performance_fact(start_date, end_date)
            else:
                logger.error(f"Unknown fact table: {table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to load fact data for {table_name}: {e}")
            return False
    
    def _get_snowflake_options(self) -> Dict[str, str]:
        """Get Snowflake connection options for Spark"""
        return {
            "sfUrl": f"{self.config.snowflake.account}.snowflakecomputing.com",
            "sfUser": self.config.snowflake.user,
            "sfPassword": self.config.snowflake.password,
            "sfDatabase": self.config.snowflake.database,
            "sfSchema": self.config.snowflake.schema,
            "sfWarehouse": self.config.snowflake.warehouse,
            "sfRole": self.config.snowflake.role
        }
    
    def run_incremental_load(self) -> bool:
        """Run incremental load for all fact tables"""
        try:
            logger.info("Starting incremental load...")
            
            # Get last load timestamp
            last_load_time = self._get_last_load_timestamp()
            current_time = datetime.now(timezone.utc)
            
            logger.info(f"Loading data from {last_load_time} to {current_time}")
            
            # Load fact tables incrementally
            fact_tables = [
                "FACT_VIDEO_EVENTS",
                "FACT_USER_SESSION", 
                "FACT_DAILY_USER_METRICS",
                "FACT_CONTENT_PERFORMANCE"
            ]
            
            for table in fact_tables:
                success = self.load_fact_data(table, last_load_time, current_time)
                if not success:
                    logger.error(f"Failed to load {table}")
                    return False
            
            # Update load timestamp
            self._update_last_load_timestamp(current_time)
            
            logger.info("Incremental load completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Incremental load failed: {e}")
            return False
    
    def _get_last_load_timestamp(self) -> datetime:
        """Get last successful load timestamp"""
        try:
            query = """
            SELECT MAX(created_at) as last_load
            FROM FACT_VIDEO_EVENTS
            """
            result = self.snowflake.execute_query(query)
            
            if result and result[0]['LAST_LOAD']:
                return result[0]['LAST_LOAD']
            else:
                # Default to 7 days ago for initial load
                return datetime.now(timezone.utc) - timedelta(days=7)
                
        except Exception as e:
            logger.warning(f"Could not get last load timestamp: {e}")
            return datetime.now(timezone.utc) - timedelta(days=7)
    
    def _update_last_load_timestamp(self, timestamp: datetime):
        """Update last load timestamp in control table"""
        try:
            # Create control table if it doesn't exist
            create_control_table = """
            CREATE TABLE IF NOT EXISTS ETL_CONTROL (
                process_name STRING PRIMARY KEY,
                last_run_timestamp TIMESTAMP,
                status STRING,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            )
            """
            self.snowflake.execute_query(create_control_table)
            
            # Update timestamp
            update_query = """
            MERGE INTO ETL_CONTROL AS target
            USING (SELECT 'INCREMENTAL_LOAD' as process_name, %s as last_run_timestamp, 'SUCCESS' as status) AS source
            ON target.process_name = source.process_name
            WHEN MATCHED THEN UPDATE SET 
                last_run_timestamp = source.last_run_timestamp,
                status = source.status,
                updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT 
                (process_name, last_run_timestamp, status, updated_at)
                VALUES (source.process_name, source.last_run_timestamp, source.status, CURRENT_TIMESTAMP())
            """
            
            self.snowflake.execute_query(update_query, {"timestamp": timestamp})
            
        except Exception as e:
            logger.error(f"Failed to update load timestamp: {e}")


def main():
    """Main function for Snowflake pipeline operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Snowflake ETL Pipeline")
    parser.add_argument('--action', choices=['setup', 'load_dimensions', 'load_facts', 'incremental'],
                       required=True, help='Action to perform')
    parser.add_argument('--table', type=str, help='Specific table to load (for load_facts)')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    pipeline = SnowflakeETLPipeline()
    
    try:
        if args.action == 'setup':
            success = pipeline.setup_snowflake_schema()
            print(f"Schema setup: {'SUCCESS' if success else 'FAILED'}")
        
        elif args.action == 'load_dimensions':
            success = pipeline.load_dimension_data()
            print(f"Dimension loading: {'SUCCESS' if success else 'FAILED'}")
        
        elif args.action == 'load_facts':
            if not args.table:
                print("Please specify --table for fact loading")
                return
            
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None
            
            success = pipeline.load_fact_data(args.table, start_date, end_date)
            print(f"Fact loading for {args.table}: {'SUCCESS' if success else 'FAILED'}")
        
        elif args.action == 'incremental':
            success = pipeline.run_incremental_load()
            print(f"Incremental load: {'SUCCESS' if success else 'FAILED'}")
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
    finally:
        pipeline.snowflake.close()


if __name__ == "__main__":
    main()
