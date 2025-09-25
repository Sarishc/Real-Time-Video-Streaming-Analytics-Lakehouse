"""
Centralized Configuration Management for Video Streaming Lakehouse

This module provides a centralized configuration system with environment-specific
settings, secret management, and validation.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from pathlib import Path
from datetime import timedelta
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Kafka configuration settings"""
    bootstrap_servers: List[str] = field(default_factory=lambda: ['localhost:9092'])
    security_protocol: str = 'PLAINTEXT'
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    
    # Producer settings
    producer_batch_size: int = 16384
    producer_linger_ms: int = 10
    producer_compression_type: str = 'gzip'
    producer_max_request_size: int = 1048576
    producer_retries: int = 3
    producer_acks: str = 'all'
    
    # Consumer settings
    consumer_group_id: str = 'video-streaming-consumer'
    consumer_auto_offset_reset: str = 'earliest'
    consumer_enable_auto_commit: bool = False
    consumer_max_poll_records: int = 500
    consumer_session_timeout_ms: int = 30000
    consumer_heartbeat_interval_ms: int = 3000
    
    # Topic settings
    topics: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'video_events': {
            'partitions': 12,
            'replication_factor': 3,
            'cleanup_policy': 'delete',
            'retention_ms': 7 * 24 * 60 * 60 * 1000  # 7 days
        },
        'user_interactions': {
            'partitions': 8,
            'replication_factor': 3,
            'cleanup_policy': 'delete',
            'retention_ms': 14 * 24 * 60 * 60 * 1000  # 14 days
        },
        'ad_events': {
            'partitions': 6,
            'replication_factor': 3,
            'cleanup_policy': 'delete',
            'retention_ms': 30 * 24 * 60 * 60 * 1000  # 30 days
        },
        'session_events': {
            'partitions': 4,
            'replication_factor': 3,
            'cleanup_policy': 'delete',
            'retention_ms': 90 * 24 * 60 * 60 * 1000  # 90 days
        }
    })


@dataclass
class SparkConfig:
    """Apache Spark configuration settings"""
    app_name: str = 'VideoStreamingLakehouse'
    master: str = 'local[*]'
    
    # Spark SQL settings
    sql_adaptive_enabled: bool = True
    sql_adaptive_coalesce_partitions_enabled: bool = True
    sql_adaptive_skew_join_enabled: bool = True
    
    # Memory settings
    driver_memory: str = '4g'
    driver_max_result_size: str = '2g'
    executor_memory: str = '4g'
    executor_cores: int = 2
    executor_instances: int = 2
    
    # Shuffle settings
    sql_shuffle_partitions: int = 200
    serializer: str = 'org.apache.spark.serializer.KryoSerializer'
    
    # Delta Lake settings
    delta_log_cache_size: int = 1000
    delta_checkpoint_interval: int = 10
    
    # Streaming settings
    streaming_trigger_interval: str = '10 seconds'
    streaming_checkpoint_location: str = '/tmp/checkpoint'
    streaming_trigger_available_now: bool = False


@dataclass
class AWSConfig:
    """AWS configuration settings"""
    region: str = 'us-west-2'
    profile: Optional[str] = None
    
    # S3 settings
    s3_bucket: str = 'video-streaming-lakehouse'
    s3_raw_prefix: str = 'raw-data'
    s3_processed_prefix: str = 'processed-data'
    s3_delta_prefix: str = 'delta-tables'
    s3_checkpoint_prefix: str = 'checkpoints'
    
    # Kinesis settings (alternative to Kafka)
    kinesis_stream_name: str = 'video-streaming-events'
    kinesis_shard_count: int = 10
    kinesis_retention_hours: int = 168  # 7 days
    
    # Secrets Manager
    secrets_manager_enabled: bool = True
    secret_name: str = 'video-streaming-lakehouse/secrets'


@dataclass
class SnowflakeConfig:
    """Snowflake configuration settings"""
    account: str = ''
    user: str = ''
    password: str = ''
    warehouse: str = 'COMPUTE_WH'
    database: str = 'VIDEO_STREAMING_DB'
    schema: str = 'ANALYTICS'
    role: str = 'ACCOUNTADMIN'
    
    # Connection settings
    connection_timeout: int = 60
    network_timeout: int = 300
    
    # Performance settings
    warehouse_size: str = 'MEDIUM'
    auto_suspend: int = 300  # seconds
    auto_resume: bool = True
    
    # Data loading settings
    file_format: str = 'PARQUET'
    compression: str = 'GZIP'
    stage_name: str = 'S3_STAGE'


@dataclass
class DeltaLakeConfig:
    """Delta Lake configuration settings"""
    # Table locations
    raw_table_path: str = 's3a://video-streaming-lakehouse/delta-tables/raw/'
    bronze_table_path: str = 's3a://video-streaming-lakehouse/delta-tables/bronze/'
    silver_table_path: str = 's3a://video-streaming-lakehouse/delta-tables/silver/'
    gold_table_path: str = 's3a://video-streaming-lakehouse/delta-tables/gold/'
    
    # Optimization settings
    auto_optimize_enabled: bool = True
    z_order_columns: Dict[str, List[str]] = field(default_factory=lambda: {
        'video_events': ['user_id', 'video_id', 'timestamp'],
        'user_interactions': ['user_id', 'content_id', 'timestamp'],
        'ad_events': ['user_id', 'ad_id', 'timestamp'],
        'session_events': ['user_id', 'session_id', 'timestamp']
    })
    
    # Maintenance settings
    vacuum_retention_hours: int = 168  # 7 days
    optimize_file_size_mb: int = 128
    checkpoint_interval: int = 10


@dataclass
class MonitoringConfig:
    """Monitoring and alerting configuration"""
    # Prometheus settings
    prometheus_port: int = 8000
    prometheus_enabled: bool = True
    
    # Grafana settings
    grafana_url: str = 'http://localhost:3000'
    grafana_api_key: str = ''
    
    # Alerting settings
    slack_webhook_url: str = ''
    email_smtp_server: str = 'smtp.gmail.com'
    email_smtp_port: int = 587
    email_username: str = ''
    email_password: str = ''
    alert_recipients: List[str] = field(default_factory=list)
    
    # Health check settings
    health_check_interval: int = 60  # seconds
    data_quality_threshold: float = 0.95
    pipeline_latency_threshold: int = 300  # seconds


@dataclass
class DashboardConfig:
    """Dashboard and BI configuration"""
    # Tableau settings
    tableau_server_url: str = ''
    tableau_username: str = ''
    tableau_password: str = ''
    tableau_site_id: str = 'default'
    
    # Power BI settings
    powerbi_tenant_id: str = ''
    powerbi_client_id: str = ''
    powerbi_client_secret: str = ''
    powerbi_workspace_id: str = ''
    
    # Report settings
    automated_reports_enabled: bool = True
    report_schedule: str = '0 8 * * *'  # Daily at 8 AM
    report_recipients: List[str] = field(default_factory=list)


class ConfigManager:
    """
    Centralized configuration manager with environment-specific settings,
    secret management, and validation.
    """
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.getenv('ENVIRONMENT', 'development')
        self.config_dir = Path(__file__).parent
        self.secrets_cache = {}
        
        # Load base configuration
        self._load_configuration()
        self._validate_configuration()
        
        logger.info(f"Configuration loaded for environment: {self.environment}")
    
    def _load_configuration(self):
        """Load configuration from YAML files and environment variables"""
        # Load environment-specific config
        config_file = self.config_dir / f"{self.environment}.yaml"
        
        if config_file.exists():
            with open(config_file, 'r') as f:
                config_data = yaml.safe_load(f)
        else:
            logger.warning(f"Configuration file not found: {config_file}")
            config_data = {}
        
        # Initialize configuration objects
        self.kafka = KafkaConfig(**config_data.get('kafka', {}))
        self.spark = SparkConfig(**config_data.get('spark', {}))
        self.aws = AWSConfig(**config_data.get('aws', {}))
        self.snowflake = SnowflakeConfig(**config_data.get('snowflake', {}))
        self.delta_lake = DeltaLakeConfig(**config_data.get('delta_lake', {}))
        self.monitoring = MonitoringConfig(**config_data.get('monitoring', {}))
        self.dashboard = DashboardConfig(**config_data.get('dashboard', {}))
        
        # Override with environment variables
        self._override_with_env_vars()
        
        # Load secrets if enabled
        if self.aws.secrets_manager_enabled:
            self._load_secrets_from_aws()
    
    def _override_with_env_vars(self):
        """Override configuration with environment variables"""
        env_mappings = {
            'KAFKA_BOOTSTRAP_SERVERS': ('kafka.bootstrap_servers', lambda x: x.split(',')),
            'SPARK_MASTER': ('spark.master', str),
            'AWS_REGION': ('aws.region', str),
            'S3_BUCKET': ('aws.s3_bucket', str),
            'SNOWFLAKE_ACCOUNT': ('snowflake.account', str),
            'SNOWFLAKE_USER': ('snowflake.user', str),
            'SNOWFLAKE_PASSWORD': ('snowflake.password', str),
            'SNOWFLAKE_WAREHOUSE': ('snowflake.warehouse', str),
            'SNOWFLAKE_DATABASE': ('snowflake.database', str),
            'MONITORING_SLACK_WEBHOOK': ('monitoring.slack_webhook_url', str),
        }
        
        for env_var, (config_path, converter) in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                self._set_nested_config(config_path, converter(value))
    
    def _set_nested_config(self, path: str, value: Any):
        """Set nested configuration value using dot notation"""
        parts = path.split('.')
        obj = self
        
        for part in parts[:-1]:
            obj = getattr(obj, part)
        
        setattr(obj, parts[-1], value)
    
    def _load_secrets_from_aws(self):
        """Load secrets from AWS Secrets Manager"""
        try:
            session = boto3.Session(
                region_name=self.aws.region,
                profile_name=self.aws.profile
            )
            secrets_client = session.client('secretsmanager')
            
            response = secrets_client.get_secret_value(
                SecretId=self.aws.secret_name
            )
            
            secrets = yaml.safe_load(response['SecretString'])
            self.secrets_cache = secrets
            
            # Apply secrets to configuration
            if 'snowflake' in secrets:
                for key, value in secrets['snowflake'].items():
                    setattr(self.snowflake, key, value)
            
            if 'monitoring' in secrets:
                for key, value in secrets['monitoring'].items():
                    setattr(self.monitoring, key, value)
            
            logger.info("Secrets loaded from AWS Secrets Manager")
            
        except ClientError as e:
            logger.warning(f"Failed to load secrets from AWS: {e}")
        except Exception as e:
            logger.error(f"Error loading secrets: {e}")
    
    def _validate_configuration(self):
        """Validate configuration settings"""
        validations = []
        
        # Validate required AWS settings
        if not self.aws.s3_bucket:
            validations.append("AWS S3 bucket name is required")
        
        # Validate Kafka settings
        if not self.kafka.bootstrap_servers:
            validations.append("Kafka bootstrap servers are required")
        
        # Validate Snowflake settings for production
        if self.environment == 'production':
            if not all([self.snowflake.account, self.snowflake.user]):
                validations.append("Snowflake account and user are required in production")
        
        if validations:
            error_msg = "Configuration validation failed:\n" + "\n".join(f"- {v}" for v in validations)
            raise ValueError(error_msg)
    
    def get_secret(self, key: str, default: Any = None) -> Any:
        """Get secret value from cache"""
        return self.secrets_cache.get(key, default)
    
    def get_spark_config(self) -> Dict[str, str]:
        """Get Spark configuration as dictionary"""
        return {
            'spark.app.name': self.spark.app_name,
            'spark.master': self.spark.master,
            'spark.sql.adaptive.enabled': str(self.spark.sql_adaptive_enabled),
            'spark.sql.adaptive.coalescePartitions.enabled': str(self.spark.sql_adaptive_coalesce_partitions_enabled),
            'spark.sql.adaptive.skewJoin.enabled': str(self.spark.sql_adaptive_skew_join_enabled),
            'spark.driver.memory': self.spark.driver_memory,
            'spark.driver.maxResultSize': self.spark.driver_max_result_size,
            'spark.executor.memory': self.spark.executor_memory,
            'spark.executor.cores': str(self.spark.executor_cores),
            'spark.executor.instances': str(self.spark.executor_instances),
            'spark.sql.shuffle.partitions': str(self.spark.sql_shuffle_partitions),
            'spark.serializer': self.spark.serializer,
            'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
            'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.DefaultAWSCredentialsProviderChain',
        }
    
    def get_kafka_producer_config(self) -> Dict[str, Any]:
        """Get Kafka producer configuration"""
        config = {
            'bootstrap_servers': self.kafka.bootstrap_servers,
            'security_protocol': self.kafka.security_protocol,
            'batch_size': self.kafka.producer_batch_size,
            'linger_ms': self.kafka.producer_linger_ms,
            'compression_type': self.kafka.producer_compression_type,
            'max_request_size': self.kafka.producer_max_request_size,
            'retries': self.kafka.producer_retries,
            'acks': self.kafka.producer_acks,
            'value_serializer': lambda x: x.encode('utf-8'),
            'key_serializer': lambda x: x.encode('utf-8') if x else None,
        }
        
        if self.kafka.sasl_mechanism:
            config.update({
                'sasl_mechanism': self.kafka.sasl_mechanism,
                'sasl_plain_username': self.kafka.sasl_username,
                'sasl_plain_password': self.kafka.sasl_password,
            })
        
        return config
    
    def get_kafka_consumer_config(self) -> Dict[str, Any]:
        """Get Kafka consumer configuration"""
        config = {
            'bootstrap_servers': self.kafka.bootstrap_servers,
            'security_protocol': self.kafka.security_protocol,
            'group_id': self.kafka.consumer_group_id,
            'auto_offset_reset': self.kafka.consumer_auto_offset_reset,
            'enable_auto_commit': self.kafka.consumer_enable_auto_commit,
            'max_poll_records': self.kafka.consumer_max_poll_records,
            'session_timeout_ms': self.kafka.consumer_session_timeout_ms,
            'heartbeat_interval_ms': self.kafka.consumer_heartbeat_interval_ms,
            'value_deserializer': lambda x: x.decode('utf-8'),
            'key_deserializer': lambda x: x.decode('utf-8') if x else None,
        }
        
        if self.kafka.sasl_mechanism:
            config.update({
                'sasl_mechanism': self.kafka.sasl_mechanism,
                'sasl_plain_username': self.kafka.sasl_username,
                'sasl_plain_password': self.kafka.sasl_password,
            })
        
        return config
    
    def get_snowflake_connection_params(self) -> Dict[str, Any]:
        """Get Snowflake connection parameters"""
        return {
            'account': self.snowflake.account,
            'user': self.snowflake.user,
            'password': self.snowflake.password,
            'warehouse': self.snowflake.warehouse,
            'database': self.snowflake.database,
            'schema': self.snowflake.schema,
            'role': self.snowflake.role,
            'connection_timeout': self.snowflake.connection_timeout,
            'network_timeout': self.snowflake.network_timeout,
        }
    
    def __str__(self) -> str:
        """String representation of configuration"""
        return f"ConfigManager(environment={self.environment})"
    
    def __repr__(self) -> str:
        return self.__str__()


# Global configuration instance
config = ConfigManager()

# Convenience functions
def get_config() -> ConfigManager:
    """Get the global configuration instance"""
    return config

def reload_config(environment: str = None) -> ConfigManager:
    """Reload configuration with optional environment override"""
    global config
    config = ConfigManager(environment)
    return config
