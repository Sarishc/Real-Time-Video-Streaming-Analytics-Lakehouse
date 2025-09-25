"""
Kafka Topic Management and Configuration

This module provides comprehensive Kafka topic management including:
- Topic creation and configuration
- Partition and replication management
- Topic monitoring and health checks
- Schema registry integration
- Topic lifecycle management
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone
import json

from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType
from kafka.admin.config_resource import ConfigResource
from kafka.admin.new_topic import NewTopic
from kafka.admin.new_partitions import NewPartitions
from kafka.errors import TopicAlreadyExistsError, KafkaError, UnknownTopicOrPartitionError
from kafka import KafkaConsumer, KafkaProducer
import requests

# Add parent directory to path for imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TopicConfig:
    """Topic configuration settings"""
    name: str
    num_partitions: int
    replication_factor: int
    config: Dict[str, str]
    description: str = ""
    
    def to_new_topic(self) -> NewTopic:
        """Convert to Kafka NewTopic object"""
        return NewTopic(
            name=self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            topic_configs=self.config
        )


@dataclass
class TopicMetrics:
    """Topic metrics and health information"""
    name: str
    partition_count: int
    replication_factor: int
    message_count: Optional[int] = None
    size_bytes: Optional[int] = None
    retention_ms: Optional[int] = None
    last_modified: Optional[datetime] = None
    is_healthy: bool = True
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class SchemaRegistry:
    """Simple schema registry client for Kafka topics"""
    
    def __init__(self, schema_registry_url: Optional[str] = None):
        self.schema_registry_url = schema_registry_url
        self.schemas = {}  # In-memory schema storage for development
        
        if schema_registry_url:
            logger.info(f"Schema registry configured: {schema_registry_url}")
        else:
            logger.info("Using in-memory schema storage")
    
    def register_schema(self, subject: str, schema: Dict[str, Any]) -> int:
        """Register schema for a subject"""
        if self.schema_registry_url:
            return self._register_schema_remote(subject, schema)
        else:
            return self._register_schema_local(subject, schema)
    
    def get_schema(self, subject: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get schema for a subject"""
        if self.schema_registry_url:
            return self._get_schema_remote(subject, version)
        else:
            return self._get_schema_local(subject, version)
    
    def _register_schema_remote(self, subject: str, schema: Dict[str, Any]) -> int:
        """Register schema with remote schema registry"""
        try:
            url = f"{self.schema_registry_url}/subjects/{subject}/versions"
            payload = {"schema": json.dumps(schema)}
            
            response = requests.post(url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            schema_id = result.get("id", 0)
            
            logger.info(f"Registered schema for subject {subject} with ID {schema_id}")
            return schema_id
            
        except Exception as e:
            logger.error(f"Failed to register schema for {subject}: {e}")
            raise
    
    def _register_schema_local(self, subject: str, schema: Dict[str, Any]) -> int:
        """Register schema locally"""
        if subject not in self.schemas:
            self.schemas[subject] = {}
        
        version = len(self.schemas[subject]) + 1
        schema_id = hash(json.dumps(schema, sort_keys=True)) % 1000000
        
        self.schemas[subject][str(version)] = {
            "id": schema_id,
            "schema": schema,
            "version": version,
            "registered_at": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Registered schema for subject {subject} with version {version}")
        return schema_id
    
    def _get_schema_remote(self, subject: str, version: str) -> Optional[Dict[str, Any]]:
        """Get schema from remote schema registry"""
        try:
            url = f"{self.schema_registry_url}/subjects/{subject}/versions/{version}"
            
            response = requests.get(url)
            response.raise_for_status()
            
            result = response.json()
            return json.loads(result.get("schema", "{}"))
            
        except Exception as e:
            logger.error(f"Failed to get schema for {subject}:{version}: {e}")
            return None
    
    def _get_schema_local(self, subject: str, version: str) -> Optional[Dict[str, Any]]:
        """Get schema from local storage"""
        if subject not in self.schemas:
            return None
        
        if version == "latest":
            if not self.schemas[subject]:
                return None
            latest_version = max(self.schemas[subject].keys(), key=int)
            return self.schemas[subject][latest_version]["schema"]
        
        return self.schemas[subject].get(version, {}).get("schema")


class KafkaTopicManager:
    """
    Comprehensive Kafka topic management with advanced features:
    - Topic creation with optimized configurations
    - Partition scaling and rebalancing
    - Topic monitoring and health checks
    - Schema management
    - Topic lifecycle operations
    """
    
    def __init__(self, schema_registry_url: Optional[str] = None):
        """Initialize Kafka topic manager"""
        self.config = get_config()
        self.admin_client = None
        self.schema_registry = SchemaRegistry(schema_registry_url)
        
        # Initialize admin client
        self._initialize_admin_client()
        
        # Define topic configurations for video streaming platform
        self.topic_configs = self._get_default_topic_configs()
        
        logger.info("Kafka topic manager initialized")
    
    def _initialize_admin_client(self):
        """Initialize Kafka admin client"""
        try:
            admin_config = {
                'bootstrap_servers': self.config.kafka.bootstrap_servers,
                'security_protocol': self.config.kafka.security_protocol,
                'request_timeout_ms': 30000,
                'api_version': (0, 10, 1)
            }
            
            if self.config.kafka.sasl_mechanism:
                admin_config.update({
                    'sasl_mechanism': self.config.kafka.sasl_mechanism,
                    'sasl_plain_username': self.config.kafka.sasl_username,
                    'sasl_plain_password': self.config.kafka.sasl_password
                })
            
            self.admin_client = KafkaAdminClient(**admin_config)
            logger.info("Kafka admin client initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka admin client: {e}")
            raise
    
    def _get_default_topic_configs(self) -> Dict[str, TopicConfig]:
        """Get default topic configurations for video streaming platform"""
        
        # Common configuration for all topics
        base_config = {
            'compression.type': 'lz4',
            'cleanup.policy': 'delete',
            'segment.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'min.insync.replicas': '2',
            'unclean.leader.election.enable': 'false'
        }
        
        topics = {}
        
        # Video events topic - high volume, real-time playback events
        video_config = base_config.copy()
        video_config.update({
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'max.message.bytes': '1048576',  # 1MB
            'segment.bytes': str(100 * 1024 * 1024),  # 100MB
        })
        
        topics['video_events'] = TopicConfig(
            name='video_events',
            num_partitions=self.config.kafka.topics['video_events']['partitions'],
            replication_factor=self.config.kafka.topics['video_events']['replication_factor'],
            config=video_config,
            description="Real-time video playback events (play, pause, seek, etc.)"
        )
        
        # User interactions topic - medium volume, user engagement events
        interaction_config = base_config.copy()
        interaction_config.update({
            'retention.ms': str(14 * 24 * 60 * 60 * 1000),  # 14 days
            'max.message.bytes': '2097152',  # 2MB (for comments)
            'segment.bytes': str(50 * 1024 * 1024),  # 50MB
        })
        
        topics['user_interactions'] = TopicConfig(
            name='user_interactions',
            num_partitions=self.config.kafka.topics['user_interactions']['partitions'],
            replication_factor=self.config.kafka.topics['user_interactions']['replication_factor'],
            config=interaction_config,
            description="User interaction events (likes, comments, shares, subscriptions)"
        )
        
        # Ad events topic - important for revenue, longer retention
        ad_config = base_config.copy()
        ad_config.update({
            'retention.ms': str(30 * 24 * 60 * 60 * 1000),  # 30 days
            'max.message.bytes': '1048576',  # 1MB
            'segment.bytes': str(75 * 1024 * 1024),  # 75MB
        })
        
        topics['ad_events'] = TopicConfig(
            name='ad_events',
            num_partitions=self.config.kafka.topics['ad_events']['partitions'],
            replication_factor=self.config.kafka.topics['ad_events']['replication_factor'],
            config=ad_config,
            description="Advertisement events (impressions, clicks, completions)"
        )
        
        # Session events topic - lower volume, longer retention for user analytics
        session_config = base_config.copy()
        session_config.update({
            'retention.ms': str(90 * 24 * 60 * 60 * 1000),  # 90 days
            'max.message.bytes': '1048576',  # 1MB
            'segment.bytes': str(25 * 1024 * 1024),  # 25MB
        })
        
        topics['session_events'] = TopicConfig(
            name='session_events',
            num_partitions=self.config.kafka.topics['session_events']['partitions'],
            replication_factor=self.config.kafka.topics['session_events']['replication_factor'],
            config=session_config,
            description="Session lifecycle events (start, end, heartbeat)"
        )
        
        # Dead letter queue topic
        dlq_config = base_config.copy()
        dlq_config.update({
            'retention.ms': str(7 * 24 * 60 * 60 * 1000),  # 7 days
            'max.message.bytes': '5242880',  # 5MB
            'segment.bytes': str(10 * 1024 * 1024),  # 10MB
        })
        
        topics['dead_letter_queue'] = TopicConfig(
            name='dead_letter_queue',
            num_partitions=4,
            replication_factor=min(3, self.config.kafka.topics['video_events']['replication_factor']),
            config=dlq_config,
            description="Dead letter queue for failed message processing"
        )
        
        return topics
    
    def create_all_topics(self) -> Dict[str, bool]:
        """Create all predefined topics"""
        results = {}
        
        for topic_name, topic_config in self.topic_configs.items():
            try:
                success = self.create_topic(topic_config)
                results[topic_name] = success
            except Exception as e:
                logger.error(f"Failed to create topic {topic_name}: {e}")
                results[topic_name] = False
        
        return results
    
    def create_topic(self, topic_config: TopicConfig) -> bool:
        """Create a single topic"""
        try:
            new_topic = topic_config.to_new_topic()
            
            # Create topic
            future = self.admin_client.create_topics([new_topic])
            
            # Wait for creation to complete
            for topic_name, future_result in future.items():
                try:
                    future_result.result(timeout=30)
                    logger.info(f"Created topic: {topic_name}")
                    
                    # Register schema for topic
                    self._register_topic_schema(topic_name)
                    
                    return True
                    
                except TopicAlreadyExistsError:
                    logger.info(f"Topic {topic_name} already exists")
                    return True
                except Exception as e:
                    logger.error(f"Failed to create topic {topic_name}: {e}")
                    return False
        
        except Exception as e:
            logger.error(f"Failed to create topic {topic_config.name}: {e}")
            return False
    
    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic"""
        try:
            future = self.admin_client.delete_topics([topic_name])
            
            for topic, future_result in future.items():
                try:
                    future_result.result(timeout=30)
                    logger.info(f"Deleted topic: {topic}")
                    return True
                except UnknownTopicOrPartitionError:
                    logger.warning(f"Topic {topic} does not exist")
                    return True
                except Exception as e:
                    logger.error(f"Failed to delete topic {topic}: {e}")
                    return False
        
        except Exception as e:
            logger.error(f"Failed to delete topic {topic_name}: {e}")
            return False
    
    def scale_topic_partitions(self, topic_name: str, new_partition_count: int) -> bool:
        """Scale topic partitions (can only increase)"""
        try:
            # Get current partition count
            current_partitions = self.get_topic_metadata(topic_name)
            if not current_partitions:
                logger.error(f"Topic {topic_name} not found")
                return False
            
            current_count = current_partitions['partition_count']
            
            if new_partition_count <= current_count:
                logger.warning(f"Cannot reduce partitions. Current: {current_count}, Requested: {new_partition_count}")
                return False
            
            # Create partition update
            partition_update = {topic_name: NewPartitions(total_count=new_partition_count)}
            future = self.admin_client.create_partitions(partition_update)
            
            for topic, future_result in future.items():
                try:
                    future_result.result(timeout=30)
                    logger.info(f"Scaled topic {topic} to {new_partition_count} partitions")
                    return True
                except Exception as e:
                    logger.error(f"Failed to scale topic {topic}: {e}")
                    return False
        
        except Exception as e:
            logger.error(f"Failed to scale topic {topic_name}: {e}")
            return False
    
    def update_topic_config(self, topic_name: str, config_updates: Dict[str, str]) -> bool:
        """Update topic configuration"""
        try:
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = {resource: config_updates}
            
            future = self.admin_client.alter_configs(configs)
            
            for resource, future_result in future.items():
                try:
                    future_result.result(timeout=30)
                    logger.info(f"Updated configuration for topic {topic_name}")
                    return True
                except Exception as e:
                    logger.error(f"Failed to update config for topic {topic_name}: {e}")
                    return False
        
        except Exception as e:
            logger.error(f"Failed to update topic config {topic_name}: {e}")
            return False
    
    def get_topic_metadata(self, topic_name: str) -> Optional[Dict[str, Any]]:
        """Get topic metadata"""
        try:
            metadata = self.admin_client.describe_topics([topic_name])
            
            for topic, future_result in metadata.items():
                try:
                    topic_metadata = future_result.result(timeout=30)
                    
                    return {
                        'name': topic_metadata.topic,
                        'partition_count': len(topic_metadata.partitions),
                        'partitions': [
                            {
                                'partition_id': p.partition,
                                'leader': p.leader,
                                'replicas': p.replicas,
                                'isr': p.isr
                            } for p in topic_metadata.partitions
                        ]
                    }
                except Exception as e:
                    logger.error(f"Failed to get metadata for topic {topic}: {e}")
                    return None
        
        except Exception as e:
            logger.error(f"Failed to describe topic {topic_name}: {e}")
            return None
    
    def list_topics(self) -> List[str]:
        """List all topics"""
        try:
            metadata = self.admin_client.list_topics(timeout=30)
            return list(metadata.topics.keys())
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []
    
    def get_topic_metrics(self, topic_name: str) -> TopicMetrics:
        """Get comprehensive topic metrics"""
        try:
            # Get basic metadata
            metadata = self.get_topic_metadata(topic_name)
            if not metadata:
                return TopicMetrics(
                    name=topic_name,
                    partition_count=0,
                    replication_factor=0,
                    is_healthy=False,
                    errors=["Topic not found"]
                )
            
            # Calculate replication factor from first partition
            replication_factor = len(metadata['partitions'][0]['replicas']) if metadata['partitions'] else 0
            
            # Get topic configuration
            config = self._get_topic_config(topic_name)
            retention_ms = int(config.get('retention.ms', 0)) if config else None
            
            # Get message count and size (requires consumer)
            message_count, size_bytes = self._get_topic_size_info(topic_name)
            
            # Health check
            is_healthy, errors = self._check_topic_health(topic_name, metadata)
            
            return TopicMetrics(
                name=topic_name,
                partition_count=metadata['partition_count'],
                replication_factor=replication_factor,
                message_count=message_count,
                size_bytes=size_bytes,
                retention_ms=retention_ms,
                last_modified=datetime.now(timezone.utc),
                is_healthy=is_healthy,
                errors=errors
            )
        
        except Exception as e:
            logger.error(f"Failed to get metrics for topic {topic_name}: {e}")
            return TopicMetrics(
                name=topic_name,
                partition_count=0,
                replication_factor=0,
                is_healthy=False,
                errors=[str(e)]
            )
    
    def _get_topic_config(self, topic_name: str) -> Optional[Dict[str, str]]:
        """Get topic configuration"""
        try:
            resource = ConfigResource(ConfigResourceType.TOPIC, topic_name)
            configs = self.admin_client.describe_configs([resource])
            
            for resource, future_result in configs.items():
                try:
                    config_result = future_result.result(timeout=30)
                    return {entry.name: entry.value for entry in config_result.configs}
                except Exception as e:
                    logger.error(f"Failed to get config for topic {topic_name}: {e}")
                    return None
        
        except Exception as e:
            logger.error(f"Failed to describe config for topic {topic_name}: {e}")
            return None
    
    def _get_topic_size_info(self, topic_name: str) -> Tuple[Optional[int], Optional[int]]:
        """Get topic message count and size (approximate)"""
        try:
            # Create consumer to get high water marks
            consumer_config = self.config.get_kafka_consumer_config()
            consumer_config['group_id'] = f'topic_manager_{int(time.time())}'
            consumer_config['auto_offset_reset'] = 'earliest'
            
            consumer = KafkaConsumer(
                topic_name,
                **consumer_config
            )
            
            # Get partition information
            partitions = consumer.partitions_for_topic(topic_name)
            if not partitions:
                consumer.close()
                return None, None
            
            total_messages = 0
            
            from kafka import TopicPartition
            for partition in partitions:
                tp = TopicPartition(topic_name, partition)
                
                # Get beginning and end offsets
                beginning_offsets = consumer.beginning_offsets([tp])
                end_offsets = consumer.end_offsets([tp])
                
                beginning_offset = beginning_offsets.get(tp, 0)
                end_offset = end_offsets.get(tp, 0)
                
                partition_messages = max(0, end_offset - beginning_offset)
                total_messages += partition_messages
            
            consumer.close()
            
            # Estimate size (rough approximation: 1KB per message)
            estimated_size = total_messages * 1024
            
            return total_messages, estimated_size
        
        except Exception as e:
            logger.error(f"Failed to get size info for topic {topic_name}: {e}")
            return None, None
    
    def _check_topic_health(self, topic_name: str, metadata: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Check topic health"""
        errors = []
        is_healthy = True
        
        # Check if all partitions have leaders
        for partition in metadata['partitions']:
            if partition['leader'] == -1:
                errors.append(f"Partition {partition['partition_id']} has no leader")
                is_healthy = False
            
            # Check if ISR is smaller than replicas
            if len(partition['isr']) < len(partition['replicas']):
                errors.append(f"Partition {partition['partition_id']} has under-replicated ISR")
                is_healthy = False
        
        return is_healthy, errors
    
    def _register_topic_schema(self, topic_name: str):
        """Register schema for topic"""
        try:
            # Define schemas for different topic types
            schemas = {
                'video_events': {
                    'type': 'object',
                    'properties': {
                        'event_id': {'type': 'string'},
                        'event_type': {'type': 'string'},
                        'event_timestamp': {'type': 'string'},
                        'user_id': {'type': 'string'},
                        'video_id': {'type': 'string'},
                        'session_id': {'type': 'string'},
                        'playback_position': {'type': 'integer'},
                        'video_quality': {'type': 'string'}
                    },
                    'required': ['event_id', 'event_type', 'event_timestamp', 'session_id']
                },
                'user_interactions': {
                    'type': 'object',
                    'properties': {
                        'event_id': {'type': 'string'},
                        'event_type': {'type': 'string'},
                        'event_timestamp': {'type': 'string'},
                        'user_id': {'type': 'string'},
                        'content_id': {'type': 'string'},
                        'session_id': {'type': 'string'}
                    },
                    'required': ['event_id', 'event_type', 'event_timestamp', 'session_id']
                },
                'ad_events': {
                    'type': 'object',
                    'properties': {
                        'event_id': {'type': 'string'},
                        'event_type': {'type': 'string'},
                        'event_timestamp': {'type': 'string'},
                        'user_id': {'type': 'string'},
                        'ad_id': {'type': 'string'},
                        'session_id': {'type': 'string'},
                        'content_id': {'type': 'string'}
                    },
                    'required': ['event_id', 'event_type', 'event_timestamp', 'session_id']
                },
                'session_events': {
                    'type': 'object',
                    'properties': {
                        'event_id': {'type': 'string'},
                        'event_type': {'type': 'string'},
                        'event_timestamp': {'type': 'string'},
                        'user_id': {'type': 'string'},
                        'session_id': {'type': 'string'},
                        'device_id': {'type': 'string'}
                    },
                    'required': ['event_id', 'event_type', 'event_timestamp', 'session_id']
                }
            }
            
            if topic_name in schemas:
                subject = f"{topic_name}-value"
                schema_id = self.schema_registry.register_schema(subject, schemas[topic_name])
                logger.info(f"Registered schema for topic {topic_name} with ID {schema_id}")
        
        except Exception as e:
            logger.error(f"Failed to register schema for topic {topic_name}: {e}")
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'admin_client_connected': self.admin_client is not None,
            'topics': {},
            'total_topics': 0,
            'healthy_topics': 0,
            'unhealthy_topics': 0
        }
        
        try:
            # Check all topics
            topics = self.list_topics()
            health['total_topics'] = len(topics)
            
            for topic_name in topics:
                if topic_name.startswith('__'):  # Skip internal topics
                    continue
                
                topic_metrics = self.get_topic_metrics(topic_name)
                health['topics'][topic_name] = {
                    'partition_count': topic_metrics.partition_count,
                    'replication_factor': topic_metrics.replication_factor,
                    'is_healthy': topic_metrics.is_healthy,
                    'errors': topic_metrics.errors,
                    'message_count': topic_metrics.message_count
                }
                
                if topic_metrics.is_healthy:
                    health['healthy_topics'] += 1
                else:
                    health['unhealthy_topics'] += 1
            
            # Determine overall status
            if health['unhealthy_topics'] > 0:
                if health['unhealthy_topics'] >= health['healthy_topics']:
                    health['status'] = 'unhealthy'
                else:
                    health['status'] = 'degraded'
        
        except Exception as e:
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        return health


def main():
    """Main function for topic management operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Topic Management")
    parser.add_argument('--action', choices=['create', 'delete', 'list', 'describe', 'health'],
                       default='create', help='Action to perform')
    parser.add_argument('--topic', type=str, help='Topic name for specific operations')
    parser.add_argument('--partitions', type=int, help='Number of partitions for scaling')
    parser.add_argument('--all', action='store_true', help='Apply to all topics')
    
    args = parser.parse_args()
    
    # Initialize topic manager
    topic_manager = KafkaTopicManager()
    
    if args.action == 'create':
        if args.all:
            results = topic_manager.create_all_topics()
            for topic, success in results.items():
                status = "✓" if success else "✗"
                print(f"{status} {topic}")
        elif args.topic:
            if args.topic in topic_manager.topic_configs:
                success = topic_manager.create_topic(topic_manager.topic_configs[args.topic])
                status = "✓" if success else "✗"
                print(f"{status} {args.topic}")
            else:
                print(f"Unknown topic: {args.topic}")
        else:
            print("Specify --topic or --all")
    
    elif args.action == 'delete':
        if args.topic:
            success = topic_manager.delete_topic(args.topic)
            status = "✓" if success else "✗"
            print(f"{status} Deleted {args.topic}")
        else:
            print("Specify --topic")
    
    elif args.action == 'list':
        topics = topic_manager.list_topics()
        print("Topics:")
        for topic in sorted(topics):
            if not topic.startswith('__'):  # Skip internal topics
                print(f"  {topic}")
    
    elif args.action == 'describe':
        if args.topic:
            metadata = topic_manager.get_topic_metadata(args.topic)
            if metadata:
                print(f"Topic: {metadata['name']}")
                print(f"Partitions: {metadata['partition_count']}")
                for partition in metadata['partitions']:
                    print(f"  Partition {partition['partition_id']}: Leader={partition['leader']}, "
                          f"Replicas={partition['replicas']}, ISR={partition['isr']}")
            else:
                print(f"Topic {args.topic} not found")
        else:
            print("Specify --topic")
    
    elif args.action == 'health':
        health = topic_manager.health_check()
        print(f"Status: {health['status']}")
        print(f"Total topics: {health['total_topics']}")
        print(f"Healthy: {health['healthy_topics']}")
        print(f"Unhealthy: {health['unhealthy_topics']}")
        
        if health['unhealthy_topics'] > 0:
            print("\nUnhealthy topics:")
            for topic, info in health['topics'].items():
                if not info['is_healthy']:
                    print(f"  {topic}: {', '.join(info['errors'])}")


if __name__ == "__main__":
    main()
