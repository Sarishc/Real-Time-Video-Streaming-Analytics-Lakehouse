"""
Fault-Tolerant Kafka Consumer for Video Streaming Events

This module implements a production-ready Kafka consumer with:
- Exactly-once processing semantics
- Automatic offset management
- Dead letter queue for failed messages
- Batch processing and S3 storage
- Comprehensive error handling and monitoring
- Graceful shutdown and recovery
"""

import json
import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List, Callable, Set
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
import signal
import sys
from pathlib import Path
import gzip
import uuid
from collections import defaultdict, deque

from kafka import KafkaConsumer
from kafka.errors import KafkaError, CommitFailedError, KafkaTimeoutError
import boto3
from botocore.exceptions import ClientError
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config
from data_generation.schemas import EventEncoder, BaseEvent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
MESSAGES_CONSUMED_TOTAL = Counter('kafka_messages_consumed_total', 'Total messages consumed', ['topic', 'status'])
MESSAGE_PROCESSING_DURATION = Histogram('kafka_message_processing_duration_seconds', 'Message processing duration')
CONSUMER_LAG = Gauge('kafka_consumer_lag', 'Consumer lag', ['topic', 'partition'])
BATCH_SIZE = Histogram('kafka_batch_size', 'Batch processing size')
S3_UPLOADS_TOTAL = Counter('s3_uploads_total', 'Total S3 uploads', ['status'])
PROCESSING_ERRORS_TOTAL = Counter('kafka_processing_errors_total', 'Total processing errors', ['error_type'])


@dataclass
class ConsumerMetrics:
    """Consumer performance metrics"""
    messages_processed: int = 0
    messages_failed: int = 0
    bytes_processed: int = 0
    total_batches: int = 0
    avg_batch_size: float = 0.0
    last_commit_timestamp: Optional[datetime] = None
    processing_times: deque = None
    errors_by_type: Dict[str, int] = None
    
    def __post_init__(self):
        if self.processing_times is None:
            self.processing_times = deque(maxlen=1000)  # Keep last 1000 processing times
        if self.errors_by_type is None:
            self.errors_by_type = {}


@dataclass
class MessageBatch:
    """Batch of messages for processing"""
    messages: List[Dict[str, Any]]
    topic: str
    partition: int
    offsets: List[int]
    timestamps: List[datetime]
    batch_id: str
    
    def __post_init__(self):
        if not self.batch_id:
            self.batch_id = str(uuid.uuid4())


class S3Uploader:
    """S3 uploader for processed events"""
    
    def __init__(self, bucket_name: str, prefix: str = "streaming-events"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.config = get_config()
        
        try:
            # Initialize S3 client
            session = boto3.Session(
                region_name=self.config.aws.region,
                profile_name=self.config.aws.profile
            )
            self.s3_client = session.client('s3')
            
            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"S3 uploader initialized for bucket: {bucket_name}")
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logger.error(f"S3 bucket not found: {bucket_name}")
            else:
                logger.error(f"Failed to initialize S3 client: {e}")
            raise
        except Exception as e:
            logger.error(f"Error initializing S3 uploader: {e}")
            raise
    
    def upload_batch(self, events: List[Dict[str, Any]], topic: str, 
                    partition: int, timestamp: datetime) -> str:
        """Upload batch of events to S3"""
        try:
            # Create S3 key with partitioning
            date_partition = timestamp.strftime("%Y/%m/%d/%H")
            filename = f"{topic}_p{partition}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json.gz"
            s3_key = f"{self.prefix}/{date_partition}/{filename}"
            
            # Prepare data
            data = []
            for event in events:
                # Add metadata
                event_with_metadata = {
                    **event,
                    '_kafka_topic': topic,
                    '_kafka_partition': partition,
                    '_processing_timestamp': timestamp.isoformat(),
                    '_batch_id': str(uuid.uuid4())
                }
                data.append(event_with_metadata)
            
            # Compress and upload
            json_data = '\n'.join(json.dumps(event, default=str) for event in data)
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'topic': topic,
                    'partition': str(partition),
                    'event_count': str(len(events)),
                    'processing_timestamp': timestamp.isoformat()
                }
            )
            
            S3_UPLOADS_TOTAL.labels(status='success').inc()
            logger.debug(f"Uploaded {len(events)} events to S3: {s3_key}")
            
            return s3_key
            
        except Exception as e:
            S3_UPLOADS_TOTAL.labels(status='error').inc()
            logger.error(f"Failed to upload batch to S3: {e}")
            raise
    
    def upload_dead_letter(self, message: Dict[str, Any], error: str, 
                          topic: str, partition: int) -> str:
        """Upload failed message to dead letter location in S3"""
        try:
            timestamp = datetime.now(timezone.utc)
            date_partition = timestamp.strftime("%Y/%m/%d")
            filename = f"dlq_{topic}_p{partition}_{timestamp.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
            s3_key = f"dead-letters/{date_partition}/{filename}"
            
            dead_letter_event = {
                'original_message': message,
                'error': error,
                'topic': topic,
                'partition': partition,
                'timestamp': timestamp.isoformat(),
                'dlq_id': str(uuid.uuid4())
            }
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(dead_letter_event, default=str),
                ContentType='application/json',
                Metadata={
                    'topic': topic,
                    'partition': str(partition),
                    'error_type': type(Exception).__name__,
                    'timestamp': timestamp.isoformat()
                }
            )
            
            logger.debug(f"Uploaded dead letter to S3: {s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Failed to upload dead letter to S3: {e}")
            raise


class EventProcessor:
    """Event processor with validation and transformation"""
    
    def __init__(self):
        self.encoder = EventEncoder()
        self.validation_enabled = True
        self.transformation_enabled = True
    
    def process_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process single message with validation and transformation"""
        try:
            # Validate message structure
            if self.validation_enabled:
                self._validate_message(message)
            
            # Transform message
            if self.transformation_enabled:
                transformed_message = self._transform_message(message)
            else:
                transformed_message = message
            
            # Add processing metadata
            transformed_message['_processed_at'] = datetime.now(timezone.utc).isoformat()
            transformed_message['_processor_version'] = '1.0.0'
            
            return transformed_message
            
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            PROCESSING_ERRORS_TOTAL.labels(error_type=type(e).__name__).inc()
            raise
    
    def _validate_message(self, message: Dict[str, Any]):
        """Validate message structure and content"""
        required_fields = ['event_type', 'event_timestamp', 'session_id']
        
        for field in required_fields:
            if field not in message:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate timestamp
        try:
            if isinstance(message['event_timestamp'], str):
                datetime.fromisoformat(message['event_timestamp'].replace('Z', '+00:00'))
        except (ValueError, TypeError):
            raise ValueError(f"Invalid timestamp format: {message['event_timestamp']}")
        
        # Validate event type
        if not isinstance(message['event_type'], str):
            raise ValueError(f"Invalid event_type: {message['event_type']}")
    
    def _transform_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Transform message (e.g., data enrichment, format normalization)"""
        transformed = message.copy()
        
        # Normalize timestamp to ISO format with timezone
        if 'event_timestamp' in transformed:
            try:
                if isinstance(transformed['event_timestamp'], str):
                    # Ensure timezone info
                    if not transformed['event_timestamp'].endswith(('Z', '+00:00')):
                        dt = datetime.fromisoformat(transformed['event_timestamp'])
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        transformed['event_timestamp'] = dt.isoformat()
            except Exception as e:
                logger.warning(f"Failed to normalize timestamp: {e}")
        
        # Add data quality score
        transformed['_data_quality_score'] = self._calculate_quality_score(transformed)
        
        # Normalize user_id
        if 'user_id' in transformed and transformed['user_id']:
            transformed['user_id'] = str(transformed['user_id']).strip()
        
        return transformed
    
    def _calculate_quality_score(self, message: Dict[str, Any]) -> float:
        """Calculate data quality score (0.0 to 1.0)"""
        score = 1.0
        
        # Penalize missing optional fields
        optional_fields = ['user_id', 'device_id', 'ip_address', 'country']
        missing_fields = sum(1 for field in optional_fields if not message.get(field))
        score -= (missing_fields / len(optional_fields)) * 0.2
        
        # Penalize invalid values
        if message.get('user_id') == 'unknown':
            score -= 0.1
        
        return max(0.0, score)


class KafkaConsumerManager:
    """
    High-performance Kafka consumer with advanced features:
    - Exactly-once processing
    - Batch processing with configurable sizes
    - Automatic S3 storage
    - Dead letter queue
    - Comprehensive monitoring
    - Graceful shutdown and recovery
    """
    
    def __init__(self, topics: List[str], config_override: Optional[Dict[str, Any]] = None):
        """Initialize Kafka consumer manager"""
        self.config = get_config()
        self.topics = topics
        self.is_running = False
        self.metrics = ConsumerMetrics()
        
        # Consumer configuration
        self.consumer_config = self.config.get_kafka_consumer_config()
        if config_override:
            self.consumer_config.update(config_override)
        
        # Add exactly-once semantics
        self.consumer_config.update({
            'isolation_level': 'read_committed',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  # Manual offset management
            'max_poll_records': 500,
            'fetch_min_bytes': 1024,
            'fetch_max_wait_ms': 500
        })
        
        # Initialize components
        self.consumer = None
        self.s3_uploader = S3Uploader(
            bucket_name=self.config.aws.s3_bucket,
            prefix=self.config.aws.s3_raw_prefix
        )
        self.event_processor = EventProcessor()
        
        # Processing configuration
        self.batch_size = 100
        self.batch_timeout = 30.0  # seconds
        self.max_retries = 3
        self.retry_delay = 1.0  # seconds
        
        # Background processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.consumer_thread = None
        self.processing_futures: Set[Future] = set()
        
        # Offset management
        self.pending_offsets = defaultdict(dict)  # {topic: {partition: offset}}
        self.committed_offsets = defaultdict(dict)
        
        logger.info(f"Kafka consumer manager initialized for topics: {topics}")
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with error handling"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                **self.consumer_config
            )
            
            # Subscribe to topics
            self.consumer.subscribe(self.topics)
            
            logger.info(f"Kafka consumer initialized for topics: {self.topics}")
            logger.info(f"Consumer group: {self.consumer_config['group_id']}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def start(self):
        """Start the consumer manager"""
        if self.is_running:
            logger.warning("Consumer manager is already running")
            return
        
        self.is_running = True
        
        # Initialize consumer
        self._initialize_consumer()
        
        # Start Prometheus metrics server (if not already started)
        try:
            start_http_server(self.config.monitoring.prometheus_port + 1)  # Different port than producer
            logger.info(f"Prometheus metrics server started on port {self.config.monitoring.prometheus_port + 1}")
        except Exception as e:
            logger.warning(f"Failed to start metrics server: {e}")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(target=self._consumer_loop, daemon=True)
        self.consumer_thread.start()
        
        logger.info("Kafka consumer manager started")
    
    def stop(self):
        """Stop the consumer manager gracefully"""
        logger.info("Stopping Kafka consumer manager...")
        self.is_running = False
        
        # Wait for current processing to complete
        if self.processing_futures:
            logger.info(f"Waiting for {len(self.processing_futures)} processing tasks to complete...")
            for future in as_completed(self.processing_futures, timeout=30):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in processing task: {e}")
        
        # Commit pending offsets
        self._commit_offsets()
        
        # Close consumer
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        logger.info("Kafka consumer manager stopped")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def _consumer_loop(self):
        """Main consumer loop"""
        logger.info("Consumer loop started")
        
        batch_messages = []
        batch_start_time = time.time()
        
        while self.is_running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    # Check if we should process current batch due to timeout
                    if batch_messages and (time.time() - batch_start_time) > self.batch_timeout:
                        self._submit_batch_for_processing(batch_messages)
                        batch_messages = []
                        batch_start_time = time.time()
                    continue
                
                # Process messages from all partitions
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            # Parse message
                            parsed_message = {
                                'topic': topic_partition.topic,
                                'partition': topic_partition.partition,
                                'offset': message.offset,
                                'key': message.key.decode('utf-8') if message.key else None,
                                'value': json.loads(message.value.decode('utf-8')),
                                'timestamp': datetime.fromtimestamp(message.timestamp / 1000, tz=timezone.utc),
                                'headers': dict(message.headers) if message.headers else {}
                            }
                            
                            batch_messages.append(parsed_message)
                            
                            # Update metrics
                            MESSAGES_CONSUMED_TOTAL.labels(
                                topic=topic_partition.topic, 
                                status='received'
                            ).inc()
                            
                            # Update consumer lag
                            high_water_mark = self.consumer.highwater(topic_partition)
                            if high_water_mark:
                                lag = high_water_mark - message.offset - 1
                                CONSUMER_LAG.labels(
                                    topic=topic_partition.topic,
                                    partition=topic_partition.partition
                                ).set(max(0, lag))
                            
                        except Exception as e:
                            logger.error(f"Failed to parse message: {e}")
                            PROCESSING_ERRORS_TOTAL.labels(error_type='parse_error').inc()
                            continue
                
                # Check if batch is ready for processing
                if (len(batch_messages) >= self.batch_size or 
                    (batch_messages and (time.time() - batch_start_time) > self.batch_timeout)):
                    
                    self._submit_batch_for_processing(batch_messages)
                    batch_messages = []
                    batch_start_time = time.time()
            
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                PROCESSING_ERRORS_TOTAL.labels(error_type='consumer_loop_error').inc()
                time.sleep(1)  # Brief pause on error
        
        # Process remaining messages
        if batch_messages:
            self._submit_batch_for_processing(batch_messages)
        
        logger.info("Consumer loop stopped")
    
    def _submit_batch_for_processing(self, messages: List[Dict[str, Any]]):
        """Submit batch of messages for async processing"""
        if not messages:
            return
        
        # Group messages by topic and partition
        batches_by_partition = defaultdict(list)
        for message in messages:
            key = (message['topic'], message['partition'])
            batches_by_partition[key].append(message)
        
        # Submit each partition batch for processing
        for (topic, partition), partition_messages in batches_by_partition.items():
            future = self.executor.submit(
                self._process_batch,
                partition_messages,
                topic,
                partition
            )
            self.processing_futures.add(future)
            
            # Clean up completed futures
            completed_futures = {f for f in self.processing_futures if f.done()}
            self.processing_futures -= completed_futures
        
        BATCH_SIZE.observe(len(messages))
        logger.debug(f"Submitted batch of {len(messages)} messages for processing")
    
    def _process_batch(self, messages: List[Dict[str, Any]], topic: str, partition: int):
        """Process batch of messages"""
        start_time = time.time()
        processed_events = []
        failed_messages = []
        max_offset = -1
        
        try:
            logger.debug(f"Processing batch of {len(messages)} messages from {topic}:{partition}")
            
            for message in messages:
                try:
                    # Process individual message
                    processed_event = self.event_processor.process_message(message['value'])
                    
                    if processed_event:
                        processed_events.append(processed_event)
                        MESSAGES_CONSUMED_TOTAL.labels(topic=topic, status='processed').inc()
                    
                    # Track offset
                    max_offset = max(max_offset, message['offset'])
                    
                except Exception as e:
                    logger.error(f"Failed to process message at offset {message['offset']}: {e}")
                    failed_messages.append((message, str(e)))
                    MESSAGES_CONSUMED_TOTAL.labels(topic=topic, status='failed').inc()
            
            # Upload processed events to S3
            if processed_events:
                try:
                    s3_key = self.s3_uploader.upload_batch(
                        processed_events,
                        topic,
                        partition,
                        datetime.now(timezone.utc)
                    )
                    logger.debug(f"Uploaded {len(processed_events)} events to S3: {s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload batch to S3: {e}")
                    # Don't commit offsets if S3 upload fails
                    raise
            
            # Handle failed messages
            for failed_message, error in failed_messages:
                try:
                    self.s3_uploader.upload_dead_letter(
                        failed_message['value'],
                        error,
                        topic,
                        partition
                    )
                except Exception as e:
                    logger.error(f"Failed to upload dead letter: {e}")
            
            # Update pending offsets
            if max_offset >= 0:
                self.pending_offsets[topic][partition] = max_offset + 1
            
            # Update metrics
            processing_time = time.time() - start_time
            MESSAGE_PROCESSING_DURATION.observe(processing_time)
            self.metrics.messages_processed += len(processed_events)
            self.metrics.messages_failed += len(failed_messages)
            self.metrics.processing_times.append(processing_time)
            self.metrics.total_batches += 1
            
            # Calculate average batch size
            self.metrics.avg_batch_size = (
                (self.metrics.avg_batch_size * (self.metrics.total_batches - 1) + len(messages))
                / self.metrics.total_batches
            )
            
        except Exception as e:
            logger.error(f"Failed to process batch for {topic}:{partition}: {e}")
            PROCESSING_ERRORS_TOTAL.labels(error_type='batch_processing_error').inc()
            raise
    
    def _commit_offsets(self):
        """Commit pending offsets"""
        if not self.pending_offsets:
            return
        
        try:
            # Prepare offset commit data
            commit_data = {}
            for topic, partitions in self.pending_offsets.items():
                for partition, offset in partitions.items():
                    from kafka import TopicPartition
                    tp = TopicPartition(topic, partition)
                    commit_data[tp] = offset
            
            if commit_data:
                self.consumer.commit(offsets=commit_data)
                
                # Update committed offsets
                for topic, partitions in self.pending_offsets.items():
                    for partition, offset in partitions.items():
                        self.committed_offsets[topic][partition] = offset
                
                # Clear pending offsets
                self.pending_offsets.clear()
                
                self.metrics.last_commit_timestamp = datetime.now(timezone.utc)
                logger.debug(f"Committed offsets for {len(commit_data)} partitions")
        
        except CommitFailedError as e:
            logger.error(f"Failed to commit offsets: {e}")
            PROCESSING_ERRORS_TOTAL.labels(error_type='commit_failed').inc()
        except Exception as e:
            logger.error(f"Error committing offsets: {e}")
            PROCESSING_ERRORS_TOTAL.labels(error_type='commit_error').inc()
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current consumer metrics"""
        avg_processing_time = (
            sum(self.metrics.processing_times) / len(self.metrics.processing_times)
            if self.metrics.processing_times else 0.0
        )
        
        return {
            'messages_processed': self.metrics.messages_processed,
            'messages_failed': self.metrics.messages_failed,
            'total_batches': self.metrics.total_batches,
            'avg_batch_size': self.metrics.avg_batch_size,
            'avg_processing_time': avg_processing_time,
            'pending_futures': len(self.processing_futures),
            'last_commit_timestamp': (
                self.metrics.last_commit_timestamp.isoformat() 
                if self.metrics.last_commit_timestamp else None
            ),
            'errors_by_type': self.metrics.errors_by_type,
            'topics': self.topics,
            'is_running': self.is_running
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'consumer_connected': self.consumer is not None,
            'is_running': self.is_running,
            'topics': self.topics,
            'pending_futures': len(self.processing_futures)
        }
        
        # Check if consumer is responsive
        try:
            if self.consumer:
                # Check partition assignment
                assignment = self.consumer.assignment()
                health['partition_count'] = len(assignment)
                health['consumer_responsive'] = True
            else:
                health['consumer_responsive'] = False
                health['status'] = 'unhealthy'
        except Exception as e:
            health['consumer_responsive'] = False
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        # Check processing performance
        if self.metrics.processing_times:
            recent_times = list(self.metrics.processing_times)[-10:]  # Last 10 processing times
            avg_recent_time = sum(recent_times) / len(recent_times)
            
            if avg_recent_time > 30.0:  # 30 seconds threshold
                health['status'] = 'degraded'
                health['warning'] = f'High processing time: {avg_recent_time:.2f}s'
        
        return health


def main():
    """Main function for running the consumer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Event Consumer")
    parser.add_argument('--topics', nargs='+', 
                       default=['video_events', 'user_interactions', 'ad_events', 'session_events'],
                       help='Kafka topics to consume')
    parser.add_argument('--group-id', type=str, default='video-streaming-consumer',
                       help='Consumer group ID')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Batch size for processing')
    parser.add_argument('--batch-timeout', type=float, default=30.0,
                       help='Batch timeout in seconds')
    
    args = parser.parse_args()
    
    # Create consumer manager
    consumer_config_override = {
        'group_id': args.group_id
    }
    
    consumer_manager = KafkaConsumerManager(
        topics=args.topics,
        config_override=consumer_config_override
    )
    
    # Configure batch processing
    consumer_manager.batch_size = args.batch_size
    consumer_manager.batch_timeout = args.batch_timeout
    
    try:
        # Start consumer
        consumer_manager.start()
        
        # Keep running until interrupted
        while consumer_manager.is_running:
            time.sleep(1)
            
            # Periodically commit offsets
            if time.time() % 30 == 0:  # Every 30 seconds
                consumer_manager._commit_offsets()
            
            # Log metrics periodically
            if consumer_manager.metrics.messages_processed > 0 and consumer_manager.metrics.messages_processed % 10000 == 0:
                metrics = consumer_manager.get_metrics()
                logger.info(f"Processed {metrics['messages_processed']} messages, "
                           f"failed {metrics['messages_failed']}, "
                           f"avg processing time: {metrics['avg_processing_time']:.3f}s")
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        consumer_manager.stop()


if __name__ == "__main__":
    main()
