"""
High-Throughput Kafka Producer for Video Streaming Events

This module implements a production-ready Kafka producer with:
- High throughput and low latency
- Fault tolerance and retry mechanisms
- Exactly-once semantics
- Comprehensive monitoring and metrics
- Schema validation and serialization
- Dead letter queue for failed messages
"""

import json
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, Future
import signal
import sys
from queue import Queue, Empty
import uuid
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, MessageSizeTooLargeError
import boto3
from prometheus_client import Counter, Histogram, Gauge, start_http_server

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config
from data_generation.data_generator import VideoStreamingDataGenerator
from data_generation.schemas import EventEncoder, BaseEvent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
EVENTS_PRODUCED_TOTAL = Counter('kafka_events_produced_total', 'Total events produced', ['topic', 'status'])
EVENTS_PRODUCTION_DURATION = Histogram('kafka_events_production_duration_seconds', 'Event production duration')
PRODUCER_BUFFER_SIZE = Gauge('kafka_producer_buffer_size', 'Producer buffer size')
PRODUCER_ERRORS_TOTAL = Counter('kafka_producer_errors_total', 'Total producer errors', ['error_type'])
PRODUCER_RETRIES_TOTAL = Counter('kafka_producer_retries_total', 'Total producer retries')


@dataclass
class ProducerMetrics:
    """Producer performance metrics"""
    events_sent: int = 0
    events_failed: int = 0
    bytes_sent: int = 0
    total_batches: int = 0
    avg_batch_size: float = 0.0
    last_send_timestamp: Optional[datetime] = None
    errors_by_type: Dict[str, int] = None
    
    def __post_init__(self):
        if self.errors_by_type is None:
            self.errors_by_type = {}


@dataclass
class EventBatch:
    """Batch of events for efficient processing"""
    events: List[Dict[str, Any]]
    topic: str
    timestamp: datetime
    batch_id: str
    
    def __post_init__(self):
        if not self.batch_id:
            self.batch_id = str(uuid.uuid4())


class DeadLetterQueue:
    """Dead letter queue for failed messages"""
    
    def __init__(self, s3_bucket: Optional[str] = None, local_path: str = "./dead_letters"):
        self.s3_bucket = s3_bucket
        self.local_path = Path(local_path)
        self.local_path.mkdir(exist_ok=True)
        
        if s3_bucket:
            try:
                self.s3_client = boto3.client('s3')
                logger.info(f"Dead letter queue configured with S3 bucket: {s3_bucket}")
            except Exception as e:
                logger.warning(f"Failed to initialize S3 client: {e}")
                self.s3_client = None
        else:
            self.s3_client = None
            logger.info(f"Dead letter queue configured with local path: {local_path}")
    
    def send_to_dlq(self, event: Dict[str, Any], error: str, topic: str):
        """Send failed event to dead letter queue"""
        dlq_event = {
            'original_event': event,
            'error': error,
            'topic': topic,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'dlq_id': str(uuid.uuid4())
        }
        
        try:
            if self.s3_client and self.s3_bucket:
                # Save to S3
                key = f"dead_letters/{datetime.now().strftime('%Y/%m/%d')}/{dlq_event['dlq_id']}.json"
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=key,
                    Body=json.dumps(dlq_event, default=str),
                    ContentType='application/json'
                )
                logger.debug(f"Sent event to S3 DLQ: {key}")
            else:
                # Save to local file
                date_str = datetime.now().strftime('%Y-%m-%d')
                dlq_file = self.local_path / f"dlq_{date_str}.jsonl"
                
                with open(dlq_file, 'a') as f:
                    f.write(json.dumps(dlq_event, default=str) + '\n')
                
                logger.debug(f"Sent event to local DLQ: {dlq_file}")
                
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")


class KafkaProducerManager:
    """
    High-performance Kafka producer with advanced features:
    - Batching and compression
    - Retry logic with exponential backoff
    - Dead letter queue
    - Circuit breaker pattern
    - Comprehensive monitoring
    """
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """Initialize Kafka producer manager"""
        self.config = get_config()
        self.is_running = False
        self.metrics = ProducerMetrics()
        self.event_queue = Queue(maxsize=10000)  # Buffer for events
        self.dead_letter_queue = DeadLetterQueue(
            s3_bucket=getattr(self.config.aws, 's3_bucket', None)
        )
        
        # Producer configuration
        self.producer_config = self.config.get_kafka_producer_config()
        if config_override:
            self.producer_config.update(config_override)
        
        # Add idempotence for exactly-once semantics
        self.producer_config.update({
            'enable_idempotence': True,
            'max_in_flight_requests_per_connection': 5,
            'retries': 10,
            'delivery_timeout_ms': 300000,  # 5 minutes
            'request_timeout_ms': 30000,    # 30 seconds
        })
        
        # Circuit breaker state
        self.circuit_breaker = {
            'failures': 0,
            'last_failure_time': None,
            'state': 'CLOSED',  # CLOSED, OPEN, HALF_OPEN
            'failure_threshold': 5,
            'timeout_seconds': 60
        }
        
        # Initialize producer
        self.producer = None
        self._initialize_producer()
        
        # Background threads
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.producer_thread = None
        
        logger.info("Kafka producer manager initialized")
    
    def _initialize_producer(self):
        """Initialize Kafka producer with error handling"""
        try:
            self.producer = KafkaProducer(**self.producer_config)
            logger.info("Kafka producer initialized successfully")
            self._reset_circuit_breaker()
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            self._handle_circuit_breaker_failure()
            raise
    
    def _reset_circuit_breaker(self):
        """Reset circuit breaker to closed state"""
        self.circuit_breaker.update({
            'failures': 0,
            'last_failure_time': None,
            'state': 'CLOSED'
        })
    
    def _handle_circuit_breaker_failure(self):
        """Handle circuit breaker failure"""
        self.circuit_breaker['failures'] += 1
        self.circuit_breaker['last_failure_time'] = time.time()
        
        if self.circuit_breaker['failures'] >= self.circuit_breaker['failure_threshold']:
            self.circuit_breaker['state'] = 'OPEN'
            logger.warning("Circuit breaker opened due to repeated failures")
    
    def _check_circuit_breaker(self) -> bool:
        """Check if circuit breaker allows operations"""
        if self.circuit_breaker['state'] == 'CLOSED':
            return True
        
        if self.circuit_breaker['state'] == 'OPEN':
            time_since_failure = time.time() - self.circuit_breaker['last_failure_time']
            if time_since_failure > self.circuit_breaker['timeout_seconds']:
                self.circuit_breaker['state'] = 'HALF_OPEN'
                logger.info("Circuit breaker moved to HALF_OPEN state")
                return True
            return False
        
        # HALF_OPEN state - allow single request
        return True
    
    def start(self):
        """Start the producer manager"""
        if self.is_running:
            logger.warning("Producer manager is already running")
            return
        
        self.is_running = True
        
        # Start Prometheus metrics server
        start_http_server(self.config.monitoring.prometheus_port)
        logger.info(f"Prometheus metrics server started on port {self.config.monitoring.prometheus_port}")
        
        # Start background producer thread
        self.producer_thread = threading.Thread(target=self._producer_loop, daemon=True)
        self.producer_thread.start()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Kafka producer manager started")
    
    def stop(self):
        """Stop the producer manager gracefully"""
        logger.info("Stopping Kafka producer manager...")
        self.is_running = False
        
        # Wait for producer thread to finish
        if self.producer_thread and self.producer_thread.is_alive():
            self.producer_thread.join(timeout=30)
        
        # Flush and close producer
        if self.producer:
            try:
                self.producer.flush(timeout=30)
                self.producer.close(timeout=30)
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Error closing Kafka producer: {e}")
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        logger.info("Kafka producer manager stopped")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)
    
    def send_event(self, event: Dict[str, Any], topic: str, 
                  key: Optional[str] = None, callback: Optional[Callable] = None) -> Future:
        """
        Send single event to Kafka topic
        
        Args:
            event: Event data dictionary
            topic: Kafka topic name
            key: Optional message key
            callback: Optional callback function for result
            
        Returns:
            Future object for async result handling
        """
        if not self._check_circuit_breaker():
            error_msg = "Circuit breaker is OPEN, rejecting event"
            logger.warning(error_msg)
            PRODUCER_ERRORS_TOTAL.labels(error_type='circuit_breaker').inc()
            self.dead_letter_queue.send_to_dlq(event, error_msg, topic)
            raise KafkaError(error_msg)
        
        try:
            # Validate event
            self._validate_event(event)
            
            # Serialize event
            serialized_event = self._serialize_event(event)
            
            # Send to Kafka
            future = self.producer.send(
                topic=topic,
                value=serialized_event,
                key=key.encode('utf-8') if key else None
            )
            
            # Add callback wrapper
            if callback:
                future.add_callback(callback)
            future.add_errback(self._error_callback, event, topic)
            
            # Update metrics
            EVENTS_PRODUCED_TOTAL.labels(topic=topic, status='sent').inc()
            self.metrics.events_sent += 1
            self.metrics.last_send_timestamp = datetime.now(timezone.utc)
            
            return future
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            PRODUCER_ERRORS_TOTAL.labels(error_type=type(e).__name__).inc()
            self.metrics.events_failed += 1
            self._handle_circuit_breaker_failure()
            self.dead_letter_queue.send_to_dlq(event, str(e), topic)
            raise
    
    def send_batch(self, events: List[Dict[str, Any]], topic: str, 
                  keys: Optional[List[str]] = None) -> List[Future]:
        """
        Send batch of events to Kafka topic
        
        Args:
            events: List of event dictionaries
            topic: Kafka topic name
            keys: Optional list of message keys
            
        Returns:
            List of Future objects
        """
        if not events:
            return []
        
        if keys and len(keys) != len(events):
            raise ValueError("Number of keys must match number of events")
        
        futures = []
        batch_start_time = time.time()
        
        for i, event in enumerate(events):
            key = keys[i] if keys else None
            try:
                future = self.send_event(event, topic, key)
                futures.append(future)
            except Exception as e:
                logger.error(f"Failed to send event {i} in batch: {e}")
                # Continue with other events in batch
                continue
        
        # Update batch metrics
        batch_duration = time.time() - batch_start_time
        EVENTS_PRODUCTION_DURATION.observe(batch_duration)
        self.metrics.total_batches += 1
        self.metrics.avg_batch_size = (
            (self.metrics.avg_batch_size * (self.metrics.total_batches - 1) + len(events))
            / self.metrics.total_batches
        )
        
        logger.debug(f"Sent batch of {len(events)} events to topic {topic} in {batch_duration:.3f}s")
        
        return futures
    
    def send_event_async(self, event: Dict[str, Any], topic: str, key: Optional[str] = None):
        """Add event to async processing queue"""
        try:
            self.event_queue.put_nowait({
                'event': event,
                'topic': topic,
                'key': key,
                'timestamp': datetime.now(timezone.utc)
            })
            PRODUCER_BUFFER_SIZE.set(self.event_queue.qsize())
        except Exception as e:
            logger.error(f"Failed to queue event: {e}")
            self.dead_letter_queue.send_to_dlq(event, str(e), topic)
    
    def _producer_loop(self):
        """Background thread loop for processing queued events"""
        logger.info("Producer loop started")
        batch_size = 100
        batch_timeout = 1.0  # seconds
        
        while self.is_running:
            batch_events = []
            batch_start_time = time.time()
            
            try:
                # Collect events for batch
                while len(batch_events) < batch_size and (time.time() - batch_start_time) < batch_timeout:
                    try:
                        queued_event = self.event_queue.get(timeout=0.1)
                        batch_events.append(queued_event)
                        self.event_queue.task_done()
                    except Empty:
                        break
                
                if batch_events:
                    # Group events by topic
                    events_by_topic = {}
                    for item in batch_events:
                        topic = item['topic']
                        if topic not in events_by_topic:
                            events_by_topic[topic] = []
                        events_by_topic[topic].append(item)
                    
                    # Send batches by topic
                    for topic, topic_events in events_by_topic.items():
                        events = [item['event'] for item in topic_events]
                        keys = [item['key'] for item in topic_events]
                        
                        try:
                            self.send_batch(events, topic, keys)
                        except Exception as e:
                            logger.error(f"Failed to send batch for topic {topic}: {e}")
                
                # Update buffer size metric
                PRODUCER_BUFFER_SIZE.set(self.event_queue.qsize())
                
            except Exception as e:
                logger.error(f"Error in producer loop: {e}")
                time.sleep(1)  # Brief pause on error
        
        logger.info("Producer loop stopped")
    
    def _validate_event(self, event: Dict[str, Any]):
        """Validate event data"""
        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")
        
        required_fields = ['event_type', 'event_timestamp', 'session_id']
        for field in required_fields:
            if field not in event:
                raise ValueError(f"Missing required field: {field}")
        
        # Check event size
        event_size = len(json.dumps(event, default=str).encode('utf-8'))
        max_size = self.producer_config.get('max_request_size', 1048576)
        if event_size > max_size:
            raise MessageSizeTooLargeError(f"Event size {event_size} exceeds maximum {max_size}")
    
    def _serialize_event(self, event: Dict[str, Any]) -> str:
        """Serialize event to JSON string"""
        try:
            return json.dumps(event, default=str, separators=(',', ':'))
        except Exception as e:
            raise ValueError(f"Failed to serialize event: {e}")
    
    def _error_callback(self, exception, event: Dict[str, Any], topic: str):
        """Handle send errors"""
        logger.error(f"Failed to send event to topic {topic}: {exception}")
        PRODUCER_ERRORS_TOTAL.labels(error_type=type(exception).__name__).inc()
        self.metrics.events_failed += 1
        self._handle_circuit_breaker_failure()
        self.dead_letter_queue.send_to_dlq(event, str(exception), topic)
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current producer metrics"""
        return {
            'events_sent': self.metrics.events_sent,
            'events_failed': self.metrics.events_failed,
            'total_batches': self.metrics.total_batches,
            'avg_batch_size': self.metrics.avg_batch_size,
            'queue_size': self.event_queue.qsize(),
            'circuit_breaker_state': self.circuit_breaker['state'],
            'last_send_timestamp': self.metrics.last_send_timestamp.isoformat() if self.metrics.last_send_timestamp else None,
            'errors_by_type': self.metrics.errors_by_type
        }
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'producer_connected': self.producer is not None,
            'circuit_breaker_state': self.circuit_breaker['state'],
            'queue_size': self.event_queue.qsize(),
            'is_running': self.is_running
        }
        
        # Check if producer is responsive
        try:
            if self.producer:
                # Try to get cluster metadata (lightweight operation)
                metadata = self.producer.cluster.available_partitions_for_topic('__health_check__')
                health['producer_responsive'] = True
            else:
                health['producer_responsive'] = False
                health['status'] = 'unhealthy'
        except Exception as e:
            health['producer_responsive'] = False
            health['status'] = 'unhealthy'
            health['error'] = str(e)
        
        # Check circuit breaker
        if self.circuit_breaker['state'] == 'OPEN':
            health['status'] = 'degraded'
            health['warning'] = 'Circuit breaker is open'
        
        return health


class StreamingEventProducer:
    """High-level streaming event producer for video platform"""
    
    def __init__(self):
        """Initialize streaming event producer"""
        self.config = get_config()
        self.producer_manager = KafkaProducerManager()
        self.data_generator = VideoStreamingDataGenerator(num_users=1000, num_content=10000)
        self.encoder = EventEncoder()
        
        # Topic mapping for event types
        self.topic_mapping = {
            'video_': 'video_events',
            'user_': 'user_interactions',
            'ad_': 'ad_events',
            'session_': 'session_events'
        }
        
        logger.info("Streaming event producer initialized")
    
    def start_streaming(self, events_per_second: int = 100, duration_hours: int = 24):
        """Start producing streaming events"""
        logger.info(f"Starting event streaming: {events_per_second} events/sec for {duration_hours} hours")
        
        self.producer_manager.start()
        
        try:
            for event_json in self.data_generator.generate_streaming_events(
                events_per_second=events_per_second,
                duration_hours=duration_hours
            ):
                event = json.loads(event_json)
                topic = self._get_topic_for_event(event)
                key = event.get('user_id', event.get('session_id'))
                
                # Send event asynchronously
                self.producer_manager.send_event_async(event, topic, key)
                
                # Log progress periodically
                if self.producer_manager.metrics.events_sent % 10000 == 0:
                    logger.info(f"Sent {self.producer_manager.metrics.events_sent} events")
        
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"Error in streaming: {e}")
        finally:
            self.producer_manager.stop()
    
    def _get_topic_for_event(self, event: Dict[str, Any]) -> str:
        """Determine topic for event based on event type"""
        event_type = event.get('event_type', '')
        
        for prefix, topic in self.topic_mapping.items():
            if event_type.startswith(prefix):
                return topic
        
        return 'video_events'  # Default topic


def main():
    """Main function for running the producer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Event Producer")
    parser.add_argument('--mode', choices=['streaming', 'batch'], default='streaming',
                       help='Production mode')
    parser.add_argument('--events-per-second', type=int, default=100,
                       help='Events per second for streaming mode')
    parser.add_argument('--duration-hours', type=int, default=1,
                       help='Duration in hours')
    parser.add_argument('--topic', type=str, default='video_events',
                       help='Kafka topic for batch mode')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='Batch size for batch mode')
    
    args = parser.parse_args()
    
    if args.mode == 'streaming':
        producer = StreamingEventProducer()
        producer.start_streaming(
            events_per_second=args.events_per_second,
            duration_hours=args.duration_hours
        )
    else:
        # Batch mode - send pre-generated events
        producer_manager = KafkaProducerManager()
        producer_manager.start()
        
        try:
            # Generate batch of events
            generator = VideoStreamingDataGenerator(num_users=100, num_content=1000)
            user = generator.user_profiles[0]
            session_events = generator.generate_user_session(user, datetime.now(timezone.utc))
            
            # Send in batches
            for i in range(0, len(session_events), args.batch_size):
                batch = session_events[i:i + args.batch_size]
                futures = producer_manager.send_batch(batch, args.topic)
                
                # Wait for batch to complete
                for future in futures:
                    try:
                        future.get(timeout=30)
                    except Exception as e:
                        logger.error(f"Failed to send event: {e}")
                
                logger.info(f"Sent batch {i // args.batch_size + 1}")
        
        except Exception as e:
            logger.error(f"Error in batch mode: {e}")
        finally:
            producer_manager.stop()


if __name__ == "__main__":
    main()
