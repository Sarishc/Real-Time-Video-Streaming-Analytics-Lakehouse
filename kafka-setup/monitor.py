"""
Kafka Cluster Monitoring and Alerting

This module provides comprehensive monitoring for Kafka clusters including:
- Producer and consumer lag monitoring
- Broker health and performance metrics
- Topic-level monitoring and alerting
- JMX metrics collection
- Integration with Prometheus and Grafana
- Alert notifications via Slack and email
"""

import logging
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import json
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
import requests

from kafka import KafkaConsumer, KafkaAdminClient
from kafka.structs import TopicPartition
from kafka.errors import KafkaError
import pymqi  # For JMX monitoring (optional)
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest

# Add parent directory to path for imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config.config import get_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
KAFKA_BROKER_ONLINE = Gauge('kafka_broker_online', 'Broker online status', ['broker_id'])
KAFKA_TOPIC_PARTITIONS = Gauge('kafka_topic_partitions', 'Number of partitions per topic', ['topic'])
KAFKA_TOPIC_MESSAGE_RATE = Gauge('kafka_topic_message_rate', 'Messages per second per topic', ['topic'])
KAFKA_CONSUMER_LAG = Gauge('kafka_consumer_lag_total', 'Consumer lag', ['topic', 'partition', 'group'])
KAFKA_CONSUMER_LAG_SUM = Gauge('kafka_consumer_lag_sum', 'Sum of consumer lag per group', ['group'])
KAFKA_BROKER_DISK_USAGE = Gauge('kafka_broker_disk_usage_percent', 'Broker disk usage percentage', ['broker_id'])
KAFKA_BROKER_NETWORK_IO = Gauge('kafka_broker_network_io_bytes', 'Broker network I/O', ['broker_id', 'direction'])
KAFKA_ALERTS_TOTAL = Counter('kafka_alerts_total', 'Total alerts generated', ['alert_type', 'severity'])


@dataclass
class BrokerMetrics:
    """Kafka broker metrics"""
    broker_id: int
    host: str
    port: int
    is_online: bool
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    disk_usage: Optional[float] = None
    network_in_rate: Optional[float] = None
    network_out_rate: Optional[float] = None
    active_connections: Optional[int] = None
    request_rate: Optional[float] = None
    error_rate: Optional[float] = None
    last_updated: Optional[datetime] = None


@dataclass
class TopicMetrics:
    """Topic-level metrics"""
    topic: str
    partition_count: int
    replication_factor: int
    message_rate: float
    byte_rate: float
    consumer_groups: List[str]
    total_lag: int
    last_updated: Optional[datetime] = None


@dataclass
class ConsumerGroupMetrics:
    """Consumer group metrics"""
    group_id: str
    topics: List[str]
    total_lag: int
    lag_by_topic: Dict[str, int]
    lag_by_partition: Dict[str, Dict[int, int]]
    members: List[str]
    state: str
    coordinator: Optional[int] = None
    last_updated: Optional[datetime] = None


@dataclass
class Alert:
    """Alert definition"""
    alert_id: str
    alert_type: str
    severity: str  # critical, warning, info
    title: str
    message: str
    metric_name: str
    metric_value: float
    threshold: float
    timestamp: datetime
    resolved: bool = False
    resolution_timestamp: Optional[datetime] = None


class AlertManager:
    """Alert management and notification system"""
    
    def __init__(self):
        self.config = get_config()
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self.notification_cooldown: Dict[str, datetime] = {}
        self.cooldown_duration = timedelta(minutes=15)  # 15 minutes cooldown
    
    def check_threshold(self, metric_name: str, current_value: float, 
                       threshold: float, comparison: str = 'greater') -> bool:
        """Check if metric exceeds threshold"""
        if comparison == 'greater':
            return current_value > threshold
        elif comparison == 'less':
            return current_value < threshold
        elif comparison == 'equal':
            return current_value == threshold
        return False
    
    def create_alert(self, alert_type: str, severity: str, title: str, 
                    message: str, metric_name: str, metric_value: float, 
                    threshold: float) -> Alert:
        """Create new alert"""
        alert_id = f"{alert_type}_{metric_name}_{int(time.time())}"
        
        alert = Alert(
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity,
            title=title,
            message=message,
            metric_name=metric_name,
            metric_value=metric_value,
            threshold=threshold,
            timestamp=datetime.now(timezone.utc)
        )
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Update Prometheus metric
        KAFKA_ALERTS_TOTAL.labels(alert_type=alert_type, severity=severity).inc()
        
        logger.warning(f"Alert created: {title} - {message}")
        
        # Send notification
        self._send_notification(alert)
        
        return alert
    
    def resolve_alert(self, alert_id: str):
        """Resolve an active alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolution_timestamp = datetime.now(timezone.utc)
            
            del self.active_alerts[alert_id]
            
            logger.info(f"Alert resolved: {alert.title}")
            
            # Send resolution notification
            self._send_resolution_notification(alert)
    
    def _send_notification(self, alert: Alert):
        """Send alert notification"""
        # Check cooldown
        cooldown_key = f"{alert.alert_type}_{alert.metric_name}"
        if cooldown_key in self.notification_cooldown:
            last_notification = self.notification_cooldown[cooldown_key]
            if datetime.now(timezone.utc) - last_notification < self.cooldown_duration:
                logger.debug(f"Alert notification skipped due to cooldown: {alert.title}")
                return
        
        # Send notifications
        try:
            if self.config.monitoring.slack_webhook_url:
                self._send_slack_notification(alert)
            
            if self.config.monitoring.alert_recipients:
                self._send_email_notification(alert)
            
            # Update cooldown
            self.notification_cooldown[cooldown_key] = datetime.now(timezone.utc)
            
        except Exception as e:
            logger.error(f"Failed to send alert notification: {e}")
    
    def _send_slack_notification(self, alert: Alert):
        """Send Slack notification"""
        if not self.config.monitoring.slack_webhook_url:
            return
        
        color = {
            'critical': '#FF0000',
            'warning': '#FFA500',
            'info': '#00FF00'
        }.get(alert.severity, '#808080')
        
        payload = {
            'attachments': [{
                'color': color,
                'title': f"🚨 Kafka Alert: {alert.title}",
                'text': alert.message,
                'fields': [
                    {'title': 'Severity', 'value': alert.severity.upper(), 'short': True},
                    {'title': 'Metric', 'value': alert.metric_name, 'short': True},
                    {'title': 'Current Value', 'value': f"{alert.metric_value:.2f}", 'short': True},
                    {'title': 'Threshold', 'value': f"{alert.threshold:.2f}", 'short': True},
                    {'title': 'Time', 'value': alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'), 'short': False}
                ]
            }]
        }
        
        response = requests.post(self.config.monitoring.slack_webhook_url, json=payload)
        response.raise_for_status()
        
        logger.debug(f"Slack notification sent for alert: {alert.title}")
    
    def _send_email_notification(self, alert: Alert):
        """Send email notification"""
        if not self.config.monitoring.alert_recipients:
            return
        
        subject = f"[KAFKA ALERT] {alert.severity.upper()}: {alert.title}"
        
        body = f"""
        Kafka Alert Details:
        
        Severity: {alert.severity.upper()}
        Title: {alert.title}
        Message: {alert.message}
        
        Metric: {alert.metric_name}
        Current Value: {alert.metric_value:.2f}
        Threshold: {alert.threshold:.2f}
        
        Timestamp: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}
        Alert ID: {alert.alert_id}
        
        Please investigate this issue promptly.
        """
        
        try:
            msg = MimeMultipart()
            msg['From'] = self.config.monitoring.email_username
            msg['Subject'] = subject
            msg.attach(MimeText(body, 'plain'))
            
            server = smtplib.SMTP(self.config.monitoring.email_smtp_server, 
                                self.config.monitoring.email_smtp_port)
            server.starttls()
            server.login(self.config.monitoring.email_username, 
                        self.config.monitoring.email_password)
            
            for recipient in self.config.monitoring.alert_recipients:
                msg['To'] = recipient
                server.send_message(msg)
            
            server.quit()
            
            logger.debug(f"Email notification sent for alert: {alert.title}")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def _send_resolution_notification(self, alert: Alert):
        """Send alert resolution notification"""
        try:
            if self.config.monitoring.slack_webhook_url:
                payload = {
                    'attachments': [{
                        'color': '#00FF00',
                        'title': f"✅ Kafka Alert Resolved: {alert.title}",
                        'text': f"Alert has been resolved.\n\nOriginal message: {alert.message}",
                        'fields': [
                            {'title': 'Resolution Time', 'value': alert.resolution_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'), 'short': False}
                        ]
                    }]
                }
                
                response = requests.post(self.config.monitoring.slack_webhook_url, json=payload)
                response.raise_for_status()
        
        except Exception as e:
            logger.error(f"Failed to send resolution notification: {e}")


class JMXMetricsCollector:
    """JMX metrics collector for Kafka brokers"""
    
    def __init__(self, jmx_ports: Dict[int, int] = None):
        """Initialize JMX collector with broker JMX ports"""
        self.jmx_ports = jmx_ports or {}  # {broker_id: jmx_port}
        self.jmx_enabled = bool(jmx_ports)
        
        if self.jmx_enabled:
            logger.info(f"JMX monitoring enabled for brokers: {list(jmx_ports.keys())}")
        else:
            logger.info("JMX monitoring disabled")
    
    def collect_broker_metrics(self, broker_id: int) -> Optional[Dict[str, float]]:
        """Collect JMX metrics for a broker"""
        if not self.jmx_enabled or broker_id not in self.jmx_ports:
            return None
        
        try:
            # This is a simplified example - in production, you'd use a proper JMX client
            # like py4j with a Java JMX bridge or jolokia for HTTP JMX access
            
            metrics = {
                'cpu_usage': self._get_jmx_metric(broker_id, 'kafka.server:type=KafkaServer,name=BrokerTopicMetrics'),
                'memory_usage': self._get_jmx_metric(broker_id, 'java.lang:type=Memory'),
                'disk_usage': self._get_jmx_metric(broker_id, 'kafka.log:type=LogSize'),
                'network_in_rate': self._get_jmx_metric(broker_id, 'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec'),
                'network_out_rate': self._get_jmx_metric(broker_id, 'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec'),
                'request_rate': self._get_jmx_metric(broker_id, 'kafka.network:type=RequestMetrics,name=RequestsPerSec'),
            }
            
            return {k: v for k, v in metrics.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Failed to collect JMX metrics for broker {broker_id}: {e}")
            return None
    
    def _get_jmx_metric(self, broker_id: int, mbean_name: str) -> Optional[float]:
        """Get specific JMX metric (placeholder implementation)"""
        # This is a placeholder - implement actual JMX connection
        # For production, use jolokia or py4j with proper JMX MBean access
        import random
        return random.uniform(0, 100)  # Mock data


class KafkaClusterMonitor:
    """
    Comprehensive Kafka cluster monitoring system with:
    - Real-time metrics collection
    - Consumer lag monitoring
    - Broker health checks
    - Automated alerting
    - Integration with monitoring systems
    """
    
    def __init__(self, jmx_ports: Optional[Dict[int, int]] = None):
        """Initialize Kafka cluster monitor"""
        self.config = get_config()
        self.admin_client = None
        self.alert_manager = AlertManager()
        self.jmx_collector = JMXMetricsCollector(jmx_ports)
        
        # Monitoring state
        self.is_running = False
        self.broker_metrics: Dict[int, BrokerMetrics] = {}
        self.topic_metrics: Dict[str, TopicMetrics] = {}
        self.consumer_group_metrics: Dict[str, ConsumerGroupMetrics] = {}
        
        # Monitoring configuration
        self.monitoring_interval = self.config.monitoring.health_check_interval
        self.lag_threshold = 10000  # Consumer lag threshold
        self.error_rate_threshold = 0.05  # 5% error rate threshold
        self.disk_usage_threshold = 0.85  # 85% disk usage threshold
        
        # Background threads
        self.monitor_thread = None
        self.metrics_thread = None
        
        # Initialize admin client
        self._initialize_admin_client()
        
        logger.info("Kafka cluster monitor initialized")
    
    def _initialize_admin_client(self):
        """Initialize Kafka admin client"""
        try:
            admin_config = {
                'bootstrap_servers': self.config.kafka.bootstrap_servers,
                'security_protocol': self.config.kafka.security_protocol,
                'request_timeout_ms': 30000
            }
            
            if self.config.kafka.sasl_mechanism:
                admin_config.update({
                    'sasl_mechanism': self.config.kafka.sasl_mechanism,
                    'sasl_plain_username': self.config.kafka.sasl_username,
                    'sasl_plain_password': self.config.kafka.sasl_password
                })
            
            self.admin_client = KafkaAdminClient(**admin_config)
            logger.info("Kafka admin client initialized for monitoring")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka admin client: {e}")
            raise
    
    def start(self):
        """Start monitoring"""
        if self.is_running:
            logger.warning("Monitor is already running")
            return
        
        self.is_running = True
        
        # Start monitoring threads
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        self.metrics_thread = threading.Thread(target=self._metrics_collection_loop, daemon=True)
        self.metrics_thread.start()
        
        logger.info("Kafka cluster monitoring started")
    
    def stop(self):
        """Stop monitoring"""
        logger.info("Stopping Kafka cluster monitor...")
        self.is_running = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=30)
        
        if self.metrics_thread and self.metrics_thread.is_alive():
            self.metrics_thread.join(timeout=30)
        
        logger.info("Kafka cluster monitor stopped")
    
    def _monitoring_loop(self):
        """Main monitoring loop"""
        logger.info("Monitoring loop started")
        
        while self.is_running:
            try:
                start_time = time.time()
                
                # Collect broker metrics
                self._collect_broker_metrics()
                
                # Collect topic metrics
                self._collect_topic_metrics()
                
                # Collect consumer group metrics
                self._collect_consumer_group_metrics()
                
                # Check alerts
                self._check_alerts()
                
                # Calculate sleep time to maintain interval
                elapsed_time = time.time() - start_time
                sleep_time = max(0, self.monitoring_interval - elapsed_time)
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)  # Brief pause on error
        
        logger.info("Monitoring loop stopped")
    
    def _metrics_collection_loop(self):
        """Metrics collection loop for Prometheus"""
        logger.info("Metrics collection loop started")
        
        while self.is_running:
            try:
                # Update Prometheus metrics
                self._update_prometheus_metrics()
                
                # Sleep for metrics update interval
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}")
                time.sleep(5)
        
        logger.info("Metrics collection loop stopped")
    
    def _collect_broker_metrics(self):
        """Collect broker metrics"""
        try:
            # Get cluster metadata
            metadata = self.admin_client.describe_cluster()
            brokers = metadata.brokers
            
            for broker in brokers:
                broker_id = broker.nodeId
                host = broker.host
                port = broker.port
                
                # Basic broker info
                broker_metrics = BrokerMetrics(
                    broker_id=broker_id,
                    host=host,
                    port=port,
                    is_online=True,  # If we can get metadata, broker is online
                    last_updated=datetime.now(timezone.utc)
                )
                
                # Collect JMX metrics if available
                jmx_metrics = self.jmx_collector.collect_broker_metrics(broker_id)
                if jmx_metrics:
                    broker_metrics.cpu_usage = jmx_metrics.get('cpu_usage')
                    broker_metrics.memory_usage = jmx_metrics.get('memory_usage')
                    broker_metrics.disk_usage = jmx_metrics.get('disk_usage')
                    broker_metrics.network_in_rate = jmx_metrics.get('network_in_rate')
                    broker_metrics.network_out_rate = jmx_metrics.get('network_out_rate')
                    broker_metrics.request_rate = jmx_metrics.get('request_rate')
                
                self.broker_metrics[broker_id] = broker_metrics
                
                logger.debug(f"Collected metrics for broker {broker_id}")
        
        except Exception as e:
            logger.error(f"Failed to collect broker metrics: {e}")
    
    def _collect_topic_metrics(self):
        """Collect topic metrics"""
        try:
            # List all topics
            topics = self.admin_client.list_topics(timeout=30)
            
            for topic_name in topics.topics:
                if topic_name.startswith('__'):  # Skip internal topics
                    continue
                
                # Get topic metadata
                topic_metadata = self.admin_client.describe_topics([topic_name])
                topic_info = topic_metadata[topic_name].result()
                
                # Calculate message rate (placeholder - would need consumer to get actual rates)
                message_rate = self._estimate_topic_message_rate(topic_name)
                
                # Get consumer groups for this topic
                consumer_groups = self._get_topic_consumer_groups(topic_name)
                
                # Calculate total lag
                total_lag = sum(
                    self.consumer_group_metrics.get(group, ConsumerGroupMetrics('', [], 0, {}, {}, [], '')).lag_by_topic.get(topic_name, 0)
                    for group in consumer_groups
                )
                
                topic_metrics = TopicMetrics(
                    topic=topic_name,
                    partition_count=len(topic_info.partitions),
                    replication_factor=len(topic_info.partitions[0].replicas) if topic_info.partitions else 0,
                    message_rate=message_rate,
                    byte_rate=message_rate * 1024,  # Estimate 1KB per message
                    consumer_groups=consumer_groups,
                    total_lag=total_lag,
                    last_updated=datetime.now(timezone.utc)
                )
                
                self.topic_metrics[topic_name] = topic_metrics
                
                logger.debug(f"Collected metrics for topic {topic_name}")
        
        except Exception as e:
            logger.error(f"Failed to collect topic metrics: {e}")
    
    def _collect_consumer_group_metrics(self):
        """Collect consumer group metrics"""
        try:
            # List consumer groups
            consumer_groups = self.admin_client.list_consumer_groups()
            
            for group_info in consumer_groups:
                group_id = group_info.group_id
                
                try:
                    # Get consumer group description
                    group_descriptions = self.admin_client.describe_consumer_groups([group_id])
                    group_desc = group_descriptions[group_id].result()
                    
                    # Get consumer group offsets
                    offsets = self._get_consumer_group_offsets(group_id)
                    
                    # Calculate lag
                    lag_by_topic = defaultdict(int)
                    lag_by_partition = defaultdict(dict)
                    total_lag = 0
                    
                    for (topic, partition), (current_offset, high_water_mark) in offsets.items():
                        lag = max(0, high_water_mark - current_offset)
                        lag_by_topic[topic] += lag
                        lag_by_partition[topic][partition] = lag
                        total_lag += lag
                    
                    group_metrics = ConsumerGroupMetrics(
                        group_id=group_id,
                        topics=list(lag_by_topic.keys()),
                        total_lag=total_lag,
                        lag_by_topic=dict(lag_by_topic),
                        lag_by_partition=dict(lag_by_partition),
                        members=[member.member_id for member in group_desc.members],
                        state=group_desc.state,
                        coordinator=group_desc.coordinator.nodeId if group_desc.coordinator else None,
                        last_updated=datetime.now(timezone.utc)
                    )
                    
                    self.consumer_group_metrics[group_id] = group_metrics
                    
                    logger.debug(f"Collected metrics for consumer group {group_id}")
                
                except Exception as e:
                    logger.error(f"Failed to collect metrics for consumer group {group_id}: {e}")
        
        except Exception as e:
            logger.error(f"Failed to collect consumer group metrics: {e}")
    
    def _get_consumer_group_offsets(self, group_id: str) -> Dict[Tuple[str, int], Tuple[int, int]]:
        """Get consumer group offsets and high water marks"""
        offsets = {}
        
        try:
            # Create consumer to get offsets
            consumer_config = self.config.get_kafka_consumer_config()
            consumer_config['group_id'] = group_id
            consumer_config['enable_auto_commit'] = False
            
            consumer = KafkaConsumer(**consumer_config)
            
            # Get group assignment
            assignment = consumer.list_consumer_group_offsets(group_id)
            
            for topic_partition, offset_metadata in assignment.items():
                current_offset = offset_metadata.offset
                
                # Get high water mark
                high_water_marks = consumer.end_offsets([topic_partition])
                high_water_mark = high_water_marks.get(topic_partition, current_offset)
                
                offsets[(topic_partition.topic, topic_partition.partition)] = (current_offset, high_water_mark)
            
            consumer.close()
            
        except Exception as e:
            logger.error(f"Failed to get offsets for consumer group {group_id}: {e}")
        
        return offsets
    
    def _estimate_topic_message_rate(self, topic_name: str) -> float:
        """Estimate topic message rate (placeholder implementation)"""
        # In production, this would calculate actual message rates
        # based on offset differences over time
        return 100.0  # Mock rate
    
    def _get_topic_consumer_groups(self, topic_name: str) -> List[str]:
        """Get consumer groups subscribed to a topic"""
        groups = []
        
        for group_id, group_metrics in self.consumer_group_metrics.items():
            if topic_name in group_metrics.topics:
                groups.append(group_id)
        
        return groups
    
    def _check_alerts(self):
        """Check metrics against thresholds and generate alerts"""
        try:
            # Check broker alerts
            for broker_id, metrics in self.broker_metrics.items():
                if not metrics.is_online:
                    self.alert_manager.create_alert(
                        alert_type='broker_offline',
                        severity='critical',
                        title=f'Broker {broker_id} Offline',
                        message=f'Kafka broker {broker_id} ({metrics.host}:{metrics.port}) is offline',
                        metric_name='broker_online',
                        metric_value=0,
                        threshold=1
                    )
                
                if metrics.disk_usage and metrics.disk_usage > self.disk_usage_threshold:
                    self.alert_manager.create_alert(
                        alert_type='high_disk_usage',
                        severity='warning',
                        title=f'High Disk Usage on Broker {broker_id}',
                        message=f'Broker {broker_id} disk usage is {metrics.disk_usage:.1%}',
                        metric_name='disk_usage',
                        metric_value=metrics.disk_usage,
                        threshold=self.disk_usage_threshold
                    )
            
            # Check consumer lag alerts
            for group_id, metrics in self.consumer_group_metrics.items():
                if metrics.total_lag > self.lag_threshold:
                    self.alert_manager.create_alert(
                        alert_type='high_consumer_lag',
                        severity='warning',
                        title=f'High Consumer Lag for Group {group_id}',
                        message=f'Consumer group {group_id} has lag of {metrics.total_lag:,} messages',
                        metric_name='consumer_lag',
                        metric_value=metrics.total_lag,
                        threshold=self.lag_threshold
                    )
            
            # Check topic alerts
            for topic_name, metrics in self.topic_metrics.items():
                if metrics.message_rate > 10000:  # High message rate threshold
                    self.alert_manager.create_alert(
                        alert_type='high_message_rate',
                        severity='info',
                        title=f'High Message Rate for Topic {topic_name}',
                        message=f'Topic {topic_name} has message rate of {metrics.message_rate:.0f} msg/sec',
                        metric_name='message_rate',
                        metric_value=metrics.message_rate,
                        threshold=10000
                    )
        
        except Exception as e:
            logger.error(f"Failed to check alerts: {e}")
    
    def _update_prometheus_metrics(self):
        """Update Prometheus metrics"""
        try:
            # Update broker metrics
            for broker_id, metrics in self.broker_metrics.items():
                KAFKA_BROKER_ONLINE.labels(broker_id=broker_id).set(1 if metrics.is_online else 0)
                
                if metrics.disk_usage is not None:
                    KAFKA_BROKER_DISK_USAGE.labels(broker_id=broker_id).set(metrics.disk_usage * 100)
                
                if metrics.network_in_rate is not None:
                    KAFKA_BROKER_NETWORK_IO.labels(broker_id=broker_id, direction='in').set(metrics.network_in_rate)
                
                if metrics.network_out_rate is not None:
                    KAFKA_BROKER_NETWORK_IO.labels(broker_id=broker_id, direction='out').set(metrics.network_out_rate)
            
            # Update topic metrics
            for topic_name, metrics in self.topic_metrics.items():
                KAFKA_TOPIC_PARTITIONS.labels(topic=topic_name).set(metrics.partition_count)
                KAFKA_TOPIC_MESSAGE_RATE.labels(topic=topic_name).set(metrics.message_rate)
            
            # Update consumer lag metrics
            for group_id, metrics in self.consumer_group_metrics.items():
                KAFKA_CONSUMER_LAG_SUM.labels(group=group_id).set(metrics.total_lag)
                
                for topic, lag_by_partition in metrics.lag_by_partition.items():
                    for partition, lag in lag_by_partition.items():
                        KAFKA_CONSUMER_LAG.labels(
                            topic=topic,
                            partition=partition,
                            group=group_id
                        ).set(lag)
        
        except Exception as e:
            logger.error(f"Failed to update Prometheus metrics: {e}")
    
    def get_cluster_health(self) -> Dict[str, Any]:
        """Get overall cluster health"""
        health = {
            'status': 'healthy',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'brokers': {
                'total': len(self.broker_metrics),
                'online': sum(1 for m in self.broker_metrics.values() if m.is_online),
                'offline': sum(1 for m in self.broker_metrics.values() if not m.is_online)
            },
            'topics': {
                'total': len(self.topic_metrics),
                'total_partitions': sum(m.partition_count for m in self.topic_metrics.values())
            },
            'consumer_groups': {
                'total': len(self.consumer_group_metrics),
                'high_lag_groups': sum(1 for m in self.consumer_group_metrics.values() if m.total_lag > self.lag_threshold)
            },
            'alerts': {
                'active': len(self.alert_manager.active_alerts),
                'total_history': len(self.alert_manager.alert_history)
            }
        }
        
        # Determine overall status
        if health['brokers']['offline'] > 0:
            health['status'] = 'degraded'
        
        if health['brokers']['offline'] >= health['brokers']['total'] // 2:
            health['status'] = 'unhealthy'
        
        if health['consumer_groups']['high_lag_groups'] > 0:
            if health['status'] == 'healthy':
                health['status'] = 'degraded'
        
        return health
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get metrics summary"""
        return {
            'brokers': {broker_id: asdict(metrics) for broker_id, metrics in self.broker_metrics.items()},
            'topics': {topic: asdict(metrics) for topic, metrics in self.topic_metrics.items()},
            'consumer_groups': {group: asdict(metrics) for group, metrics in self.consumer_group_metrics.items()},
            'active_alerts': {alert_id: asdict(alert) for alert_id, alert in self.alert_manager.active_alerts.items()}
        }


def main():
    """Main function for running the monitor"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Cluster Monitor")
    parser.add_argument('--interval', type=int, default=60,
                       help='Monitoring interval in seconds')
    parser.add_argument('--jmx-ports', type=str,
                       help='JMX ports for brokers (format: broker1:port1,broker2:port2)')
    parser.add_argument('--lag-threshold', type=int, default=10000,
                       help='Consumer lag threshold for alerts')
    
    args = parser.parse_args()
    
    # Parse JMX ports
    jmx_ports = {}
    if args.jmx_ports:
        for mapping in args.jmx_ports.split(','):
            broker_id, port = mapping.split(':')
            jmx_ports[int(broker_id)] = int(port)
    
    # Create and start monitor
    monitor = KafkaClusterMonitor(jmx_ports=jmx_ports)
    monitor.monitoring_interval = args.interval
    monitor.lag_threshold = args.lag_threshold
    
    try:
        monitor.start()
        
        # Keep running and print status periodically
        while monitor.is_running:
            time.sleep(60)  # Print status every minute
            
            health = monitor.get_cluster_health()
            logger.info(f"Cluster Status: {health['status']} | "
                       f"Brokers: {health['brokers']['online']}/{health['brokers']['total']} | "
                       f"Topics: {health['topics']['total']} | "
                       f"Consumer Groups: {health['consumer_groups']['total']} | "
                       f"Active Alerts: {health['alerts']['active']}")
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, shutting down...")
    except Exception as e:
        logger.error(f"Error in monitor: {e}")
    finally:
        monitor.stop()


if __name__ == "__main__":
    main()
