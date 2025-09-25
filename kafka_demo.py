#!/usr/bin/env python3
"""
Kafka-enabled Video Streaming Data Generator
Produces events to Kafka topics for the lakehouse pipeline
"""

import json
import time
import os
from datetime import datetime, timezone
from faker import Faker
import random
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()

def create_kafka_producer(bootstrap_servers='localhost:19092'):
    """Create Kafka producer with error handling"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            retries=3,
            acks='all'
        )
        print(f"✅ Connected to Kafka at {bootstrap_servers}")
        return producer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        return None

def generate_video_event():
    """Generate a realistic video streaming event"""
    event_types = ['video_play', 'video_pause', 'video_stop', 'video_seek', 'video_complete']
    device_types = ['mobile', 'desktop', 'tablet', 'tv']
    platforms = ['ios', 'android', 'windows', 'macos', 'web']
    content_types = ['movie', 'tv_show', 'documentary', 'short_form']
    
    event = {
        'event_id': str(uuid.uuid4()),
        'event_type': random.choice(event_types),
        'event_timestamp': datetime.now(timezone.utc).isoformat(),
        'user_id': f"user_{random.randint(1, 1000)}",
        'session_id': f"session_{uuid.uuid4().hex[:8]}",
        'device_id': f"device_{uuid.uuid4().hex[:8]}",
        'video_id': f"video_{random.randint(1, 10000)}",
        'content_title': fake.catch_phrase(),
        'content_type': random.choice(content_types),
        'content_duration': random.randint(300, 7200),
        'playback_position': random.randint(0, 3600),
        'video_quality': random.choice(['480p', '720p', '1080p', '4k']),
        'device_type': random.choice(device_types),
        'platform': random.choice(platforms),
        'country': fake.country_code(),
        'city': fake.city(),
        'ip_address': fake.ipv4(),
        'subscription_tier': random.choice(['free', 'basic', 'premium']),
        'is_mobile': random.choice([True, False]),
        'bitrate': random.randint(500, 15000),
        'buffer_duration': round(random.uniform(0, 5), 2),
        'startup_time': round(random.uniform(0.5, 3.0), 2)
    }
    
    return event

def generate_user_interaction_event():
    """Generate a user interaction event"""
    interaction_types = ['user_like', 'user_dislike', 'user_comment', 'user_share', 'user_subscribe']
    
    event = {
        'event_id': str(uuid.uuid4()),
        'event_type': random.choice(interaction_types),
        'event_timestamp': datetime.now(timezone.utc).isoformat(),
        'user_id': f"user_{random.randint(1, 1000)}",
        'session_id': f"session_{uuid.uuid4().hex[:8]}",
        'content_id': f"video_{random.randint(1, 10000)}",
        'content_type': random.choice(['movie', 'tv_show', 'documentary']),
        'device_type': random.choice(['mobile', 'desktop', 'tablet']),
        'country': fake.country_code(),
        'subscription_tier': random.choice(['free', 'basic', 'premium'])
    }
    
    if event['event_type'] == 'user_comment':
        event['comment_text'] = fake.text(max_nb_chars=200)
    elif event['event_type'] == 'user_share':
        event['share_platform'] = random.choice(['facebook', 'twitter', 'instagram'])
    
    return event

def generate_ad_event():
    """Generate an advertisement event"""
    ad_types = ['ad_impression', 'ad_click', 'ad_skip', 'ad_complete']
    
    event = {
        'event_id': str(uuid.uuid4()),
        'event_type': random.choice(ad_types),
        'event_timestamp': datetime.now(timezone.utc).isoformat(),
        'user_id': f"user_{random.randint(1, 1000)}",
        'session_id': f"session_{uuid.uuid4().hex[:8]}",
        'ad_id': f"ad_{random.randint(1, 500)}",
        'ad_type': random.choice(['pre_roll', 'mid_roll', 'post_roll']),
        'ad_duration': random.choice([15, 30, 60]),
        'content_id': f"video_{random.randint(1, 10000)}",
        'advertiser_id': f"advertiser_{random.randint(1, 100)}",
        'ad_price': round(random.uniform(0.5, 10.0), 2),
        'currency': 'USD',
        'device_type': random.choice(['mobile', 'desktop', 'tablet']),
        'country': fake.country_code()
    }
    
    return event

def send_to_kafka(producer, event, topic):
    """Send event to Kafka topic"""
    try:
        future = producer.send(topic, value=event, key=event['user_id'])
        future.get(timeout=10)
        return True
    except KafkaError as e:
        print(f"❌ Failed to send event to {topic}: {e}")
        return False

def main():
    """Run the Kafka-enabled demo"""
    print("🎬 Video Streaming Analytics Lakehouse - Kafka Producer")
    print("=" * 60)
    
    # Get Kafka bootstrap servers from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092')
    events_per_second = int(os.getenv('EVENTS_PER_SECOND', '5'))
    
    # Create Kafka producer
    producer = create_kafka_producer(bootstrap_servers)
    if not producer:
        print("❌ Cannot proceed without Kafka connection")
        return
    
    # Create topics
    topics = {
        'video': 'video_events',
        'interaction': 'user_interactions', 
        'ad': 'ad_events'
    }
    
    print(f"📊 Producing {events_per_second} events/second to Kafka topics:")
    for event_type, topic in topics.items():
        print(f"   - {event_type} events → {topic}")
    print("Press Ctrl+C to stop\n")
    
    event_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate different types of events
            event_type = random.choices(
                ['video', 'interaction', 'ad'], 
                weights=[70, 20, 10]
            )[0]
            
            if event_type == 'video':
                event = generate_video_event()
                topic = topics['video']
            elif event_type == 'interaction':
                event = generate_user_interaction_event()
                topic = topics['interaction']
            else:
                event = generate_ad_event()
                topic = topics['ad']
            
            # Send to Kafka
            success = send_to_kafka(producer, event, topic)
            
            if success:
                event_count += 1
                print(f"📊 Event #{event_count} → {topic}: {event['event_type']} from {event['user_id']}")
                
                # Show stats every 50 events
                if event_count % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed
                    print(f"📈 STATS: {event_count} events sent | {rate:.1f} events/sec")
                    print("=" * 60)
            
            # Rate limiting
            time.sleep(1.0 / events_per_second)
            
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        rate = event_count / elapsed if elapsed > 0 else 0
        
        print(f"\n🏁 Kafka Producer Stopped!")
        print(f"   Total Events Sent: {event_count}")
        print(f"   Duration: {elapsed:.1f} seconds")
        print(f"   Average Rate: {rate:.1f} events/sec")
        print(f"   Topics Used: {list(topics.values())}")
        
    finally:
        if producer:
            producer.flush()
            producer.close()
            print("✅ Kafka producer closed cleanly")

if __name__ == "__main__":
    main()
