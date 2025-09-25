#!/usr/bin/env python3
"""
Video Streaming Analytics Lakehouse - Live Demo
This script demonstrates the data generation capabilities without external dependencies.
"""

import json
import time
from datetime import datetime, timezone
from faker import Faker
import random
import uuid

fake = Faker()

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
        'content_duration': random.randint(300, 7200),  # 5 min to 2 hours
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

def main():
    """Run the live demo"""
    print("🎬 Video Streaming Analytics Lakehouse - Live Demo")
    print("=" * 60)
    print("Generating realistic video streaming events...")
    print("Press Ctrl+C to stop\n")
    
    event_count = 0
    start_time = time.time()
    
    try:
        while True:
            # Generate different types of events
            event_type = random.choices(
                ['video', 'interaction', 'ad'], 
                weights=[70, 20, 10]  # 70% video, 20% interaction, 10% ad
            )[0]
            
            if event_type == 'video':
                event = generate_video_event()
            elif event_type == 'interaction':
                event = generate_user_interaction_event()
            else:
                event = generate_ad_event()
            
            # Pretty print the event
            print(f"📊 Event #{event_count + 1} - {event['event_type'].upper()}")
            print(f"   User: {event['user_id']} | Device: {event.get('device_type', 'N/A')}")
            if 'content_title' in event:
                print(f"   Content: {event['content_title']}")
            elif 'content_id' in event:
                print(f"   Content ID: {event['content_id']}")
            print(f"   Timestamp: {event['event_timestamp']}")
            print(f"   Country: {event.get('country', 'N/A')}")
            if 'video_quality' in event:
                print(f"   Quality: {event['video_quality']} | Bitrate: {event.get('bitrate', 'N/A')} kbps")
            print()
            
            event_count += 1
            
            # Show statistics every 10 events
            if event_count % 10 == 0:
                elapsed = time.time() - start_time
                events_per_sec = event_count / elapsed
                print(f"📈 STATS: {event_count} events generated | {events_per_sec:.1f} events/sec")
                print("=" * 60)
            
            # Wait 1 second between events for demo
            time.sleep(1)
            
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        events_per_sec = event_count / elapsed if elapsed > 0 else 0
        
        print(f"\n🏁 Demo Complete!")
        print(f"   Total Events: {event_count}")
        print(f"   Duration: {elapsed:.1f} seconds")
        print(f"   Average Rate: {events_per_sec:.1f} events/sec")
        print(f"\nThis demonstrates the type of data your lakehouse processes!")
        print(f"In production, this runs at 10-100+ events/second with:")
        print(f"   ✓ Real-time Kafka streaming")
        print(f"   ✓ Delta Lake ACID storage")
        print(f"   ✓ Spark analytics processing")
        print(f"   ✓ Snowflake data warehouse")
        print(f"   ✓ Real-time dashboards")

if __name__ == "__main__":
    main()
