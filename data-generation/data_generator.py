"""
Realistic Video Streaming Data Generator

This module generates realistic video streaming events for testing and development.
It simulates user behavior patterns, device distributions, geographic data, and
realistic event sequences that mirror actual video streaming platforms.
"""

import random
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Generator
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import json
import uuid
from faker import Faker
import numpy as np

from schemas import (
    EventType, DeviceType, Platform, VideoQuality, ContentType, 
    AdType, SubscriptionTier, VideoEvent, UserInteractionEvent, 
    AdEvent, SessionEvent, EventEncoder
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Faker with multiple locales for diverse data
fake = Faker(['en_US', 'en_GB', 'es_ES', 'fr_FR', 'de_DE', 'ja_JP', 'ko_KR'])


@dataclass
class UserProfile:
    """User profile for consistent event generation"""
    user_id: str
    signup_date: datetime
    subscription_tier: SubscriptionTier
    preferred_device: DeviceType
    preferred_platform: Platform
    preferred_quality: VideoQuality
    country: str
    region: str
    city: str
    latitude: float
    longitude: float
    viewing_patterns: Dict[str, Any]
    engagement_level: float  # 0.0 to 1.0


@dataclass
class ContentCatalog:
    """Content catalog for realistic content selection"""
    content_id: str
    title: str
    content_type: ContentType
    duration: int  # seconds
    genre: str
    rating: str
    language: str
    creator_id: str
    popularity_score: float  # 0.0 to 1.0
    release_date: datetime


class VideoStreamingDataGenerator:
    """
    Advanced video streaming data generator that creates realistic event patterns
    based on user behavior modeling and statistical distributions.
    """
    
    def __init__(self, num_users: int = 1000, num_content: int = 10000, seed: int = 42):
        """Initialize the data generator"""
        random.seed(seed)
        np.random.seed(seed)
        Faker.seed(seed)
        
        self.num_users = num_users
        self.num_content = num_content
        self.encoder = EventEncoder()
        
        # Device and platform distributions (based on real-world data)
        self.device_distribution = {
            DeviceType.MOBILE: 0.45,
            DeviceType.DESKTOP: 0.25,
            DeviceType.TV: 0.20,
            DeviceType.TABLET: 0.08,
            DeviceType.GAMING_CONSOLE: 0.02
        }
        
        self.platform_mapping = {
            DeviceType.MOBILE: [Platform.IOS, Platform.ANDROID],
            DeviceType.DESKTOP: [Platform.WINDOWS, Platform.MACOS, Platform.LINUX],
            DeviceType.TV: [Platform.ROKU, Platform.APPLE_TV, Platform.ANDROID_TV, Platform.FIRE_TV],
            DeviceType.TABLET: [Platform.IOS, Platform.ANDROID],
            DeviceType.GAMING_CONSOLE: [Platform.ANDROID]
        }
        
        # Content type distributions
        self.content_type_distribution = {
            ContentType.MOVIE: 0.30,
            ContentType.TV_SHOW: 0.35,
            ContentType.SHORT_FORM: 0.20,
            ContentType.DOCUMENTARY: 0.08,
            ContentType.LIVE_STREAM: 0.05,
            ContentType.TRAILER: 0.02
        }
        
        # Quality distributions by device
        self.quality_by_device = {
            DeviceType.MOBILE: {
                VideoQuality.Q_720P: 0.40,
                VideoQuality.Q_480P: 0.30,
                VideoQuality.Q_1080P: 0.20,
                VideoQuality.AUTO: 0.10
            },
            DeviceType.DESKTOP: {
                VideoQuality.Q_1080P: 0.50,
                VideoQuality.Q_720P: 0.25,
                VideoQuality.Q_1440P: 0.15,
                VideoQuality.AUTO: 0.10
            },
            DeviceType.TV: {
                VideoQuality.Q_4K: 0.40,
                VideoQuality.Q_1080P: 0.35,
                VideoQuality.Q_1440P: 0.15,
                VideoQuality.AUTO: 0.10
            }
        }
        
        # Generate user profiles and content catalog
        self.user_profiles = self._generate_user_profiles()
        self.content_catalog = self._generate_content_catalog()
        self.ad_catalog = self._generate_ad_catalog()
        
        logger.info(f"Initialized generator with {num_users} users and {num_content} content items")
    
    def _generate_user_profiles(self) -> List[UserProfile]:
        """Generate realistic user profiles"""
        profiles = []
        
        for _ in range(self.num_users):
            # Generate basic user info
            user_id = f"user_{uuid.uuid4().hex[:8]}"
            signup_date = fake.date_time_between(start_date='-3y', end_date='now', tzinfo=timezone.utc)
            
            # Subscription tier distribution (freemium model)
            tier_weights = [0.60, 0.25, 0.13, 0.02]  # Free, Basic, Premium, Enterprise
            tier_choices = list(SubscriptionTier)
            tier_idx = np.random.choice(len(tier_choices), p=tier_weights)
            subscription_tier = tier_choices[tier_idx]
            
            # Device preference
            device_choices = list(self.device_distribution.keys())
            device_probs = list(self.device_distribution.values())
            device_idx = np.random.choice(len(device_choices), p=device_probs)
            preferred_device = device_choices[device_idx]
            
            preferred_platform = random.choice(self.platform_mapping[preferred_device])
            
            # Quality preference based on subscription tier
            if subscription_tier in [SubscriptionTier.PREMIUM, SubscriptionTier.ENTERPRISE]:
                preferred_quality = np.random.choice(
                    [VideoQuality.Q_1080P, VideoQuality.Q_4K, VideoQuality.AUTO],
                    p=[0.4, 0.4, 0.2]
                )
            elif subscription_tier == SubscriptionTier.BASIC:
                preferred_quality = np.random.choice(
                    [VideoQuality.Q_720P, VideoQuality.Q_1080P, VideoQuality.AUTO],
                    p=[0.5, 0.3, 0.2]
                )
            else:  # Free tier
                preferred_quality = np.random.choice(
                    [VideoQuality.Q_480P, VideoQuality.Q_720P, VideoQuality.AUTO],
                    p=[0.6, 0.3, 0.1]
                )
            
            # Geographic distribution
            location = fake.location_on_land()
            country = fake.country_code()
            region = fake.state()
            city = fake.city()
            
            # Viewing patterns
            viewing_patterns = {
                'daily_watch_hours': np.random.gamma(2, 2),  # Average 4 hours, realistic distribution
                'session_frequency': np.random.poisson(3),  # Sessions per day
                'preferred_times': self._generate_viewing_times(),
                'content_preferences': self._generate_content_preferences(),
                'binge_tendency': random.uniform(0.1, 0.9)  # Likelihood to watch multiple episodes
            }
            
            # Engagement level (affects interaction frequency)
            engagement_level = np.random.beta(2, 5)  # Most users have low-medium engagement
            
            profile = UserProfile(
                user_id=user_id,
                signup_date=signup_date,
                subscription_tier=subscription_tier,
                preferred_device=preferred_device,
                preferred_platform=preferred_platform,
                preferred_quality=preferred_quality,
                country=country,
                region=region,
                city=city,
                latitude=float(location[0]),
                longitude=float(location[1]),
                viewing_patterns=viewing_patterns,
                engagement_level=engagement_level
            )
            
            profiles.append(profile)
        
        return profiles
    
    def _generate_viewing_times(self) -> List[int]:
        """Generate preferred viewing times (hours of day)"""
        # Most people watch in evening (19-23) and some during lunch (12-13)
        evening_hours = list(range(19, 24))
        lunch_hours = [12, 13]
        weekend_hours = list(range(10, 16))
        
        preferred_times = []
        
        # Everyone has some evening preference
        preferred_times.extend(random.sample(evening_hours, random.randint(2, 4)))
        
        # Some watch during lunch
        if random.random() < 0.3:
            preferred_times.extend(lunch_hours)
        
        # Weekend watchers
        if random.random() < 0.6:
            preferred_times.extend(random.sample(weekend_hours, random.randint(1, 3)))
        
        return list(set(preferred_times))
    
    def _generate_content_preferences(self) -> Dict[str, float]:
        """Generate content preference weights"""
        genres = ['action', 'comedy', 'drama', 'horror', 'documentary', 'sci-fi', 'romance', 'thriller']
        preferences = {}
        
        # Generate random preferences that sum to 1
        weights = np.random.dirichlet(np.ones(len(genres)))
        for genre, weight in zip(genres, weights):
            preferences[genre] = float(weight)
        
        return preferences
    
    def _generate_content_catalog(self) -> List[ContentCatalog]:
        """Generate realistic content catalog"""
        catalog = []
        genres = ['action', 'comedy', 'drama', 'horror', 'documentary', 'sci-fi', 'romance', 'thriller']
        ratings = ['G', 'PG', 'PG-13', 'R', 'NC-17', 'TV-Y', 'TV-PG', 'TV-14', 'TV-MA']
        languages = ['en', 'es', 'fr', 'de', 'ja', 'ko', 'hi', 'zh']
        
        for _ in range(self.num_content):
            content_id = f"content_{uuid.uuid4().hex[:8]}"
            
            # Content type affects duration
            content_choices = list(self.content_type_distribution.keys())
            content_probs = list(self.content_type_distribution.values())
            content_idx = np.random.choice(len(content_choices), p=content_probs)
            content_type = content_choices[content_idx]
            
            if content_type == ContentType.MOVIE:
                duration = int(np.random.normal(6600, 1800))  # ~110 min avg, 30 min std
                duration = max(3600, min(14400, duration))  # 1-4 hours
            elif content_type == ContentType.TV_SHOW:
                duration = int(np.random.normal(2700, 600))  # ~45 min avg
                duration = max(1200, min(4800, duration))  # 20-80 minutes
            elif content_type == ContentType.SHORT_FORM:
                duration = int(np.random.exponential(300))  # Short videos
                duration = max(30, min(1200, duration))  # 30 seconds to 20 minutes
            elif content_type == ContentType.DOCUMENTARY:
                duration = int(np.random.normal(5400, 1200))  # ~90 min avg
                duration = max(2400, min(10800, duration))  # 40 min to 3 hours
            else:
                duration = int(np.random.normal(3600, 900))  # Default ~60 min
                duration = max(300, min(7200, duration))
            
            title = fake.catch_phrase()
            genre = random.choice(genres)
            rating = random.choice(ratings)
            language = np.random.choice(languages, p=[0.4, 0.15, 0.1, 0.1, 0.08, 0.07, 0.05, 0.05])
            creator_id = f"creator_{uuid.uuid4().hex[:8]}"
            
            # Popularity follows power law (few very popular, many less popular)
            popularity_score = np.random.pareto(0.5)
            popularity_score = min(1.0, popularity_score / 10)  # Normalize to 0-1
            
            release_date = fake.date_time_between(start_date='-10y', end_date='now', tzinfo=timezone.utc)
            
            content = ContentCatalog(
                content_id=content_id,
                title=title,
                content_type=content_type,
                duration=duration,
                genre=genre,
                rating=rating,
                language=language,
                creator_id=creator_id,
                popularity_score=popularity_score,
                release_date=release_date
            )
            
            catalog.append(content)
        
        return catalog
    
    def _generate_ad_catalog(self) -> List[Dict[str, Any]]:
        """Generate advertisement catalog"""
        ads = []
        ad_types = list(AdType)
        
        for _ in range(500):  # 500 different ads
            ad_id = f"ad_{uuid.uuid4().hex[:8]}"
            ad_type = random.choice(ad_types)
            
            # Ad duration based on type
            if ad_type in [AdType.PRE_ROLL, AdType.MID_ROLL, AdType.POST_ROLL]:
                duration = random.choice([15, 30, 60])  # Standard video ad lengths
            else:
                duration = random.randint(5, 30)  # Banner/overlay ads
            
            ad = {
                'ad_id': ad_id,
                'ad_type': ad_type,
                'duration': duration,
                'advertiser_id': f"advertiser_{uuid.uuid4().hex[:8]}",
                'campaign_id': f"campaign_{uuid.uuid4().hex[:8]}",
                'creative_id': f"creative_{uuid.uuid4().hex[:8]}",
                'price': round(random.uniform(0.5, 50.0), 2)  # CPM price
            }
            
            ads.append(ad)
        
        return ads
    
    def _select_content_for_user(self, user: UserProfile) -> ContentCatalog:
        """Select content based on user preferences and popularity"""
        # Weight content by user preferences and popularity
        weights = []
        for content in self.content_catalog:
            # Base weight from popularity
            weight = content.popularity_score
            
            # Adjust by user's content preferences
            if content.genre in user.viewing_patterns['content_preferences']:
                weight *= (1 + user.viewing_patterns['content_preferences'][content.genre])
            
            # Subscription tier affects content access
            if user.subscription_tier == SubscriptionTier.FREE:
                # Free users have limited access to premium content
                if content.content_type in [ContentType.MOVIE, ContentType.LIVE_STREAM]:
                    weight *= 0.3
            
            weights.append(weight)
        
        # Normalize weights
        total_weight = sum(weights)
        if total_weight == 0:
            return random.choice(self.content_catalog)
        
        weights = [w / total_weight for w in weights]
        
        return np.random.choice(self.content_catalog, p=weights)
    
    def generate_user_session(self, user: UserProfile, session_start_time: datetime) -> List[Dict[str, Any]]:
        """Generate a complete user session with realistic event sequence"""
        events = []
        session_id = f"session_{uuid.uuid4().hex[:8]}"
        device_id = f"device_{uuid.uuid4().hex[:8]}"
        
        # Generate user agent and IP
        user_agent = self._generate_user_agent(user.preferred_platform, user.preferred_device)
        ip_address = fake.ipv4()
        
        current_time = session_start_time
        
        # Session start event
        session_start = SessionEvent(
            event_type=EventType.SESSION_START,
            session_id=session_id,
            device_id=device_id,
            ip_address=ip_address,
            user_agent=user_agent,
            device_type=user.preferred_device,
            platform=user.preferred_platform,
            app_version=self._generate_app_version(),
            event_timestamp=current_time,
            user_id=user.user_id,
            country=user.country,
            region=user.region,
            city=user.city,
            latitude=user.latitude,
            longitude=user.longitude,
            subscription_tier=user.subscription_tier,
            user_signup_date=user.signup_date
        )
        
        events.append(session_start.model_dump())
        
        # Calculate session duration based on user patterns
        session_freq = max(1, user.viewing_patterns['session_frequency'])  # Avoid division by zero
        base_duration = user.viewing_patterns['daily_watch_hours'] * 3600 / session_freq
        session_duration = max(300, int(np.random.exponential(base_duration)))  # Min 5 minutes
        session_end_time = current_time + timedelta(seconds=session_duration)
        
        videos_watched = 0
        
        # Generate video watching events
        while current_time < session_end_time:
            # Select content
            content = self._select_content_for_user(user)
            
            # Generate video events for this content
            video_events, watch_duration = self._generate_video_events(
                user, content, session_id, device_id, ip_address, user_agent, current_time
            )
            
            events.extend(video_events)
            videos_watched += 1
            
            # Add interaction events (based on engagement level)
            if random.random() < user.engagement_level:
                interaction_events = self._generate_interaction_events(
                    user, content, session_id, device_id, ip_address, user_agent,
                    current_time + timedelta(seconds=random.randint(0, watch_duration))
                )
                events.extend(interaction_events)
            
            current_time += timedelta(seconds=watch_duration + random.randint(5, 60))  # Break between videos
            
            # Check if user continues watching (binge behavior)
            if random.random() > user.viewing_patterns['binge_tendency']:
                break
        
        # Session end event
        session_end = SessionEvent(
            event_type=EventType.SESSION_END,
            session_id=session_id,
            device_id=device_id,
            ip_address=ip_address,
            user_agent=user_agent,
            device_type=user.preferred_device,
            platform=user.preferred_platform,
            app_version=session_start.app_version,
            event_timestamp=current_time,
            user_id=user.user_id,
            country=user.country,
            region=user.region,
            city=user.city,
            latitude=user.latitude,
            longitude=user.longitude,
            session_duration=int((current_time - session_start_time).total_seconds()),
            videos_watched=videos_watched,
            subscription_tier=user.subscription_tier
        )
        
        events.append(session_end.model_dump())
        
        return events
    
    def _generate_video_events(self, user: UserProfile, content: ContentCatalog, 
                             session_id: str, device_id: str, ip_address: str, 
                             user_agent: str, start_time: datetime) -> tuple:
        """Generate video playback events for a single video"""
        events = []
        current_time = start_time
        
        # Determine video quality
        quality_options = self.quality_by_device.get(user.preferred_device, 
                                                   {VideoQuality.Q_720P: 1.0})
        quality_choices = list(quality_options.keys())
        quality_probs = list(quality_options.values())
        quality_idx = np.random.choice(len(quality_choices), p=quality_probs)
        video_quality = quality_choices[quality_idx]
        
        # Video play event
        play_event = VideoEvent(
            event_type=EventType.VIDEO_PLAY,
            video_id=content.content_id,
            content_title=content.title,
            content_type=content.content_type,
            content_duration=content.duration,
            content_genre=content.genre,
            content_rating=content.rating,
            content_language=content.language,
            content_creator_id=content.creator_id,
            playback_position=0,
            video_quality=video_quality,
            session_id=session_id,
            device_id=device_id,
            ip_address=ip_address,
            user_agent=user_agent,
            device_type=user.preferred_device,
            platform=user.preferred_platform,
            app_version=user_agent.split('/')[-1] if '/' in user_agent else '1.0.0',
            event_timestamp=current_time,
            user_id=user.user_id,
            country=user.country,
            region=user.region,
            city=user.city,
            latitude=user.latitude,
            longitude=user.longitude,
            startup_time=random.uniform(0.5, 3.0),
            bitrate=self._get_bitrate_for_quality(video_quality),
            connection_type=random.choice(['wifi', 'cellular', 'ethernet']),
            bandwidth=random.uniform(5.0, 100.0)
        )
        
        events.append(play_event.model_dump())
        current_time += timedelta(seconds=1)
        
        # Simulate playback with realistic patterns
        position = 0
        watch_duration = 0
        completed = False
        
        # Add ads for non-premium users
        if user.subscription_tier in [SubscriptionTier.FREE, SubscriptionTier.BASIC]:
            ad_events = self._generate_ad_events(
                user, content, session_id, device_id, ip_address, user_agent, current_time
            )
            events.extend(ad_events)
            current_time += timedelta(seconds=30)  # Assume 30s ad
        
        while position < content.duration:
            # Probability of continuing to watch (decreases over time)
            continue_probability = max(0.1, 1.0 - (position / content.duration) * 0.7)
            
            if random.random() > continue_probability:
                # User stops watching
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_STOP
                play_data['playback_position'] = position
                play_data['event_timestamp'] = current_time
                stop_event = VideoEvent(**play_data)
                events.append(stop_event.model_dump())
                break
            
            # Random events during playback
            watch_segment = random.randint(30, 300)  # 30s to 5min segments
            position = min(position + watch_segment, content.duration)
            current_time += timedelta(seconds=watch_segment)
            watch_duration += watch_segment
            
            # Random pause/resume
            if random.random() < 0.1:  # 10% chance of pause
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_PAUSE
                play_data['playback_position'] = position
                play_data['event_timestamp'] = current_time
                pause_event = VideoEvent(**play_data)
                events.append(pause_event.model_dump())
                
                # Pause duration
                pause_duration = random.randint(5, 120)
                current_time += timedelta(seconds=pause_duration)
                
                # Resume
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_PLAY
                play_data['playback_position'] = position
                play_data['event_timestamp'] = current_time
                resume_event = VideoEvent(**play_data)
                events.append(resume_event.model_dump())
            
            # Random seek
            if random.random() < 0.05:  # 5% chance of seek
                seek_from = position
                seek_to = random.randint(0, max(1, content.duration - 1))
                
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_SEEK
                play_data['playback_position'] = seek_to
                play_data['seek_from_position'] = seek_from
                play_data['seek_to_position'] = seek_to
                play_data['event_timestamp'] = current_time
                seek_event = VideoEvent(**play_data)
                events.append(seek_event.model_dump())
                position = seek_to
            
            # Buffer event (occasional)
            if random.random() < 0.02:  # 2% chance of buffering
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_BUFFER
                play_data['playback_position'] = position
                play_data['buffer_duration'] = random.uniform(1.0, 10.0)
                play_data['event_timestamp'] = current_time
                buffer_event = VideoEvent(**play_data)
                events.append(buffer_event.model_dump())
            
            # Quality change (occasional)
            if random.random() < 0.03:  # 3% chance of quality change
                old_quality = video_quality
                video_quality = random.choice(list(VideoQuality))
                
                play_data = play_event.model_dump()
                play_data['event_type'] = EventType.VIDEO_QUALITY_CHANGE
                play_data['playback_position'] = position
                play_data['video_quality'] = video_quality
                play_data['event_timestamp'] = current_time
                quality_event = VideoEvent(**play_data)
                events.append(quality_event.model_dump())
        
        # Video completion
        if position >= content.duration * 0.95:  # Consider 95% as complete
            play_data = play_event.model_dump()
            play_data['event_type'] = EventType.VIDEO_COMPLETE
            play_data['playback_position'] = content.duration
            play_data['event_timestamp'] = current_time
            complete_event = VideoEvent(**play_data)
            events.append(complete_event.model_dump())
            completed = True
            watch_duration = content.duration
        
        return events, watch_duration
    
    def _generate_interaction_events(self, user: UserProfile, content: ContentCatalog,
                                   session_id: str, device_id: str, ip_address: str,
                                   user_agent: str, event_time: datetime) -> List[Dict[str, Any]]:
        """Generate user interaction events"""
        events = []
        
        # Probability of different interactions based on engagement
        interaction_probs = {
            EventType.USER_LIKE: user.engagement_level * 0.3,
            EventType.USER_COMMENT: user.engagement_level * 0.1,
            EventType.USER_SHARE: user.engagement_level * 0.05,
            EventType.USER_SUBSCRIBE: user.engagement_level * 0.02,
            EventType.USER_BOOKMARK: user.engagement_level * 0.08
        }
        
        for event_type, probability in interaction_probs.items():
            if random.random() < probability:
                interaction_event = UserInteractionEvent(
                    event_type=event_type,
                    content_id=content.content_id,
                    content_type=content.content_type,
                    content_creator_id=content.creator_id,
                    session_id=session_id,
                    device_id=device_id,
                    ip_address=ip_address,
                    user_agent=user_agent,
                    device_type=user.preferred_device,
                    platform=user.preferred_platform,
                    app_version=user_agent.split('/')[-1] if '/' in user_agent else '1.0.0',
                    event_timestamp=event_time,
                    user_id=user.user_id,
                    country=user.country,
                    region=user.region,
                    city=user.city,
                    latitude=user.latitude,
                    longitude=user.longitude,
                    subscription_tier=user.subscription_tier
                )
                
                # Add specific details for comment events
                if event_type == EventType.USER_COMMENT:
                    interaction_event.comment_text = fake.text(max_nb_chars=200)
                elif event_type == EventType.USER_SHARE:
                    interaction_event.share_platform = random.choice(['facebook', 'twitter', 'instagram', 'whatsapp'])
                
                events.append(interaction_event.model_dump())
        
        return events
    
    def _generate_ad_events(self, user: UserProfile, content: ContentCatalog,
                          session_id: str, device_id: str, ip_address: str,
                          user_agent: str, event_time: datetime) -> List[Dict[str, Any]]:
        """Generate advertisement events"""
        events = []
        
        # Select random ad
        ad = random.choice(self.ad_catalog)
        
        # Ad impression
        impression_event = AdEvent(
            event_type=EventType.AD_IMPRESSION,
            ad_id=ad['ad_id'],
            ad_type=ad['ad_type'],
            ad_duration=ad['duration'],
            ad_position=0,
            ad_creative_id=ad['creative_id'],
            ad_campaign_id=ad['campaign_id'],
            advertiser_id=ad['advertiser_id'],
            content_id=content.content_id,
            content_type=content.content_type,
            session_id=session_id,
            device_id=device_id,
            ip_address=ip_address,
            user_agent=user_agent,
            device_type=user.preferred_device,
            platform=user.preferred_platform,
            app_version=user_agent.split('/')[-1] if '/' in user_agent else '1.0.0',
            event_timestamp=event_time,
            user_id=user.user_id,
            country=user.country,
            region=user.region,
            city=user.city,
            latitude=user.latitude,
            longitude=user.longitude,
            ad_price=ad['price'],
            currency='USD'
        )
        
        events.append(impression_event.model_dump())
        
        # Ad interactions
        if random.random() < 0.02:  # 2% click-through rate
            impression_data = impression_event.model_dump()
            impression_data['event_type'] = EventType.AD_CLICK
            impression_data['click_position'] = random.randint(0, ad['duration'])
            click_event = AdEvent(**impression_data)
            events.append(click_event.model_dump())
        
        if random.random() < 0.15:  # 15% skip rate
            impression_data = impression_event.model_dump()
            impression_data['event_type'] = EventType.AD_SKIP
            impression_data['skip_position'] = random.randint(5, ad['duration'])
            skip_event = AdEvent(**impression_data)
            events.append(skip_event.model_dump())
        else:
            # Ad completion
            impression_data = impression_event.model_dump()
            impression_data['event_type'] = EventType.AD_COMPLETE
            impression_data['view_duration'] = ad['duration']
            complete_event = AdEvent(**impression_data)
            events.append(complete_event.model_dump())
        
        return events
    
    def _generate_user_agent(self, platform: Platform, device: DeviceType) -> str:
        """Generate realistic user agent string"""
        version = f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
        
        if platform == Platform.IOS:
            return f"VideoApp/{version} (iOS; iPhone; iOS {random.randint(13, 17)}.{random.randint(0, 9)})"
        elif platform == Platform.ANDROID:
            return f"VideoApp/{version} (Android; {device.value}; Android {random.randint(8, 14)})"
        elif platform == Platform.WEB:
            browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
            browser = random.choice(browsers)
            return f"Mozilla/5.0 VideoApp/{version} {browser}/{random.randint(90, 120)}.0"
        else:
            return f"VideoApp/{version} ({platform.value}; {device.value})"
    
    def _generate_app_version(self) -> str:
        """Generate app version"""
        return f"{random.randint(1, 5)}.{random.randint(0, 20)}.{random.randint(0, 99)}"
    
    def _get_bitrate_for_quality(self, quality: VideoQuality) -> int:
        """Get realistic bitrate for video quality"""
        bitrates = {
            VideoQuality.Q_144P: random.randint(80, 200),
            VideoQuality.Q_240P: random.randint(200, 400),
            VideoQuality.Q_360P: random.randint(400, 800),
            VideoQuality.Q_480P: random.randint(800, 1500),
            VideoQuality.Q_720P: random.randint(1500, 3000),
            VideoQuality.Q_1080P: random.randint(3000, 6000),
            VideoQuality.Q_1440P: random.randint(6000, 12000),
            VideoQuality.Q_4K: random.randint(12000, 25000),
            VideoQuality.AUTO: random.randint(1000, 5000)
        }
        return bitrates.get(quality, 2000)
    
    def generate_streaming_events(self, events_per_second: int = 100, 
                                duration_hours: int = 1) -> Generator[str, None, None]:
        """
        Generate continuous streaming events at specified rate
        
        Args:
            events_per_second: Target events per second
            duration_hours: Duration to generate events
            
        Yields:
            JSON string of event data
        """
        total_events = events_per_second * duration_hours * 3600
        events_generated = 0
        start_time = datetime.now(timezone.utc)
        
        logger.info(f"Starting to generate {total_events} events over {duration_hours} hours")
        
        while events_generated < total_events:
            # Select random user
            user = random.choice(self.user_profiles)
            
            # Check if it's a good time for this user to watch
            current_hour = datetime.now().hour
            if current_hour in user.viewing_patterns['preferred_times'] or random.random() < 0.1:
                # Generate session events
                session_time = start_time + timedelta(seconds=events_generated / events_per_second)
                session_events = self.generate_user_session(user, session_time)
                
                for event in session_events:
                    if events_generated >= total_events:
                        break
                    
                    yield json.dumps(event, default=str)
                    events_generated += 1
                    
                    # Rate limiting
                    if events_generated % events_per_second == 0:
                        time.sleep(1)
            else:
                # Generate single event for inactive period
                event_types = [EventType.SESSION_HEARTBEAT, EventType.AD_IMPRESSION]
                event_type = random.choice(event_types)
                
                # Simple event generation for inactive periods
                event = {
                    'event_id': str(uuid.uuid4()),
                    'event_type': event_type,
                    'event_timestamp': (start_time + timedelta(seconds=events_generated / events_per_second)).isoformat(),
                    'user_id': user.user_id,
                    'session_id': f"session_{uuid.uuid4().hex[:8]}",
                    'device_id': f"device_{uuid.uuid4().hex[:8]}"
                }
                
                yield json.dumps(event, default=str)
                events_generated += 1
                
                if events_generated % events_per_second == 0:
                    time.sleep(1)
        
        logger.info(f"Generated {events_generated} events")
    
    def generate_batch_data(self, start_date: datetime, end_date: datetime, 
                          output_file: str = None) -> List[Dict[str, Any]]:
        """
        Generate historical batch data for a date range
        
        Args:
            start_date: Start date for data generation
            end_date: End date for data generation
            output_file: Optional file to save data
            
        Returns:
            List of event dictionaries
        """
        all_events = []
        current_date = start_date
        
        logger.info(f"Generating batch data from {start_date} to {end_date}")
        
        while current_date < end_date:
            # Generate events for each day
            daily_sessions = random.randint(100, 500)  # Variable daily activity
            
            for _ in range(daily_sessions):
                user = random.choice(self.user_profiles)
                
                # Random time during the day
                session_time = current_date + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                session_events = self.generate_user_session(user, session_time)
                all_events.extend(session_events)
            
            current_date += timedelta(days=1)
            logger.info(f"Generated events for {current_date.date()}")
        
        if output_file:
            with open(output_file, 'w') as f:
                for event in all_events:
                    f.write(json.dumps(event, default=str) + '\n')
            
            logger.info(f"Saved {len(all_events)} events to {output_file}")
        
        return all_events


def main():
    """Main function for testing data generation"""
    generator = VideoStreamingDataGenerator(num_users=100, num_content=1000)
    
    # Generate streaming events for 1 hour at 10 events per second
    event_count = 0
    for event_json in generator.generate_streaming_events(events_per_second=10, duration_hours=1):
        event_count += 1
        if event_count <= 10:  # Print first 10 events
            print(event_json)
        
        if event_count % 1000 == 0:
            logger.info(f"Generated {event_count} events")
    
    logger.info(f"Total events generated: {event_count}")


if __name__ == "__main__":
    main()
