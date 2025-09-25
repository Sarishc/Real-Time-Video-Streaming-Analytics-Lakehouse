"""
Event Schemas for Video Streaming Analytics Lakehouse

This module defines Pydantic models for all event types in the video streaming platform.
These schemas ensure data validation, type safety, and consistent event structure.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Literal
from enum import Enum
from pydantic import BaseModel, Field, validator, root_validator
from uuid import UUID, uuid4
import json


class EventType(str, Enum):
    """Event type enumeration"""
    # Video events
    VIDEO_PLAY = "video_play"
    VIDEO_PAUSE = "video_pause"
    VIDEO_STOP = "video_stop"
    VIDEO_SEEK = "video_seek"
    VIDEO_BUFFER = "video_buffer"
    VIDEO_ERROR = "video_error"
    VIDEO_QUALITY_CHANGE = "video_quality_change"
    VIDEO_COMPLETE = "video_complete"
    
    # User interaction events
    USER_LIKE = "user_like"
    USER_DISLIKE = "user_dislike"
    USER_COMMENT = "user_comment"
    USER_SHARE = "user_share"
    USER_SUBSCRIBE = "user_subscribe"
    USER_UNSUBSCRIBE = "user_unsubscribe"
    USER_BOOKMARK = "user_bookmark"
    
    # Ad events
    AD_IMPRESSION = "ad_impression"
    AD_CLICK = "ad_click"
    AD_SKIP = "ad_skip"
    AD_COMPLETE = "ad_complete"
    AD_ERROR = "ad_error"
    
    # Session events
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    SESSION_HEARTBEAT = "session_heartbeat"
    DEVICE_CHANGE = "device_change"


class DeviceType(str, Enum):
    """Device type enumeration"""
    MOBILE = "mobile"
    TABLET = "tablet"
    DESKTOP = "desktop"
    TV = "tv"
    GAMING_CONSOLE = "gaming_console"
    SMART_SPEAKER = "smart_speaker"


class Platform(str, Enum):
    """Platform enumeration"""
    IOS = "ios"
    ANDROID = "android"
    WINDOWS = "windows"
    MACOS = "macos"
    LINUX = "linux"
    WEB = "web"
    ROKU = "roku"
    APPLE_TV = "apple_tv"
    ANDROID_TV = "android_tv"
    FIRE_TV = "fire_tv"


class VideoQuality(str, Enum):
    """Video quality enumeration"""
    Q_144P = "144p"
    Q_240P = "240p"
    Q_360P = "360p"
    Q_480P = "480p"
    Q_720P = "720p"
    Q_1080P = "1080p"
    Q_1440P = "1440p"
    Q_4K = "4k"
    Q_8K = "8k"
    AUTO = "auto"


class ContentType(str, Enum):
    """Content type enumeration"""
    MOVIE = "movie"
    TV_SHOW = "tv_show"
    DOCUMENTARY = "documentary"
    LIVE_STREAM = "live_stream"
    SHORT_FORM = "short_form"
    TRAILER = "trailer"
    ADVERTISEMENT = "advertisement"
    USER_GENERATED = "user_generated"


class AdType(str, Enum):
    """Advertisement type enumeration"""
    PRE_ROLL = "pre_roll"
    MID_ROLL = "mid_roll"
    POST_ROLL = "post_roll"
    BANNER = "banner"
    OVERLAY = "overlay"
    SPONSORED = "sponsored"


class SubscriptionTier(str, Enum):
    """User subscription tier"""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


class BaseEvent(BaseModel):
    """Base event model with common fields"""
    event_id: UUID = Field(default_factory=uuid4, description="Unique event identifier")
    event_type: EventType = Field(..., description="Type of event")
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Event timestamp in UTC")
    user_id: Optional[str] = Field(None, description="User identifier")
    session_id: str = Field(..., description="Session identifier")
    device_id: str = Field(..., description="Device identifier")
    ip_address: str = Field(..., description="User IP address")
    user_agent: str = Field(..., description="User agent string")
    
    # Geographic information
    country: Optional[str] = Field(None, description="Country code (ISO 3166-1)")
    region: Optional[str] = Field(None, description="Region/state")
    city: Optional[str] = Field(None, description="City")
    latitude: Optional[float] = Field(None, ge=-90, le=90, description="Latitude")
    longitude: Optional[float] = Field(None, ge=-180, le=180, description="Longitude")
    
    # Device and platform information
    device_type: DeviceType = Field(..., description="Type of device")
    platform: Platform = Field(..., description="Platform/OS")
    app_version: str = Field(..., description="Application version")
    
    # Additional context
    referrer: Optional[str] = Field(None, description="Referrer URL or source")
    campaign_id: Optional[str] = Field(None, description="Marketing campaign ID")
    ab_test_variant: Optional[str] = Field(None, description="A/B test variant")
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }
        use_enum_values = True


class VideoEvent(BaseEvent):
    """Video playback events"""
    event_type: Literal[
        EventType.VIDEO_PLAY,
        EventType.VIDEO_PAUSE,
        EventType.VIDEO_STOP,
        EventType.VIDEO_SEEK,
        EventType.VIDEO_BUFFER,
        EventType.VIDEO_ERROR,
        EventType.VIDEO_QUALITY_CHANGE,
        EventType.VIDEO_COMPLETE
    ]
    
    # Video content information
    video_id: str = Field(..., description="Video content identifier")
    content_title: str = Field(..., description="Video title")
    content_type: ContentType = Field(..., description="Type of content")
    content_duration: int = Field(..., gt=0, description="Total video duration in seconds")
    content_genre: Optional[str] = Field(None, description="Content genre")
    content_rating: Optional[str] = Field(None, description="Content rating (G, PG, R, etc.)")
    content_language: Optional[str] = Field(None, description="Content language code")
    content_creator_id: Optional[str] = Field(None, description="Content creator/channel ID")
    
    # Playback information
    playback_position: int = Field(..., ge=0, description="Current playback position in seconds")
    video_quality: VideoQuality = Field(..., description="Current video quality")
    audio_language: Optional[str] = Field(None, description="Audio language code")
    subtitle_language: Optional[str] = Field(None, description="Subtitle language code")
    
    # Performance metrics
    buffer_duration: Optional[float] = Field(None, ge=0, description="Buffer duration in seconds")
    startup_time: Optional[float] = Field(None, ge=0, description="Video startup time in seconds")
    bitrate: Optional[int] = Field(None, gt=0, description="Video bitrate in kbps")
    dropped_frames: Optional[int] = Field(None, ge=0, description="Number of dropped frames")
    
    # Network information
    connection_type: Optional[str] = Field(None, description="Connection type (wifi, cellular, etc.)")
    bandwidth: Optional[float] = Field(None, gt=0, description="Available bandwidth in Mbps")
    
    # Error information (for error events)
    error_code: Optional[str] = Field(None, description="Error code")
    error_message: Optional[str] = Field(None, description="Error description")
    
    # Seek information (for seek events)
    seek_from_position: Optional[int] = Field(None, ge=0, description="Position before seek")
    seek_to_position: Optional[int] = Field(None, ge=0, description="Position after seek")
    
    @validator('playback_position')
    def validate_playback_position(cls, v, values):
        if 'content_duration' in values and v > values['content_duration']:
            raise ValueError('playback_position cannot exceed content_duration')
        return v


class UserInteractionEvent(BaseEvent):
    """User interaction events (likes, comments, shares, etc.)"""
    event_type: Literal[
        EventType.USER_LIKE,
        EventType.USER_DISLIKE,
        EventType.USER_COMMENT,
        EventType.USER_SHARE,
        EventType.USER_SUBSCRIBE,
        EventType.USER_UNSUBSCRIBE,
        EventType.USER_BOOKMARK
    ]
    
    # Content information
    content_id: str = Field(..., description="Content identifier")
    content_type: ContentType = Field(..., description="Type of content")
    content_creator_id: Optional[str] = Field(None, description="Content creator/channel ID")
    
    # Interaction details
    interaction_context: Optional[str] = Field(None, description="Context of interaction")
    comment_text: Optional[str] = Field(None, max_length=5000, description="Comment text")
    share_platform: Optional[str] = Field(None, description="Platform shared to")
    share_url: Optional[str] = Field(None, description="Shared URL")
    
    # User subscription information
    subscription_tier: Optional[SubscriptionTier] = Field(None, description="User subscription tier")
    
    @validator('comment_text')
    def validate_comment_text(cls, v, values):
        if values.get('event_type') == EventType.USER_COMMENT and not v:
            raise ValueError('comment_text is required for comment events')
        return v


class AdEvent(BaseEvent):
    """Advertisement events"""
    event_type: Literal[
        EventType.AD_IMPRESSION,
        EventType.AD_CLICK,
        EventType.AD_SKIP,
        EventType.AD_COMPLETE,
        EventType.AD_ERROR
    ]
    
    # Ad information
    ad_id: str = Field(..., description="Advertisement identifier")
    ad_type: AdType = Field(..., description="Type of advertisement")
    ad_duration: int = Field(..., gt=0, description="Ad duration in seconds")
    ad_position: int = Field(..., ge=0, description="Ad position in content (seconds)")
    ad_creative_id: Optional[str] = Field(None, description="Creative asset identifier")
    ad_campaign_id: Optional[str] = Field(None, description="Campaign identifier")
    advertiser_id: Optional[str] = Field(None, description="Advertiser identifier")
    
    # Content context
    content_id: str = Field(..., description="Content where ad is shown")
    content_type: ContentType = Field(..., description="Type of content")
    
    # Ad performance
    view_duration: Optional[int] = Field(None, ge=0, description="How long ad was viewed (seconds)")
    skip_position: Optional[int] = Field(None, ge=0, description="Position where ad was skipped")
    click_position: Optional[int] = Field(None, ge=0, description="Position where ad was clicked")
    
    # Revenue information
    ad_price: Optional[float] = Field(None, ge=0, description="Ad price (CPM/CPC)")
    currency: Optional[str] = Field(None, description="Currency code")
    
    # Error information
    error_code: Optional[str] = Field(None, description="Error code")
    error_message: Optional[str] = Field(None, description="Error description")


class SessionEvent(BaseEvent):
    """Session and device events"""
    event_type: Literal[
        EventType.SESSION_START,
        EventType.SESSION_END,
        EventType.SESSION_HEARTBEAT,
        EventType.DEVICE_CHANGE
    ]
    
    # Session information
    session_duration: Optional[int] = Field(None, ge=0, description="Session duration in seconds")
    page_views: Optional[int] = Field(None, ge=0, description="Number of page views in session")
    videos_watched: Optional[int] = Field(None, ge=0, description="Number of videos watched")
    
    # User information
    subscription_tier: Optional[SubscriptionTier] = Field(None, description="User subscription tier")
    user_signup_date: Optional[datetime] = Field(None, description="User signup date")
    user_tier_change_date: Optional[datetime] = Field(None, description="Last tier change date")
    
    # Device change information
    previous_device_id: Optional[str] = Field(None, description="Previous device ID")
    previous_device_type: Optional[DeviceType] = Field(None, description="Previous device type")
    
    # Performance metrics
    app_crashes: Optional[int] = Field(None, ge=0, description="Number of app crashes in session")
    network_errors: Optional[int] = Field(None, ge=0, description="Number of network errors")


class EventEncoder:
    """Event encoder for serialization"""
    
    @staticmethod
    def encode_event(event: BaseEvent) -> str:
        """Encode event to JSON string"""
        return event.json()
    
    @staticmethod
    def encode_events(events: List[BaseEvent]) -> List[str]:
        """Encode multiple events to JSON strings"""
        return [event.json() for event in events]
    
    @staticmethod
    def decode_event(event_data: str) -> BaseEvent:
        """Decode JSON string to event object"""
        data = json.loads(event_data)
        event_type = data.get('event_type')
        
        if event_type in [e.value for e in EventType if e.value.startswith('video_')]:
            return VideoEvent(**data)
        elif event_type in [e.value for e in EventType if e.value.startswith('user_')]:
            return UserInteractionEvent(**data)
        elif event_type in [e.value for e in EventType if e.value.startswith('ad_')]:
            return AdEvent(**data)
        elif event_type in [e.value for e in EventType if e.value.startswith('session_')]:
            return SessionEvent(**data)
        else:
            return BaseEvent(**data)


class EventValidator:
    """Event validation utilities"""
    
    @staticmethod
    def validate_event_sequence(events: List[BaseEvent]) -> List[str]:
        """Validate sequence of events for logical consistency"""
        errors = []
        session_events = {}
        
        for event in sorted(events, key=lambda x: x.event_timestamp):
            session_id = event.session_id
            
            if session_id not in session_events:
                session_events[session_id] = {
                    'started': False,
                    'ended': False,
                    'events': []
                }
            
            session_data = session_events[session_id]
            session_data['events'].append(event)
            
            # Check session start/end logic
            if isinstance(event, SessionEvent):
                if event.event_type == EventType.SESSION_START:
                    if session_data['started']:
                        errors.append(f"Multiple session_start events for session {session_id}")
                    session_data['started'] = True
                elif event.event_type == EventType.SESSION_END:
                    if not session_data['started']:
                        errors.append(f"session_end without session_start for session {session_id}")
                    session_data['ended'] = True
            else:
                # Non-session events should have started session
                if not session_data['started']:
                    errors.append(f"Event {event.event_type} without session_start for session {session_id}")
            
            # Check if events after session end
            if session_data['ended'] and not isinstance(event, SessionEvent):
                errors.append(f"Event {event.event_type} after session_end for session {session_id}")
        
        return errors
    
    @staticmethod
    def validate_video_playback_sequence(events: List[VideoEvent]) -> List[str]:
        """Validate video playback event sequence"""
        errors = []
        video_sessions = {}
        
        for event in sorted(events, key=lambda x: x.event_timestamp):
            key = f"{event.session_id}_{event.video_id}"
            
            if key not in video_sessions:
                video_sessions[key] = {
                    'started': False,
                    'completed': False,
                    'last_position': 0
                }
            
            session_data = video_sessions[key]
            
            if event.event_type == EventType.VIDEO_PLAY:
                session_data['started'] = True
            elif event.event_type == EventType.VIDEO_COMPLETE:
                session_data['completed'] = True
            
            # Check playback position progression
            if event.playback_position < session_data['last_position']:
                if event.event_type != EventType.VIDEO_SEEK:
                    errors.append(f"Playback position regression without seek for {key}")
            
            session_data['last_position'] = event.playback_position
        
        return errors


# Event factory for testing and data generation
class EventFactory:
    """Factory for creating test events"""
    
    @staticmethod
    def create_video_play_event(**kwargs) -> VideoEvent:
        """Create a video play event"""
        defaults = {
            'event_type': EventType.VIDEO_PLAY,
            'video_id': 'video_123',
            'content_title': 'Sample Video',
            'content_type': ContentType.MOVIE,
            'content_duration': 7200,
            'playback_position': 0,
            'video_quality': VideoQuality.Q_1080P,
            'session_id': 'session_123',
            'device_id': 'device_123',
            'ip_address': '192.168.1.1',
            'user_agent': 'VideoApp/1.0',
            'device_type': DeviceType.MOBILE,
            'platform': Platform.IOS,
            'app_version': '1.0.0'
        }
        defaults.update(kwargs)
        return VideoEvent(**defaults)
    
    @staticmethod
    def create_user_interaction_event(**kwargs) -> UserInteractionEvent:
        """Create a user interaction event"""
        defaults = {
            'event_type': EventType.USER_LIKE,
            'content_id': 'video_123',
            'content_type': ContentType.MOVIE,
            'session_id': 'session_123',
            'device_id': 'device_123',
            'ip_address': '192.168.1.1',
            'user_agent': 'VideoApp/1.0',
            'device_type': DeviceType.MOBILE,
            'platform': Platform.IOS,
            'app_version': '1.0.0'
        }
        defaults.update(kwargs)
        return UserInteractionEvent(**defaults)
    
    @staticmethod
    def create_ad_event(**kwargs) -> AdEvent:
        """Create an ad event"""
        defaults = {
            'event_type': EventType.AD_IMPRESSION,
            'ad_id': 'ad_123',
            'ad_type': AdType.PRE_ROLL,
            'ad_duration': 30,
            'ad_position': 0,
            'content_id': 'video_123',
            'content_type': ContentType.MOVIE,
            'session_id': 'session_123',
            'device_id': 'device_123',
            'ip_address': '192.168.1.1',
            'user_agent': 'VideoApp/1.0',
            'device_type': DeviceType.MOBILE,
            'platform': Platform.IOS,
            'app_version': '1.0.0'
        }
        defaults.update(kwargs)
        return AdEvent(**defaults)
    
    @staticmethod
    def create_session_event(**kwargs) -> SessionEvent:
        """Create a session event"""
        defaults = {
            'event_type': EventType.SESSION_START,
            'session_id': 'session_123',
            'device_id': 'device_123',
            'ip_address': '192.168.1.1',
            'user_agent': 'VideoApp/1.0',
            'device_type': DeviceType.MOBILE,
            'platform': Platform.IOS,
            'app_version': '1.0.0'
        }
        defaults.update(kwargs)
        return SessionEvent(**defaults)
