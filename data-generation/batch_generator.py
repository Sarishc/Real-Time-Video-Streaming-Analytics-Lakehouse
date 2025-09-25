"""
Historical Batch Data Generator for Video Streaming Analytics

This module generates large volumes of historical data for backtesting,
model training, and analytics development. It creates realistic time-series
data with seasonal patterns, trends, and user behavior evolution.
"""

import os
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
from dataclasses import dataclass
import argparse

from data_generator import VideoStreamingDataGenerator, UserProfile
from schemas import EventType, SubscriptionTier, ContentType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class BatchConfig:
    """Configuration for batch data generation"""
    start_date: datetime
    end_date: datetime
    num_users: int
    num_content: int
    base_events_per_day: int
    output_directory: str
    file_format: str = 'json'  # json, parquet, csv
    partition_by: str = 'date'  # date, month, year
    compress: bool = True
    parallel_workers: int = mp.cpu_count()
    seasonal_patterns: bool = True
    growth_simulation: bool = True


class SeasonalPatterns:
    """Seasonal and temporal pattern modeling"""
    
    @staticmethod
    def get_daily_multiplier(date: datetime) -> float:
        """Get daily activity multiplier based on day of week"""
        # Higher activity on weekends
        weekday = date.weekday()
        multipliers = {
            0: 1.0,   # Monday
            1: 0.95,  # Tuesday
            2: 0.9,   # Wednesday
            3: 0.95,  # Thursday
            4: 1.1,   # Friday
            5: 1.3,   # Saturday
            6: 1.2    # Sunday
        }
        return multipliers.get(weekday, 1.0)
    
    @staticmethod
    def get_hourly_multiplier(hour: int) -> float:
        """Get hourly activity multiplier"""
        # Peak hours: 19-23 (evening), 12-13 (lunch)
        if 19 <= hour <= 23:
            return 2.0
        elif hour in [12, 13]:
            return 1.3
        elif 14 <= hour <= 18:
            return 1.1
        elif 8 <= hour <= 11:
            return 0.8
        elif 0 <= hour <= 6:
            return 0.3
        else:
            return 0.6
    
    @staticmethod
    def get_monthly_multiplier(month: int) -> float:
        """Get monthly activity multiplier (seasonal effects)"""
        # Higher activity in winter months (holiday season)
        # Lower in summer (people outside more)
        multipliers = {
            1: 1.3,   # January - post-holiday binge
            2: 1.1,   # February
            3: 1.0,   # March
            4: 0.9,   # April - spring
            5: 0.8,   # May
            6: 0.7,   # June - summer start
            7: 0.7,   # July - summer
            8: 0.8,   # August
            9: 0.9,   # September - back to school/work
            10: 1.0,  # October
            11: 1.2,  # November - thanksgiving
            12: 1.4   # December - holidays
        }
        return multipliers.get(month, 1.0)
    
    @staticmethod
    def get_holiday_multiplier(date: datetime) -> float:
        """Get activity multiplier for holidays"""
        # Define major holidays (simplified)
        holidays = [
            (1, 1),   # New Year's Day
            (7, 4),   # Independence Day
            (11, 25), # Thanksgiving (simplified)
            (12, 25), # Christmas
            (12, 31)  # New Year's Eve
        ]
        
        if (date.month, date.day) in holidays:
            return 1.5
        
        # Weekend of holidays
        for month, day in holidays:
            holiday_date = datetime(date.year, month, day)
            if abs((date - holiday_date).days) <= 1 and date.weekday() in [5, 6]:
                return 1.3
        
        return 1.0


class UserEvolutionSimulator:
    """Simulates user behavior evolution over time"""
    
    def __init__(self):
        self.user_lifecycles = {}
    
    def evolve_user_profile(self, user: UserProfile, current_date: datetime) -> UserProfile:
        """Evolve user profile based on time and usage patterns"""
        user_age_days = (current_date - user.signup_date).days
        
        # User engagement changes over time
        if user_age_days < 30:
            # New users are highly engaged
            user.engagement_level = min(1.0, user.engagement_level * 1.5)
        elif user_age_days < 90:
            # Engagement stabilizes
            user.engagement_level = user.engagement_level * 0.95
        elif user_age_days > 365:
            # Long-term users might decrease engagement
            user.engagement_level = max(0.1, user.engagement_level * 0.9)
        
        # Subscription tier evolution
        if user_age_days > 60 and user.subscription_tier == SubscriptionTier.FREE:
            if np.random.random() < 0.1:  # 10% chance to upgrade after 2 months
                user.subscription_tier = SubscriptionTier.BASIC
                user.user_tier_change_date = current_date
        
        if user_age_days > 180 and user.subscription_tier == SubscriptionTier.BASIC:
            if np.random.random() < 0.05:  # 5% chance to upgrade to premium
                user.subscription_tier = SubscriptionTier.PREMIUM
                user.user_tier_change_date = current_date
        
        # Viewing pattern evolution
        if user_age_days > 30:
            # Users discover new content preferences over time
            if np.random.random() < 0.1:
                genres = list(user.viewing_patterns['content_preferences'].keys())
                random_genre = np.random.choice(genres)
                user.viewing_patterns['content_preferences'][random_genre] *= 1.1
                
                # Normalize preferences
                total = sum(user.viewing_patterns['content_preferences'].values())
                for genre in user.viewing_patterns['content_preferences']:
                    user.viewing_patterns['content_preferences'][genre] /= total
        
        return user


class BatchDataGenerator:
    """
    High-performance batch data generator with realistic temporal patterns,
    user evolution, and scalable parallel processing.
    """
    
    def __init__(self, config: BatchConfig, seed: int = 42):
        """Initialize batch data generator"""
        self.config = config
        self.seasonal_patterns = SeasonalPatterns()
        self.user_evolution = UserEvolutionSimulator()
        
        # Initialize base generator
        self.base_generator = VideoStreamingDataGenerator(
            num_users=config.num_users,
            num_content=config.num_content,
            seed=seed
        )
        
        # Create output directory
        Path(config.output_directory).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized batch generator for {config.start_date} to {config.end_date}")
        logger.info(f"Users: {config.num_users}, Content: {config.num_content}")
        logger.info(f"Base events per day: {config.base_events_per_day}")
    
    def calculate_daily_events(self, date: datetime) -> int:
        """Calculate number of events for a specific date"""
        base_events = self.config.base_events_per_day
        
        if not self.config.seasonal_patterns:
            return base_events
        
        # Apply temporal multipliers
        daily_mult = self.seasonal_patterns.get_daily_multiplier(date)
        monthly_mult = self.seasonal_patterns.get_monthly_multiplier(date.month)
        holiday_mult = self.seasonal_patterns.get_holiday_multiplier(date)
        
        # Platform growth simulation
        growth_mult = 1.0
        if self.config.growth_simulation:
            days_since_start = (date - self.config.start_date).days
            total_days = (self.config.end_date - self.config.start_date).days
            # Simulate 50% growth over the period
            growth_mult = 1.0 + (0.5 * days_since_start / total_days)
        
        # Add some random variation
        random_mult = np.random.uniform(0.8, 1.2)
        
        total_multiplier = daily_mult * monthly_mult * holiday_mult * growth_mult * random_mult
        return int(base_events * total_multiplier)
    
    def generate_day_data(self, date: datetime) -> List[Dict[str, Any]]:
        """Generate all events for a single day"""
        daily_events = self.calculate_daily_events(date)
        events = []
        
        # Distribute events throughout the day based on hourly patterns
        hourly_distribution = []
        for hour in range(24):
            hourly_mult = self.seasonal_patterns.get_hourly_multiplier(hour)
            hourly_distribution.append(hourly_mult)
        
        # Normalize to get event distribution
        total_hourly = sum(hourly_distribution)
        hourly_events = [int(daily_events * (mult / total_hourly)) for mult in hourly_distribution]
        
        # Generate events for each hour
        for hour, event_count in enumerate(hourly_events):
            if event_count == 0:
                continue
            
            # Evolve user profiles for this date
            evolved_users = []
            for user in self.base_generator.user_profiles:
                evolved_user = self.user_evolution.evolve_user_profile(user, date)
                evolved_users.append(evolved_user)
            
            # Generate sessions for this hour
            sessions_this_hour = max(1, event_count // 20)  # ~20 events per session avg
            
            for _ in range(sessions_this_hour):
                user = np.random.choice(evolved_users)
                
                # Random time within the hour
                session_time = date.replace(hour=hour) + timedelta(
                    minutes=np.random.randint(0, 60),
                    seconds=np.random.randint(0, 60)
                )
                
                try:
                    session_events = self.base_generator.generate_user_session(user, session_time)
                    events.extend(session_events)
                except Exception as e:
                    logger.error(f"Error generating session for {session_time}: {e}")
                    continue
        
        logger.info(f"Generated {len(events)} events for {date.date()}")
        return events
    
    def save_day_data(self, date: datetime, events: List[Dict[str, Any]]) -> str:
        """Save events data for a single day"""
        if self.config.partition_by == 'date':
            partition_path = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"
        elif self.config.partition_by == 'month':
            partition_path = f"year={date.year}/month={date.month:02d}"
        elif self.config.partition_by == 'year':
            partition_path = f"year={date.year}"
        else:
            partition_path = ""
        
        output_dir = Path(self.config.output_directory) / partition_path
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate filename
        date_str = date.strftime("%Y-%m-%d")
        
        if self.config.file_format == 'json':
            filename = f"events_{date_str}.json"
            if self.config.compress:
                filename += ".gz"
            
            filepath = output_dir / filename
            
            if self.config.compress:
                import gzip
                with gzip.open(filepath, 'wt') as f:
                    for event in events:
                        f.write(json.dumps(event, default=str) + '\n')
            else:
                with open(filepath, 'w') as f:
                    for event in events:
                        f.write(json.dumps(event, default=str) + '\n')
        
        elif self.config.file_format == 'parquet':
            filename = f"events_{date_str}.parquet"
            if self.config.compress:
                filename = f"events_{date_str}.parquet.gz"
            
            filepath = output_dir / filename
            
            # Convert to DataFrame and save as Parquet
            df = pd.DataFrame(events)
            
            # Convert datetime columns
            datetime_cols = [col for col in df.columns if 'timestamp' in col or 'date' in col]
            for col in datetime_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            
            compression = 'gzip' if self.config.compress else None
            df.to_parquet(filepath, compression=compression, index=False)
        
        elif self.config.file_format == 'csv':
            filename = f"events_{date_str}.csv"
            if self.config.compress:
                filename += ".gz"
            
            filepath = output_dir / filename
            
            df = pd.DataFrame(events)
            compression = 'gzip' if self.config.compress else None
            df.to_csv(filepath, compression=compression, index=False)
        
        return str(filepath)
    
    def generate_parallel(self) -> List[str]:
        """Generate batch data using parallel processing"""
        date_range = []
        current_date = self.config.start_date
        
        while current_date <= self.config.end_date:
            date_range.append(current_date)
            current_date += timedelta(days=1)
        
        logger.info(f"Generating data for {len(date_range)} days using {self.config.parallel_workers} workers")
        
        output_files = []
        
        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
            # Submit all days for processing
            future_to_date = {
                executor.submit(self._process_single_day, date): date 
                for date in date_range
            }
            
            # Collect results
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    filepath = future.result()
                    output_files.append(filepath)
                    logger.info(f"Completed processing for {date.date()}")
                except Exception as e:
                    logger.error(f"Error processing {date.date()}: {e}")
        
        return output_files
    
    def _process_single_day(self, date: datetime) -> str:
        """Process a single day (for parallel execution)"""
        events = self.generate_day_data(date)
        filepath = self.save_day_data(date, events)
        return filepath
    
    def generate_summary_stats(self, output_files: List[str]) -> Dict[str, Any]:
        """Generate summary statistics for the generated data"""
        total_events = 0
        total_users = set()
        total_content = set()
        event_type_counts = {}
        daily_counts = []
        
        for filepath in output_files:
            try:
                if self.config.file_format == 'json':
                    if filepath.endswith('.gz'):
                        import gzip
                        with gzip.open(filepath, 'rt') as f:
                            events = [json.loads(line) for line in f]
                    else:
                        with open(filepath, 'r') as f:
                            events = [json.loads(line) for line in f]
                
                elif self.config.file_format == 'parquet':
                    df = pd.read_parquet(filepath)
                    events = df.to_dict('records')
                
                elif self.config.file_format == 'csv':
                    df = pd.read_csv(filepath)
                    events = df.to_dict('records')
                
                daily_count = len(events)
                total_events += daily_count
                daily_counts.append(daily_count)
                
                for event in events:
                    if 'user_id' in event and event['user_id']:
                        total_users.add(event['user_id'])
                    
                    if 'video_id' in event and event['video_id']:
                        total_content.add(event['video_id'])
                    elif 'content_id' in event and event['content_id']:
                        total_content.add(event['content_id'])
                    
                    event_type = event.get('event_type', 'unknown')
                    event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
                
            except Exception as e:
                logger.error(f"Error processing {filepath} for stats: {e}")
        
        stats = {
            'total_events': total_events,
            'total_files': len(output_files),
            'unique_users': len(total_users),
            'unique_content': len(total_content),
            'date_range': {
                'start': self.config.start_date.isoformat(),
                'end': self.config.end_date.isoformat(),
                'days': (self.config.end_date - self.config.start_date).days + 1
            },
            'daily_stats': {
                'mean_events': np.mean(daily_counts),
                'median_events': np.median(daily_counts),
                'min_events': np.min(daily_counts),
                'max_events': np.max(daily_counts),
                'std_events': np.std(daily_counts)
            },
            'event_type_distribution': event_type_counts,
            'events_per_second_avg': total_events / ((self.config.end_date - self.config.start_date).total_seconds()),
            'file_format': self.config.file_format,
            'compression': self.config.compress,
            'partition_by': self.config.partition_by
        }
        
        return stats
    
    def save_summary_stats(self, stats: Dict[str, Any]) -> str:
        """Save summary statistics to file"""
        stats_file = Path(self.config.output_directory) / "batch_generation_summary.json"
        
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        logger.info(f"Summary statistics saved to {stats_file}")
        return str(stats_file)


def main():
    """Main function for command-line usage"""
    parser = argparse.ArgumentParser(description="Generate historical video streaming data")
    
    parser.add_argument('--start-date', type=str, required=True,
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=True,
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, required=True,
                       help='Output directory')
    parser.add_argument('--num-users', type=int, default=10000,
                       help='Number of users (default: 10000)')
    parser.add_argument('--num-content', type=int, default=50000,
                       help='Number of content items (default: 50000)')
    parser.add_argument('--events-per-day', type=int, default=1000000,
                       help='Base events per day (default: 1000000)')
    parser.add_argument('--format', choices=['json', 'parquet', 'csv'], default='json',
                       help='Output format (default: json)')
    parser.add_argument('--partition-by', choices=['date', 'month', 'year'], default='date',
                       help='Partitioning strategy (default: date)')
    parser.add_argument('--no-compress', action='store_true',
                       help='Disable compression')
    parser.add_argument('--workers', type=int, default=mp.cpu_count(),
                       help='Number of parallel workers')
    parser.add_argument('--no-seasonal', action='store_true',
                       help='Disable seasonal patterns')
    parser.add_argument('--no-growth', action='store_true',
                       help='Disable growth simulation')
    parser.add_argument('--seed', type=int, default=42,
                       help='Random seed (default: 42)')
    
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Create configuration
    config = BatchConfig(
        start_date=start_date,
        end_date=end_date,
        num_users=args.num_users,
        num_content=args.num_content,
        base_events_per_day=args.events_per_day,
        output_directory=args.output_dir,
        file_format=args.format,
        partition_by=args.partition_by,
        compress=not args.no_compress,
        parallel_workers=args.workers,
        seasonal_patterns=not args.no_seasonal,
        growth_simulation=not args.no_growth
    )
    
    # Generate data
    generator = BatchDataGenerator(config, seed=args.seed)
    
    start_time = datetime.now()
    logger.info("Starting batch data generation...")
    
    output_files = generator.generate_parallel()
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"Generated {len(output_files)} files in {duration}")
    
    # Generate and save summary statistics
    stats = generator.generate_summary_stats(output_files)
    stats_file = generator.save_summary_stats(stats)
    
    logger.info("Batch data generation completed!")
    logger.info(f"Total events: {stats['total_events']:,}")
    logger.info(f"Unique users: {stats['unique_users']:,}")
    logger.info(f"Unique content: {stats['unique_content']:,}")
    logger.info(f"Average events per day: {stats['daily_stats']['mean_events']:,.0f}")
    logger.info(f"Summary saved to: {stats_file}")


if __name__ == "__main__":
    main()
