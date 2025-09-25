-- Analytics Views for Video Streaming Lakehouse
-- These views provide pre-computed analytics for dashboards and reporting

-- ==============================================================================
-- USER ANALYTICS VIEWS
-- ==============================================================================

-- Daily Active Users (DAU) with engagement metrics
CREATE OR REPLACE VIEW V_DAILY_ACTIVE_USERS AS
SELECT 
    d.date_value,
    COUNT(DISTINCT f.user_key) as daily_active_users,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'premium' THEN f.user_key END) as premium_users,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'free' THEN f.user_key END) as free_users,
    AVG(m.total_watch_time_minutes) as avg_watch_time_minutes,
    AVG(m.sessions_count) as avg_sessions_per_user,
    AVG(m.completion_rate) as avg_completion_rate,
    SUM(m.estimated_revenue) as total_revenue
FROM FACT_DAILY_USER_METRICS m
JOIN DIM_TIME d ON m.time_key = d.time_key
JOIN DIM_USER u ON m.user_key = u.user_key
WHERE d.date_value >= CURRENT_DATE - 90  -- Last 90 days
GROUP BY d.date_value
ORDER BY d.date_value;

-- User Cohort Analysis
CREATE OR REPLACE VIEW V_USER_COHORTS AS
WITH user_signup_cohorts AS (
    SELECT 
        user_key,
        DATE_TRUNC('month', signup_date) as cohort_month,
        DATEDIFF('month', signup_date, CURRENT_DATE) as months_since_signup
    FROM DIM_USER
    WHERE signup_date IS NOT NULL
),
cohort_metrics AS (
    SELECT 
        c.cohort_month,
        c.months_since_signup,
        COUNT(DISTINCT c.user_key) as cohort_size,
        COUNT(DISTINCT m.user_key) as active_users,
        AVG(m.total_watch_time_minutes) as avg_watch_time,
        AVG(m.engagement_score) as avg_engagement
    FROM user_signup_cohorts c
    LEFT JOIN FACT_DAILY_USER_METRICS m ON c.user_key = m.user_key
    LEFT JOIN DIM_TIME d ON m.time_key = d.time_key
    WHERE d.date_value >= DATEADD('month', c.months_since_signup, c.cohort_month)
      AND d.date_value < DATEADD('month', c.months_since_signup + 1, c.cohort_month)
    GROUP BY c.cohort_month, c.months_since_signup
)
SELECT 
    cohort_month,
    months_since_signup,
    cohort_size,
    active_users,
    CASE WHEN cohort_size > 0 THEN active_users::FLOAT / cohort_size ELSE 0 END as retention_rate,
    avg_watch_time,
    avg_engagement
FROM cohort_metrics
ORDER BY cohort_month, months_since_signup;

-- User Segmentation based on engagement
CREATE OR REPLACE VIEW V_USER_SEGMENTS AS
WITH user_engagement AS (
    SELECT 
        u.user_key,
        u.user_id,
        u.subscription_tier,
        AVG(m.total_watch_time_minutes) as avg_daily_watch_time,
        AVG(m.sessions_count) as avg_daily_sessions,
        AVG(m.completion_rate) as avg_completion_rate,
        AVG(m.engagement_score) as avg_engagement_score,
        COUNT(DISTINCT m.time_key) as active_days_last_30
    FROM DIM_USER u
    LEFT JOIN FACT_DAILY_USER_METRICS m ON u.user_key = m.user_key
    LEFT JOIN DIM_TIME d ON m.time_key = d.time_key
    WHERE d.date_value >= CURRENT_DATE - 30
    GROUP BY u.user_key, u.user_id, u.subscription_tier
)
SELECT 
    user_key,
    user_id,
    subscription_tier,
    avg_daily_watch_time,
    avg_daily_sessions,
    avg_completion_rate,
    avg_engagement_score,
    active_days_last_30,
    CASE 
        WHEN avg_engagement_score >= 8 AND avg_daily_watch_time >= 60 THEN 'Power User'
        WHEN avg_engagement_score >= 6 AND avg_daily_watch_time >= 30 THEN 'Regular User'
        WHEN avg_engagement_score >= 3 AND avg_daily_watch_time >= 15 THEN 'Casual User'
        WHEN active_days_last_30 > 0 THEN 'Light User'
        ELSE 'Inactive User'
    END as user_segment,
    CASE 
        WHEN active_days_last_30 = 0 THEN 'At Risk'
        WHEN active_days_last_30 <= 3 THEN 'Low Activity'
        WHEN active_days_last_30 <= 10 THEN 'Medium Activity'
        ELSE 'High Activity'
    END as activity_level
FROM user_engagement;

-- ==============================================================================
-- CONTENT ANALYTICS VIEWS
-- ==============================================================================

-- Top Performing Content
CREATE OR REPLACE VIEW V_TOP_CONTENT AS
SELECT 
    c.content_id,
    c.content_title,
    c.content_type,
    c.content_genre,
    SUM(p.total_views) as total_views_30d,
    SUM(p.unique_viewers) as unique_viewers_30d,
    SUM(p.total_watch_time_minutes) as total_watch_time_30d,
    AVG(p.completion_rate) as avg_completion_rate,
    AVG(p.engagement_score) as avg_engagement_score,
    SUM(p.revenue) as total_revenue_30d,
    RANK() OVER (ORDER BY SUM(p.total_views) DESC) as view_rank,
    RANK() OVER (ORDER BY AVG(p.engagement_score) DESC) as engagement_rank
FROM FACT_CONTENT_PERFORMANCE p
JOIN DIM_CONTENT c ON p.content_key = c.content_key
JOIN DIM_TIME d ON p.time_key = d.time_key
WHERE d.date_value >= CURRENT_DATE - 30
GROUP BY c.content_key, c.content_id, c.content_title, c.content_type, c.content_genre
ORDER BY total_views_30d DESC;

-- Content Performance Trends
CREATE OR REPLACE VIEW V_CONTENT_TRENDS AS
SELECT 
    d.date_value,
    c.content_type,
    c.content_genre,
    COUNT(DISTINCT c.content_key) as unique_content_count,
    SUM(p.total_views) as total_views,
    SUM(p.unique_viewers) as unique_viewers,
    AVG(p.completion_rate) as avg_completion_rate,
    SUM(p.total_watch_time_minutes) as total_watch_time_minutes
FROM FACT_CONTENT_PERFORMANCE p
JOIN DIM_CONTENT c ON p.content_key = c.content_key
JOIN DIM_TIME d ON p.time_key = d.time_key
WHERE d.date_value >= CURRENT_DATE - 90
GROUP BY d.date_value, c.content_type, c.content_genre
ORDER BY d.date_value, c.content_type, c.content_genre;

-- Content Drop-off Analysis
CREATE OR REPLACE VIEW V_CONTENT_DROPOFF AS
WITH video_positions AS (
    SELECT 
        v.content_key,
        c.content_title,
        c.content_duration,
        v.playback_position,
        CASE 
            WHEN v.playback_position <= c.content_duration * 0.1 THEN '0-10%'
            WHEN v.playback_position <= c.content_duration * 0.25 THEN '10-25%'
            WHEN v.playback_position <= c.content_duration * 0.5 THEN '25-50%'
            WHEN v.playback_position <= c.content_duration * 0.75 THEN '50-75%'
            WHEN v.playback_position <= c.content_duration * 0.9 THEN '75-90%'
            ELSE '90-100%'
        END as position_bucket,
        COUNT(*) as event_count
    FROM FACT_VIDEO_EVENTS v
    JOIN DIM_CONTENT c ON v.content_key = c.content_key
    JOIN DIM_TIME d ON v.time_key = d.time_key
    WHERE d.date_value >= CURRENT_DATE - 7
      AND v.event_type = 'video_stop'
    GROUP BY v.content_key, c.content_title, c.content_duration, 
             v.playback_position, position_bucket
)
SELECT 
    content_key,
    content_title,
    position_bucket,
    SUM(event_count) as total_stops,
    SUM(event_count) * 100.0 / SUM(SUM(event_count)) OVER (PARTITION BY content_key) as dropoff_percentage
FROM video_positions
GROUP BY content_key, content_title, position_bucket
ORDER BY content_key, position_bucket;

-- ==============================================================================
-- DEVICE AND PLATFORM ANALYTICS
-- ==============================================================================

-- Device Usage Statistics
CREATE OR REPLACE VIEW V_DEVICE_ANALYTICS AS
SELECT 
    d.device_type,
    d.platform,
    d.device_category,
    COUNT(DISTINCT v.user_key) as unique_users_30d,
    COUNT(DISTINCT v.session_id) as unique_sessions_30d,
    SUM(v.watch_duration_minutes) as total_watch_time_30d,
    AVG(v.watch_duration_minutes) as avg_watch_duration,
    AVG(v.completion_rate) as avg_completion_rate,
    COUNT(CASE WHEN v.event_type = 'video_buffer' THEN 1 END) as buffer_events,
    COUNT(CASE WHEN v.event_type = 'video_error' THEN 1 END) as error_events
FROM FACT_VIDEO_EVENTS v
JOIN DIM_DEVICE d ON v.device_key = d.device_key
JOIN DIM_TIME dt ON v.time_key = dt.time_key
WHERE dt.date_value >= CURRENT_DATE - 30
GROUP BY d.device_type, d.platform, d.device_category
ORDER BY unique_users_30d DESC;

-- Platform Performance Quality Metrics
CREATE OR REPLACE VIEW V_PLATFORM_QUALITY AS
SELECT 
    d.platform,
    d.device_type,
    COUNT(*) as total_events,
    AVG(v.startup_time) as avg_startup_time,
    AVG(v.buffer_duration) as avg_buffer_duration,
    COUNT(CASE WHEN v.event_type = 'video_error' THEN 1 END) * 100.0 / COUNT(*) as error_rate_percent,
    COUNT(CASE WHEN v.event_type = 'video_buffer' THEN 1 END) * 100.0 / COUNT(*) as buffer_rate_percent,
    AVG(v.bitrate) as avg_bitrate
FROM FACT_VIDEO_EVENTS v
JOIN DIM_DEVICE d ON v.device_key = d.device_key
JOIN DIM_TIME dt ON v.time_key = dt.time_key
WHERE dt.date_value >= CURRENT_DATE - 7
  AND v.startup_time IS NOT NULL
GROUP BY d.platform, d.device_type
ORDER BY error_rate_percent ASC, avg_startup_time ASC;

-- ==============================================================================
-- GEOGRAPHIC ANALYTICS
-- ==============================================================================

-- Geographic User Distribution
CREATE OR REPLACE VIEW V_GEOGRAPHIC_ANALYTICS AS
SELECT 
    g.country_code,
    g.country_name,
    g.continent,
    COUNT(DISTINCT m.user_key) as unique_users_30d,
    SUM(m.total_watch_time_minutes) as total_watch_time_30d,
    AVG(m.total_watch_time_minutes) as avg_watch_time_per_user,
    AVG(m.completion_rate) as avg_completion_rate,
    SUM(m.estimated_revenue) as total_revenue_30d,
    COUNT(DISTINCT CASE WHEN u.subscription_tier IN ('premium', 'enterprise') THEN m.user_key END) as premium_users
FROM FACT_DAILY_USER_METRICS m
JOIN DIM_USER u ON m.user_key = u.user_key
JOIN DIM_TIME d ON m.time_key = d.time_key
JOIN FACT_VIDEO_EVENTS v ON m.user_key = v.user_key AND m.time_key = v.time_key
JOIN DIM_GEOGRAPHY g ON v.geography_key = g.geography_key
WHERE d.date_value >= CURRENT_DATE - 30
GROUP BY g.country_code, g.country_name, g.continent
ORDER BY unique_users_30d DESC;

-- ==============================================================================
-- BUSINESS METRICS AND KPIs
-- ==============================================================================

-- Executive Dashboard KPIs
CREATE OR REPLACE VIEW V_EXECUTIVE_KPIS AS
WITH current_period AS (
    SELECT 
        COUNT(DISTINCT m.user_key) as current_dau,
        SUM(m.total_watch_time_minutes) as current_total_watch_time,
        AVG(m.total_watch_time_minutes) as current_avg_watch_time,
        SUM(m.estimated_revenue) as current_revenue,
        AVG(m.completion_rate) as current_completion_rate
    FROM FACT_DAILY_USER_METRICS m
    JOIN DIM_TIME d ON m.time_key = d.time_key
    WHERE d.date_value = CURRENT_DATE - 1
),
previous_period AS (
    SELECT 
        COUNT(DISTINCT m.user_key) as previous_dau,
        SUM(m.total_watch_time_minutes) as previous_total_watch_time,
        AVG(m.total_watch_time_minutes) as previous_avg_watch_time,
        SUM(m.estimated_revenue) as previous_revenue,
        AVG(m.completion_rate) as previous_completion_rate
    FROM FACT_DAILY_USER_METRICS m
    JOIN DIM_TIME d ON m.time_key = d.time_key
    WHERE d.date_value = CURRENT_DATE - 8  -- Week over week
)
SELECT 
    c.current_dau,
    c.current_total_watch_time,
    c.current_avg_watch_time,
    c.current_revenue,
    c.current_completion_rate,
    CASE WHEN p.previous_dau > 0 THEN 
        ((c.current_dau - p.previous_dau) * 100.0 / p.previous_dau) 
    ELSE 0 END as dau_growth_percent,
    CASE WHEN p.previous_revenue > 0 THEN 
        ((c.current_revenue - p.previous_revenue) * 100.0 / p.previous_revenue)
    ELSE 0 END as revenue_growth_percent,
    CASE WHEN p.previous_total_watch_time > 0 THEN 
        ((c.current_total_watch_time - p.previous_total_watch_time) * 100.0 / p.previous_total_watch_time)
    ELSE 0 END as watch_time_growth_percent
FROM current_period c
CROSS JOIN previous_period p;

-- Monthly Recurring Revenue (MRR) Analysis
CREATE OR REPLACE VIEW V_MRR_ANALYSIS AS
WITH monthly_revenue AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        u.subscription_tier,
        COUNT(DISTINCT m.user_key) as subscribers,
        SUM(m.estimated_revenue) as total_revenue,
        AVG(m.estimated_revenue) as arpu  -- Average Revenue Per User
    FROM FACT_DAILY_USER_METRICS m
    JOIN DIM_TIME d ON m.time_key = d.time_key
    JOIN DIM_USER u ON m.user_key = u.user_key
    WHERE u.subscription_tier IN ('basic', 'premium', 'enterprise')
      AND d.date_value >= CURRENT_DATE - 365
    GROUP BY d.year, d.month, d.month_name, u.subscription_tier
)
SELECT 
    year,
    month,
    month_name,
    subscription_tier,
    subscribers,
    total_revenue,
    arpu,
    LAG(total_revenue) OVER (PARTITION BY subscription_tier ORDER BY year, month) as prev_month_revenue,
    total_revenue - LAG(total_revenue) OVER (PARTITION BY subscription_tier ORDER BY year, month) as revenue_change,
    CASE WHEN LAG(total_revenue) OVER (PARTITION BY subscription_tier ORDER BY year, month) > 0 THEN
        ((total_revenue - LAG(total_revenue) OVER (PARTITION BY subscription_tier ORDER BY year, month)) * 100.0 / 
         LAG(total_revenue) OVER (PARTITION BY subscription_tier ORDER BY year, month))
    ELSE 0 END as revenue_growth_percent
FROM monthly_revenue
ORDER BY year, month, subscription_tier;

-- Churn Risk Analysis
CREATE OR REPLACE VIEW V_CHURN_RISK AS
WITH user_activity AS (
    SELECT 
        u.user_key,
        u.user_id,
        u.subscription_tier,
        COUNT(DISTINCT m.time_key) as active_days_last_30,
        AVG(m.total_watch_time_minutes) as avg_daily_watch_time,
        AVG(m.engagement_score) as avg_engagement_score,
        MAX(d.date_value) as last_active_date,
        DATEDIFF('day', MAX(d.date_value), CURRENT_DATE) as days_since_last_active
    FROM DIM_USER u
    LEFT JOIN FACT_DAILY_USER_METRICS m ON u.user_key = m.user_key
    LEFT JOIN DIM_TIME d ON m.time_key = d.time_key
    WHERE d.date_value >= CURRENT_DATE - 30
    GROUP BY u.user_key, u.user_id, u.subscription_tier
)
SELECT 
    user_key,
    user_id,
    subscription_tier,
    active_days_last_30,
    avg_daily_watch_time,
    avg_engagement_score,
    last_active_date,
    days_since_last_active,
    CASE 
        WHEN days_since_last_active >= 14 THEN 'High Risk'
        WHEN days_since_last_active >= 7 OR avg_engagement_score < 3 THEN 'Medium Risk'
        WHEN active_days_last_30 <= 5 OR avg_daily_watch_time < 10 THEN 'Low Risk'
        ELSE 'Healthy'
    END as churn_risk_category,
    CASE 
        WHEN days_since_last_active >= 14 THEN 0.8
        WHEN days_since_last_active >= 7 THEN 0.6
        WHEN avg_engagement_score < 2 THEN 0.7
        WHEN active_days_last_30 <= 3 THEN 0.5
        WHEN avg_daily_watch_time < 5 THEN 0.4
        ELSE 0.1
    END as churn_risk_score
FROM user_activity
WHERE subscription_tier IN ('basic', 'premium', 'enterprise')
ORDER BY churn_risk_score DESC;
