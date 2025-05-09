CREATE KEYSPACE marketing WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE marketing.user_engagement_metrics (
    user_id UUID PRIMARY KEY,
    login_count INT
);

CREATE TABLE marketing.active_user_metrics (
    preferences_timezone TEXT PRIMARY KEY,
    active_users INT
);

CREATE TABLE marketing.conversion_metrics (
    id UUID PRIMARY KEY, -- arbitrary UUID, since it's a single row
    completed BIGINT,
    initiated BIGINT,
    conversion_rate DECIMAL
);