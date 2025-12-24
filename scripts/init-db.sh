#!/bin/bash
set -e


psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
    CREATE TABLE IF NOT EXISTS events (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        event_type VARCHAR(50) NOT NULL,
        user_id UUID NOT NULL,
        session_id UUID NOT NULL,
        product_id UUID,
        data JSONB NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        processed_at TIMESTAMP WITH TIME ZONE
    );

    CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id);
    CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
    CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_events_processed_at ON events(processed_at) WHERE processed_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_events_data_gin ON events USING GIN(data);

    CREATE TABLE IF NOT EXISTS analytics_summary (
        id SERIAL PRIMARY KEY,
        date DATE NOT NULL,
        hour INTEGER NOT NULL CHECK (hour >= 0 AND hour <= 23),
        event_type VARCHAR(50) NOT NULL,
        total_events BIGINT DEFAULT 0,
        unique_users BIGINT DEFAULT 0,
        metadata JSONB,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(date, hour, event_type)
    );

    CREATE INDEX IF NOT EXISTS idx_analytics_date_hour ON analytics_summary(date, hour);
    CREATE INDEX IF NOT EXISTS idx_analytics_event_type ON analytics_summary(event_type);
    CREATE TABLE IF NOT EXISTS processed_offsets (
        topic VARCHAR(255) NOT NULL,
        partition INTEGER NOT NULL,
        "offset" BIGINT NOT NULL,
        consumer_group VARCHAR(255) NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (topic, partition, consumer_group)
    );

    CREATE USER readonly WITH PASSWORD 'readonly_password';
    GRANT CONNECT ON DATABASE analytics TO readonly;
    GRANT USAGE ON SCHEMA public TO readonly;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly;
EOSQL