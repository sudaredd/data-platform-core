-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS test_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE test_keyspace;

-- Create UDT
CREATE TYPE IF NOT EXISTS numeric_data_point (
    value decimal,
    report_time timestamp
);

-- Create Table
CREATE TABLE IF NOT EXISTS DailyNumeric (
    tenant_id text,
    instrument_id text,
    period_year int,           -- Bucket column
    period_date date,
    field_id text,
    data frozen<numeric_data_point>,  -- UDT column
    PRIMARY KEY ((tenant_id, instrument_id, period_year), period_date, field_id)
);
