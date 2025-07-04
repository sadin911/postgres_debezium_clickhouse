-- init_clickhouse_layered_details.sql
-- SQL script to create a layered data architecture in ClickHouse for CDC from Debezium/Kafka.
-- This focuses on the 'transaction_details' table, following the pattern:
-- Kafka -> raw_mv -> raw_data -> final_mv -> final (future step)

-- Create a database for analytics if it doesn't exist
CREATE DATABASE IF NOT EXISTS analytics;

-- --- Layer 1: Kafka Engine Table (Source) ---
-- This table directly reads the raw JSON payload from the specified Kafka topic.
-- We read only the '_value' column as the Debezium payload is typically in the value part.
-- Topic: high_volume_poc.public.transaction_details (as confirmed by user)
CREATE TABLE IF NOT EXISTS analytics.kafka_transaction_details_source
(
    `_value` String,       -- Debezium's record value (full JSON string)
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'high_volume_poc.public.transaction_details',
    kafka_group_name = 'clickhouse_consumer_group_trans_details_layered', -- Unique consumer group for this flow
    kafka_format = 'JSONAsString', -- Treats the entire message value as a single JSON string
    kafka_skip_broken_messages = 1;

-- --- Layer 2: Raw Data Storage Table ---
-- This table stores the raw JSON message, along with Kafka metadata like partition and offset.
-- It serves as a durable, queryable historical record of all CDC events.
CREATE TABLE IF NOT EXISTS analytics.transaction_details_raw_data
(
    `key` String,
    `value_json` String,       -- The raw JSON payload from Debezium
    `partition_num` Int32,     -- Kafka partition number
    `offset_num` UInt64,       -- Kafka offset number
    `ingestion_time` DateTime DEFAULT now() -- Timestamp when the record was inserted into ClickHouse
)
ENGINE = MergeTree()
ORDER BY (partition_num, offset_num); -- Order by partition and offset for logical ordering and efficient merges

-- --- Layer 3: Raw Materialized View ---
-- This MV pulls messages from the Kafka Engine table and inserts them into the raw data storage table.
-- It will automatically process new messages as they arrive in the Kafka topic.
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.transaction_details_raw_mv TO analytics.transaction_details_raw_data AS
SELECT
    _value AS value_json,
    _partition AS partition_num,
    _offset AS offset_num
FROM analytics.kafka_transaction_details_source
WHERE _value IS NOT NULL AND _value != ''; -- Ensure we only process non-empty messages