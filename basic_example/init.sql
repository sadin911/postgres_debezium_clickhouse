DROP VIEW IF EXISTS analytics.products_cdc_mv;
DROP TABLE IF EXISTS analytics.products_cdc;
DROP TABLE IF EXISTS analytics.products_kafka_raw;
DROP TABLE IF EXISTS analytics.products_raw_data;
DROP TABLE IF EXISTS analytics.products_final;

-- ตรวจสอบให้แน่ใจว่า database 'analytics' มีอยู่ หรือสร้างขึ้นใหม่หากยังไม่มี
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics; -- <--- เพิ่มบรรทัดนี้

CREATE TABLE analytics.products_kafka_raw -- <--- ระบุ analytics.
(
    `raw_value` String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'poc.public.products',
    kafka_group_name = 'clickhouse_consumer_group_raw',
    kafka_format = 'JSONAsString',
    kafka_skip_broken_messages = 1;

CREATE TABLE analytics.products_raw_data -- <--- ระบุ analytics.
(
    `key` String,
    `value` String
)
ENGINE = MergeTree()
ORDER BY `key`;

CREATE MATERIALIZED VIEW analytics.products_raw_mv TO analytics.products_raw_data AS -- <--- ระบุ analytics.
SELECT
    _key AS key,
    raw_value AS value
FROM analytics.products_kafka_raw; -- <--- ระบุ analytics.


CREATE TABLE analytics.products_final -- <--- ระบุ analytics.
(
    `id` String,
    `name` String,
    `description` String,
    `price` String,
    `created_at` String,
    `updated_at` String,
    `op` String,
    `ts_ms` Int64
)
ENGINE = ReplacingMergeTree(ts_ms)
ORDER BY id;

CREATE MATERIALIZED VIEW analytics.products_final_mv TO analytics.products_final AS -- <--- ระบุ analytics.
SELECT
    JSONExtractString(value, 'payload', 'after', 'id') AS id,
    JSONExtractString(value, 'payload', 'after', 'name') AS name,
    JSONExtractString(value, 'payload', 'after', 'description') AS description,
    JSONExtractString(value, 'payload', 'after', 'price') AS price,
    JSONExtractString(value, 'payload', 'after', 'created_at') AS created_at,
    JSONExtractString(value, 'payload', 'after', 'updated_at') AS updated_at,
    JSONExtractString(value, 'payload', 'op') AS op,
    JSONExtractInt(value, 'payload', 'source', 'ts_ms') AS ts_ms
FROM analytics.products_raw_data -- <--- ระบุ analytics.
WHERE JSONHas(value, 'payload', 'after');