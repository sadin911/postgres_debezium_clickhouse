-- ตรวจสอบให้แน่ใจว่า database 'analytics' มีอยู่ หรือสร้างขึ้นใหม่หากยังไม่มี
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics; -- กำหนด default database เป็น analytics

-- 1. ตาราง Kafka Engine: สำหรับรับข้อมูลดิบจาก Kafka Topic
-- เราจะอ่านเฉพาะ _value เพราะ _key ไม่มีข้อมูลหรือไม่ได้ใช้งาน
CREATE TABLE analytics.passport_kafka_raw
(
    `_value` String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'poc.public.trn_passport',
    kafka_group_name = 'clickhouse_consumer_group_passport_raw_no_key',
    kafka_format = 'JSONAsString',
    kafka_skip_broken_messages = 1;

-- 2. ตาราง Staging Data: สำหรับเก็บข้อมูลดิบจาก Kafka Engine table
-- Materialized View จะส่งข้อมูลจาก Kafka Engine table มายังตารางนี้
CREATE TABLE analytics.passport_raw_data
(
    `key` String,
    `value_json` String,
    `partition_str` String,
    `offset` Int32     -- Debezium's record value (JSON string)
)
ENGINE = MergeTree()
ORDER BY `offset`;

-- Materialized View เพื่อย้ายข้อมูลจาก Kafka Engine table ไปยัง Staging Data table
CREATE MATERIALIZED VIEW analytics.passport_raw_mv TO analytics.passport_raw_data AS
SELECT
    _value AS value_json,
    _partition AS partition_str,
    _offset AS offset
FROM analytics.passport_kafka_raw;


-- 3. ตาราง Final Data: สำหรับเก็บสถานะปัจจุบันของข้อมูล trn_passport
-- ตารางนี้จะใช้ ReplacingMergeTree เพื่อจัดการกับการอัปเดตและลบ
CREATE TABLE analytics.trn_passport
(
    trn_no Int32,
    trn_id String,
    nationality String,
    doctype String,
    passport_number String,
    gender String,
    birthday String,
    expire_date String,
    surname String,
    given_name String,
    gate_id String,
    site_id String,
    created_by String,
    created_datetime String,
    issuer String,
)
ENGINE = ReplacingMergeTree() -- ใช้ _event_time เป็นเวอร์ชัน
ORDER BY (trn_id);


-- 4. Materialized View สำหรับประมวลผลข้อมูล CDC จาก Raw Data ไปยัง Final Table
CREATE MATERIALIZED VIEW analytics.trn_passport_mv TO analytics.trn_passport AS
SELECT
    -- Extracting fields directly from 'payload.after' using path-based JSONExtract functions
    JSONExtractInt(value_json, 'payload', 'after', 'trn_no') AS trn_no,
    JSONExtractString(value_json, 'payload', 'after', 'trn_id') AS trn_id,
    JSONExtractString(value_json, 'payload', 'after', 'nationality') AS nationality,
    JSONExtractString(value_json, 'payload', 'after', 'doctype') AS doctype,
    JSONExtractString(value_json, 'payload', 'after', 'passport_number') AS passport_number,
    JSONExtractString(value_json, 'payload', 'after', 'gender') AS gender,
    JSONExtractString(value_json, 'payload', 'after', 'birthday') AS birthday,
    JSONExtractString(value_json, 'payload', 'after', 'expire_date') AS expire_date,
    JSONExtractString(value_json, 'payload', 'after', 'surname') AS surname,
    JSONExtractString(value_json, 'payload', 'after', 'given_name') AS given_name,
    JSONExtractString(value_json, 'payload', 'after', 'gate_id') AS gate_id,
    JSONExtractString(value_json, 'payload', 'after', 'site_id') AS site_id,
    JSONExtractString(value_json, 'payload', 'after', 'created_by') AS created_by,
    -- Assuming created_datetime can be extracted or derived as a DateTime for versioning
    -- If created_datetime is a string, you might need to convert it to a proper DateTime type
    -- for better versioning with ReplacingMergeTree
    JSONExtractString(value_json, 'payload', 'after', 'created_datetime') AS created_datetime,
    JSONExtractString(value_json, 'payload', 'after', 'issuer') AS issuer
    -- Add a version column for ReplacingMergeTree.
    -- This could be a timestamp of the event, or a specific version number from the CDC payload.
    -- For Debezium, 'ts_ms' from 'payload.source' is often used as a reliable timestamp for ordering.
    -- JSONExtractInt(value_json, 'payload', 'source', 'ts_ms') AS _version
FROM analytics.passport_raw_data
WHERE JSONHas(value_json, 'payload', 'after');