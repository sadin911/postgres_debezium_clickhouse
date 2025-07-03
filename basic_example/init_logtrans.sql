-- ตรวจสอบให้แน่ใจว่า database 'analytics' มีอยู่ หรือสร้างขึ้นใหม่หากยังไม่มี
CREATE DATABASE IF NOT EXISTS analytics;
USE analytics; -- กำหนด default database เป็น analytics

-- 1. ตาราง Kafka Engine: สำหรับรับข้อมูลดิบจาก Kafka Topic
-- เราจะอ่านเฉพาะ _value เพราะ _key ไม่มีข้อมูลหรือไม่ได้ใช้งาน
CREATE TABLE analytics.logtran_kafka_raw
(
    `_value` String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'poc.public.log_trans',
    kafka_group_name = 'clickhouse_consumer_group_logtran_raw_no_key',
    kafka_format = 'JSONAsString',
    kafka_skip_broken_messages = 1;

-- 2. ตาราง Staging Data: สำหรับเก็บข้อมูลดิบจาก Kafka Engine table
-- Materialized View จะส่งข้อมูลจาก Kafka Engine table มายังตารางนี้
CREATE TABLE analytics.logtran_raw_data
(
    `key` String,
    `value_json` String,
    `partition_str` String,
    `offset` Int32     -- Debezium's record value (JSON string)
)
ENGINE = MergeTree()
ORDER BY `offset`;

-- Materialized View เพื่อย้ายข้อมูลจาก Kafka Engine table ไปยัง Staging Data table
CREATE MATERIALIZED VIEW analytics.logtran_raw_mv TO analytics.logtran_raw_data AS
SELECT
    _value AS value_json,
    _partition AS partition_str,
    _offset AS offset
FROM analytics.logtran_kafka_raw;