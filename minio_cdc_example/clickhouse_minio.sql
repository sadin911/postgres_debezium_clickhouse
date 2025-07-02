-- In clickhouse_minio.sql

-- 1. ตารางสำหรับเก็บข้อมูลจริง (สมมติว่าไฟล์เป็น JSON ที่มี id และ message)
CREATE TABLE IF NOT EXISTS default.minio_data
(
    `id` Int64,
    `message` String,
    `timestamp` DateTime
)
ENGINE = MergeTree()
ORDER BY id;

-- 2. Kafka Engine Table สำหรับเชื่อมกับ topic 'minio_events'
CREATE TABLE IF NOT EXISTS default.minio_kafka
(
    `id` Int64,
    `message` String,
    `timestamp` DateTime
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'minio_events',
    kafka_group_name = 'clickhouse_minio_consumer_group',
    kafka_format = 'JSONEachRow', -- รูปแบบข้อมูลใน Kafka
    kafka_skip_broken_messages = 1;

-- 3. Materialized View เพื่อย้ายข้อมูลจาก Kafka -> ตารางจริง
CREATE MATERIALIZED VIEW IF NOT EXISTS default.minio_consumer TO default.minio_data
AS SELECT * FROM default.minio_kafka;