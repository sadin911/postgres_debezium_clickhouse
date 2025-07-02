-- 1. ล้างของเก่าทิ้งก่อน (เหมือนเดิม)
DROP VIEW IF EXISTS products_raw_mv;
DROP TABLE IF EXISTS products_raw_data;
DROP TABLE IF EXISTS products_kafka_raw;
DROP VIEW IF EXISTS products_final_mv;
DROP TABLE IF EXISTS products_final;

-- 2. สร้างตารางสำหรับเชื่อมต่อ Kafka ใหม่ (มีการเปลี่ยนแปลง)
-- เราจะอ่าน Value ทั้งก้อนมาเป็น String เดียว
CREATE TABLE products_kafka_raw
(
    `value` String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'poc.public.products',
    kafka_group_name = 'clickhouse_consumer_group_raw', -- แนะนำให้ใช้ชื่อ group เดิมเพื่อให้เริ่มอ่านต่อจากของเก่า หรือเปลี่ยนชื่อเพื่อเริ่มอ่านใหม่ทั้งหมด
    kafka_format = 'JSONAsString', -- *** เปลี่ยนจาก JSONEachRow เป็น JSONAsString ***
    kafka_skip_broken_messages = 1;


-- 3. สร้างตารางสำหรับเก็บข้อมูลถาวร (เหมือนเดิม)
/* CREATE TABLE products_raw_data
(
    `key` String,
    `value` String
)
ENGINE = MergeTree()
ORDER BY `key`; */


-- 4. สร้าง Materialized View ใหม่ (มีการเปลี่ยนแปลง)
/* CREATE MATERIALIZED VIEW products_raw_mv TO products_raw_data AS
SELECT
    _key AS key,    -- `_key` คือ virtual column ที่เก็บ Key ของ Kafka Message
    value AS value -- `raw_value` คือ JSON ทั้งก้อนที่อ่านมา */
    
-- ขั้นตอนที่ 3: สร้างตารางปลายทางสำหรับ POC (เกือบทุกอย่างเป็น String)
CREATE TABLE products_final
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


-- ขั้นตอนที่ 4: สร้าง Materialized View ที่อ่านจากตารางที่ถูกต้อง
-- และใช้แค่ฟังก์ชันพื้นฐานที่ v23.8 รับได้
CREATE MATERIALIZED VIEW products_final_mv TO products_final AS
SELECT
    JSONExtractString(value, 'payload', 'after', 'id') AS id,
    JSONExtractString(value, 'payload', 'after', 'name') AS name,
    JSONExtractString(value, 'payload', 'after', 'description') AS description,
    JSONExtractString(value, 'payload', 'after', 'price') AS price,
    JSONExtractString(value, 'payload', 'after', 'created_at') AS created_at,
    JSONExtractString(value, 'payload', 'after', 'updated_at') AS updated_at,
    JSONExtractString(value, 'payload', 'op') AS op,
    JSONExtractInt(value, 'payload', 'source', 'ts_ms') AS ts_ms
FROM products_kafka_raw; -- อ่านจากตารางที่ถูกต้อง
FROM products_kafka_raw;

CREATE OR REPLACE VIEW products_usable_vw AS
SELECT
    toInt32(id) AS id,
    name,
    description,
    -- ตอนนี้ price เป็น String ที่อ่านได้แล้ว แปลงเป็น Float ได้ง่ายๆ
    toFloat64(price) AS price,
    toDateTime64(trimRight(created_at, 'Z'), 6, 'UTC') AS created_at,
    toDateTime64(trimRight(updated_at, 'Z'), 6, 'UTC') AS updated_at,
    op,
    ts_ms
FROM products_final
FINAL;