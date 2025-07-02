DROP VIEW IF EXISTS products_cdc_mv;
DROP TABLE IF EXISTS products_cdc;
DROP TABLE IF EXISTS products_kafka_raw;

CREATE TABLE products_kafka_raw
(
    `raw_value` String
)
ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'poc.public.products',
    kafka_group_name = 'clickhouse_consumer_group_raw', -- แนะนำให้ใช้ชื่อ group เดิมเพื่อให้เริ่มอ่านต่อจากของเก่า หรือเปลี่ยนชื่อเพื่อเริ่มอ่านใหม่ทั้งหมด
    kafka_format = 'JSONAsString', -- *** เปลี่ยนจาก JSONEachRow เป็น JSONAsString ***
    kafka_skip_broken_messages = 1;

CREATE TABLE products_raw_data
(
    `key` String,
    `value` String
)
ENGINE = MergeTree()
ORDER BY `key`;

CREATE MATERIALIZED VIEW products_raw_mv TO products_raw_data AS
SELECT
    _key AS key,    -- `_key` คือ virtual column ที่เก็บ Key ของ Kafka Message
    raw_value AS value -- `raw_value` คือ JSON ทั้งก้อนที่อ่านมา
FROM products_kafka_raw;


CREATE TABLE products_final
(
    `id` String,
    `name` String,
    `description` String,
    `price` String,
    `created_at` String,
    `updated_at` String,
    `op` String,
    `ts_ms` Int64  -- <--- แก้ไขเป็น Int64 สำหรับ ReplacingMergeTree
)
ENGINE = ReplacingMergeTree(ts_ms) -- <--- ตอนนี้ถูกต้องและสมบูรณ์แล้ว
ORDER BY id;

-- ขั้นตอนที่ 2: สร้าง View ใหม่แบบที่เรียบง่ายที่สุดเพื่อทดสอบ
CREATE MATERIALIZED VIEW products_final_mv TO products_final AS
SELECT
    JSONExtractString(value, 'payload', 'after', 'id') AS id,
    JSONExtractString(value, 'payload', 'after', 'name') AS name,
    JSONExtractString(value, 'payload', 'after', 'description') AS description,
    JSONExtractString(value, 'payload', 'after', 'price') AS price,
    JSONExtractString(value, 'payload', 'after', 'created_at') AS created_at,
    JSONExtractString(value, 'payload', 'after', 'updated_at') AS updated_at,
    JSONExtractString(value, 'payload', 'op') AS op,
    JSONExtractInt(value, 'payload', 'source', 'ts_ms') AS ts_ms -- <--- แก้ไขให้ดึงค่าเป็นตัวเลข
FROM products_raw_data
WHERE JSONHas(value, 'payload', 'after');