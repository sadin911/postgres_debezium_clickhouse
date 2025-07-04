-- init_postgresql_data_scenario2.sql
-- Unified SQL file to create tables and insert initial data into PostgreSQL for POC Scenario 2 (High-Volume Transaction Details)

-- Drop tables if they exist to ensure a clean start (for development/testing)
-- Order matters due to foreign key constraints
DROP TABLE IF EXISTS transaction_details;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

-- Table for product master data
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    description TEXT,
    price NUMERIC(10, 2),
    stock_quantity INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table for customer master data
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone_number VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    registered_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Table for transactions (summary/header level)
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_uuid UUID DEFAULT gen_random_uuid(),
    customer_id INT NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL, -- This will be summed from details
    transaction_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'completed',
    payment_method VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- NEW TABLE: transaction_details (large volume)
CREATE TABLE transaction_details (
    id BIGSERIAL PRIMARY KEY, -- Use BIGSERIAL for potentially very large IDs
    transaction_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    line_total NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    detail_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, -- Timestamp for this specific detail
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Trigger to update 'updated_at' column automatically for products table
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'set_updated_at_products') THEN
        CREATE TRIGGER set_updated_at_products
        BEFORE UPDATE ON products
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    END IF;
END $$;

-- Essential for Debezium CDC
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE transaction_details REPLICA IDENTITY FULL; -- NEW: Essential for Debezium CDC for this table

-- Insert 10 products
INSERT INTO products (name, category, description, price, stock_quantity) VALUES
('Laptop Pro X', 'Electronics', 'Powerful laptop for professionals', 1500.00, 50),
('Gaming PC Ultra', 'Electronics', 'High-end gaming desktop', 2500.00, 20),
('Wireless Mouse', 'Accessories', 'Ergonomic mouse with long battery life', 35.00, 200),
('Mechanical Keyboard RGB', 'Accessories', 'Durable keyboard with customizable RGB lighting', 120.00, 100),
('4K Monitor 27"', 'Electronics', 'Vibrant display for work and entertainment', 400.00, 75),
('USB-C Hub 7-in-1', 'Accessories', 'Expand your device connectivity', 50.00, 150),
('External SSD 1TB', 'Storage', 'Fast and portable storage solution', 90.00, 80),
('Noise-Cancelling Headphones', 'Audio', 'Immersive sound experience', 200.00, 60),
('Webcam Full HD', 'Peripherals', 'Clear video calls and streaming', 60.00, 120),
('Smartwatch Fitness', 'Wearables', 'Track your health and fitness', 180.00, 90)
ON CONFLICT (id) DO NOTHING;

-- Insert 10 customers
INSERT INTO customers (first_name, last_name, email, phone_number, address, city, state, zip_code, country) VALUES
('Alice', 'Smith', 'alice.smith@example.com', '123-456-7890', '101 Pine St', 'New York', 'NY', '10001', 'USA'),
('Bob', 'Johnson', 'bob.j@example.com', '098-765-4321', '202 Oak Ave', 'Los Angeles', 'CA', '90001', 'USA'),
('Charlie', 'Brown', 'charlie.b@example.com', '555-111-2222', '303 Maple Rd', 'Toronto', 'ON', 'M5V 2H1', 'Canada'),
('Diana', 'Prince', 'diana.p@example.com', '444-333-2222', '404 Elm Blvd', 'London', 'England', 'SW1A 0AA', 'UK'),
('Eve', 'Davis', 'eve.d@example.com', '777-888-9999', '505 Birch Ln', 'Sydney', 'NSW', '2000', 'Australia'),
('Frank', 'Miller', 'frank.m@example.com', '111-222-3333', '606 Cedar Cir', 'Berlin', 'Berlin', '10115', 'Germany'),
('Grace', 'Wilson', 'grace.w@example.com', '999-000-1111', '707 Poplar Way', 'Paris', 'Ile-de-France', '75001', 'France'),
('Henry', 'Moore', 'henry.m@example.com', '222-333-4444', '808 Spruce Dr', 'Tokyo', 'Tokyo', '100-0005', 'Japan'),
('Ivy', 'Taylor', 'ivy.t@example.com', '333-444-5555', '909 Willow Ct', 'Singapore', '', '012345', 'Singapore'),
('Jack', 'Anderson', 'jack.a@example.com', '666-777-8888', '1010 Redwood Ave', 'Bangkok', '', '10120', 'Thailand')
ON CONFLICT (id) DO NOTHING;

-- Insert some initial transaction headers (fewer than details)
DO $$
DECLARE
    i INT := 1;
    num_headers INT := 1000; -- Fewer transaction headers, each can have multiple details
    customer_id_val INT;
    transaction_date_val TIMESTAMP WITH TIME ZONE;
    status_val VARCHAR(50);
    payment_method_val VARCHAR(50);
BEGIN
    FOR i IN 1..num_headers LOOP
        customer_id_val := floor(random() * 10) + 1;
        transaction_date_val := NOW() - (random() * (INTERVAL '730 days')); -- Up to 2 years ago
        status_val := CASE floor(random() * 10) WHEN 0 THEN 'pending' WHEN 1 THEN 'cancelled' WHEN 2 THEN 'returned' ELSE 'completed' END;
        payment_method_val := CASE floor(random() * 3) WHEN 0 THEN 'credit_card' WHEN 1 THEN 'cash' ELSE 'transfer' END;
        
        -- Insert transaction header with dummy total_amount first, will update later
        INSERT INTO transactions (customer_id, total_amount, transaction_date, status, payment_method)
        VALUES (customer_id_val, 0.00, transaction_date_val, status_val, payment_method_val);
    END LOOP;
END $$;


-- Insert a large volume of transaction_details data (1 million rows)
-- This will take some time, especially with Docker and random data generation
DO $$
DECLARE
    i INT := 1;
    num_detail_rows INT := 1000000; -- Set to 1 million rows
    transaction_id_val INT;
    product_id_val INT;
    quantity_val INT;
    unit_price_val NUMERIC(10, 2);
    detail_date_val TIMESTAMP WITH TIME ZONE;
    last_transaction_id INT;
BEGIN
    -- Get the max transaction_id to distribute details across existing transactions
    SELECT MAX(id) INTO last_transaction_id FROM transactions;

    FOR i IN 1..num_detail_rows LOOP
        -- Random transaction_id from existing headers
        transaction_id_val := floor(random() * last_transaction_id) + 1;
        -- Random product_id (1 to 10)
        product_id_val := floor(random() * 10) + 1;
        -- Random quantity (1 to 5)
        quantity_val := floor(random() * 5) + 1;
        
        -- Get a realistic price from products table
        SELECT price INTO unit_price_val FROM products WHERE id = product_id_val;
        
        -- Detail date around the transaction date, or just random in the last 2 years
        detail_date_val := NOW() - (random() * (INTERVAL '730 days')); 

        INSERT INTO transaction_details (transaction_id, product_id, quantity, unit_price, detail_date)
        VALUES (transaction_id_val, product_id_val, quantity_val, unit_price_val, detail_date_val);
    END LOOP;

    -- Update total_amount in transactions table based on aggregated transaction_details
    -- This update will also be captured by Debezium
    UPDATE transactions AS t
    SET total_amount = (
        SELECT SUM(td.line_total)
        FROM transaction_details AS td
        WHERE td.transaction_id = t.id
    );
END $$;