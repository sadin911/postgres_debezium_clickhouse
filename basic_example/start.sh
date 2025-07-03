#!/bin/bash

echo "Starting Docker Compose services..."

# Start all services in detached mode
docker-compose down 
docker volume prune
#docker volume rm basic_example_clickhouse_data
docker-compose up --build -d
if [ $? -eq 0 ]; then
    echo "All Docker Compose services started successfully!"
else
    echo "ERROR: Failed to start one or more Docker Compose services. Please check the logs above for errors."
    exit 1
fi

echo "Waiting for PostgreSQL to be ready..."
# Wait for PostgreSQL to be ready
# Use a timeout for pg_isready to prevent infinite loop in case of unrecoverable issues
TIMEOUT_SECONDS=60
START_TIME=$(date +%s)
until docker exec postgresql pg_isready -U user -d sourcedb > /dev/null 2>&1; do
  if (( $(date +%s) - START_TIME > TIMEOUT_SECONDS )); then
    echo "ERROR: PostgreSQL did not become ready within $TIMEOUT_SECONDS seconds. Exiting."
    exit 1
  fi
  echo "PostgreSQL is unavailable - sleeping (checked for $(($(date +%s) - START_TIME))s / ${TIMEOUT_SECONDS}s)"
  sleep 2
done
echo "PostgreSQL is ready!"

echo "Creating POC table in PostgreSQL and inserting initial data..."
# Execute SQL commands to create a table in the sourcedb
# Note: Escaped $$ as \$ for shell interpretation
docker exec postgresql psql -U user -d sourcedb -c "
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        price NUMERIC(10, 2),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    -- Insert some initial data
    INSERT INTO products (name, description, price) VALUES
    ('Laptop', 'High-performance laptop for work and gaming', 1200.00),
    ('Mouse', 'Ergonomic wireless mouse', 25.00)
    ON CONFLICT (id) DO NOTHING;
    
    -- Add a trigger to update 'updated_at' column automatically
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS \$\$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    \$\$ LANGUAGE plpgsql;

    DO \$\$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'set_updated_at_products') THEN
            CREATE TRIGGER set_updated_at_products
            BEFORE UPDATE ON products
            FOR EACH ROW
            EXECUTE FUNCTION update_updated_at_column();
        END IF;
    END \$\$;

    ALTER TABLE products REPLICA IDENTITY FULL; -- Essential for Debezium CDC
"
if [ $? -eq 0 ]; then
    echo "POC table 'products' created and initial data inserted successfully in PostgreSQL."
else
    echo "ERROR: Failed to create POC table in PostgreSQL or insert initial data. Please check the PostgreSQL logs."
    exit 1
fi

echo "Waiting for Kafka Connect (Debezium) to be ready..."
# Wait for Kafka Connect to be ready (assuming it's on localhost:8083)
# Check /connectors endpoint for HTTP 200 response
CONNECT_TIMEOUT_SECONDS=120
CONNECT_START_TIME=$(date +%s)
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors | grep -q "200"; do
  if (( $(date +%s) - CONNECT_START_TIME > CONNECT_TIMEOUT_SECONDS )); then
    echo "ERROR: Kafka Connect did not become ready within $CONNECT_TIMEOUT_SECONDS seconds. Exiting."
    exit 1
  fi
  echo "Kafka Connect is unavailable - sleeping (checked for $(($(date +%s) - CONNECT_START_TIME))s / ${CONNECT_TIMEOUT_SECONDS}s)"
  sleep 5 # Wait a bit longer for Kafka Connect to fully initialize
done
echo "Kafka Connect is ready!"

# echo "Configuring Debezium PostgreSQL connector (products-connector)..."
# curl -X POST -H "Content-Type: application/json" --data @debezium-postgres-connector.json http://localhost:8083/connectors
# if [ $? -eq 0 ]; then
#     echo "Debezium 'products-connector' configured successfully."
# else
#     echo "ERROR: Failed to configure Debezium 'products-connector'. Please check Debezium Connect logs."
#     exit 1
# fi

echo "Configuring Debezium PostgreSQL connector (abcsvb-connector)..."
curl -X POST -H "Content-Type: application/json" --data @abcsvb-connector.json http://localhost:8083/connectors
if [ $? -eq 0 ]; then
    echo "Debezium 'abcsvb-connector' configured successfully."
else
    echo "ERROR: Failed to configure Debezium 'abcsvb-connector'. Please check Debezium Connect logs."
    exit 1
fi

echo "Executing ClickHouse initialization script..."
# Execute the ClickHouse SQL script
# We use docker exec with clickhouse client and pipe the SQL file into it
# docker exec -i clickhouse clickhouse client -u default --password password --query_id "init_script_$(date +%s)" < init.sql
# docker exec -i clickhouse clickhouse client -u default --password password --query_id "init_script_$(date +%s)" < init_transpassport.sql
# docker exec -i clickhouse clickhouse client -u default --password password --query_id "init_script_$(date +%s)" < init_logtrans.sql

if [ $? -eq 0 ]; then # <--- เพิ่มการตรวจสอบนี้แล้ว
    echo "ClickHouse initialization script executed successfully!"
else
    echo "ERROR: Failed to execute ClickHouse initialization script. Please check ClickHouse logs."
    exit 1
fi

echo "All services and initial setup complete!"
echo "Access points:"
echo "  Kafka UI: http://localhost:8090"
echo "  pgAdmin:  http://localhost:8091 (Email: admin@example.com, Password: password)"
echo "  Debezium Connect API: http://localhost:8083"
echo "  ClickHouse HTTP: http://localhost:8123"
echo "  ClickHouse Native: localhost:9000"
echo "  PostgreSQL: localhost:5432 (DB: sourcedb, User: user, Password: password)"

# Optional: Tail logs of all services
# docker-compose logs -f