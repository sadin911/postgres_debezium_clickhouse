#!/bin/bash

echo "Starting Docker Compose services..."

# Start all services in detached mode
docker-compose up --build -d

if [ $? -eq 0 ]; then
    echo "All core services started successfully!"
else
    echo "Failed to start one or more core services. Please check the logs above for errors."
    exit 1
fi

echo "Waiting for PostgreSQL to be ready..."
# Wait for PostgreSQL to be ready
until docker exec postgresql pg_isready -U user -d sourcedb; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 2
done
echo "PostgreSQL is ready!"

echo "Creating POC table in PostgreSQL..."
# Execute SQL commands to create a table in the sourcedb
# Note: Escaped $$ as \$ for shell interpretation
docker exec -it postgresql psql -U user -d sourcedb -c "
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
    echo "Failed to create POC table in PostgreSQL. Please check the logs."
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