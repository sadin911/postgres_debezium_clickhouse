#!/bin/bash

echo "Starting Docker Compose services for High-Volume CDC Scenario..."

# Clean up previous runs, especially volumes for fresh data
docker-compose down --volumes
docker volume prune -f 

# Start all services in detached mode
docker-compose up --build -d
if [ $? -eq 0 ]; then
    echo "All Docker Compose services started successfully!"
else
    echo "ERROR: Failed to start one or more Docker Compose services. Please check the logs above for errors."
    exit 1
fi

echo "Waiting for PostgreSQL to be ready..."
# Wait for PostgreSQL to be ready
TIMEOUT_SECONDS=120 # Increased timeout for potential large data insertion and DB readiness
START_TIME=$(date +%s)
until docker exec postgresql pg_isready -U user -d sourcedb > /dev/null 2>&1; do
  if (( $(date +%s) - START_TIME > TIMEOUT_SECONDS )); then
    echo "ERROR: PostgreSQL did not become ready within $TIMEOUT_SECONDS seconds. Exiting."
    docker-compose logs postgresql 
    exit 1
  fi
  echo "PostgreSQL is unavailable - sleeping (checked for $(($(date +%s) - START_TIME))s / ${TIMEOUT_SECONDS}s)"
  sleep 2
done
echo "PostgreSQL is ready!"

echo "Executing PostgreSQL initialization script (create tables and insert high-volume data)..."
# Execute the combined SQL script for PostgreSQL, including transaction_details
docker exec -i postgresql psql -U user -d sourcedb < init_postgresql_data_scenario2.sql
if [ $? -eq 0 ]; then
    echo "PostgreSQL tables (including transaction_details) created and initial data inserted successfully."
else
    echo "ERROR: Failed to initialize PostgreSQL data. Please check the PostgreSQL logs."
    docker-compose logs postgresql 
    exit 1
fi

echo "Waiting for Kafka Connect (Debezium) to be ready..."
# Wait for Kafka Connect to be ready
CONNECT_TIMEOUT_SECONDS=120
CONNECT_START_TIME=$(date +%s)
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors | grep -q "200"; do
  if (( $(date +%s) - CONNECT_START_TIME > CONNECT_TIMEOUT_SECONDS )); then
    echo "ERROR: Kafka Connect did not become ready within $CONNECT_TIMEOUT_SECONDS seconds. Exiting."
    docker-compose logs debezium-connect 
    exit 1
  fi
  echo "Kafka Connect is unavailable - sleeping (checked for $(($(date +%s) - CONNECT_START_TIME))s / ${CONNECT_TIMEOUT_SECONDS}s)"
  sleep 5 
done
echo "Kafka Connect is ready!"

# --- Configuration for high-volume-details-connector ---
echo "Configuring Debezium PostgreSQL connector (high-volume-details-connector)..."
CONNECTOR_NAME="high-volume-details-connector"
CONNECTOR_CONFIG_FILE="debezium-high-volume-details-connector.json"

# Check if the connector already exists, then update or create
CONNECTOR_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8083/connectors/$CONNECTOR_NAME/status)

if [ "$CONNECTOR_STATUS" -eq 200 ]; then
    echo "Debezium connector '$CONNECTOR_NAME' already exists. Updating its configuration..."
    curl -X PUT -H "Content-Type: application/json" --data @$CONNECTOR_CONFIG_FILE http://localhost:8083/connectors/$CONNECTOR_NAME/config
    if [ $? -eq 0 ]; then echo "Debezium '$CONNECTOR_NAME' updated successfully."; else echo "ERROR: Failed to update Debezium '$CONNECTOR_NAME'. Please check Debezium Connect logs."; docker-compose logs debezium-connect; exit 1; fi
else
    echo "Debezium connector '$CONNECTOR_NAME' does not exist. Creating new connector..."
    curl -X POST -H "Content-Type: application/json" --data @$CONNECTOR_CONFIG_FILE http://localhost:8083/connectors
    if [ $? -eq 0 ]; then echo "Debezium '$CONNECTOR_NAME' created successfully."; else echo "ERROR: Failed to create Debezium '$CONNECTOR_NAME'. Please check Debezium Connect logs."; docker-compose logs debezium-connect; exit 1; fi
fi

echo "ClickHouse initialization script (skipped for this scenario as requested - focus on CDC data flow)..."
# In this scenario, we skip ClickHouse initialization as requested.
# You would add your ClickHouse table creation and Materialized View setup here if needed later.
# docker exec -i clickhouse clickhouse client -u default --password password < init_clickhouse_tables_scenario2.sql
# docker exec -i clickhouse clickhouse client -u default --password password --query_id "init_script_$(date +%s)" < init_clickhouse_raw_tables.sql

echo "All services and initial setup complete for High-Volume CDC Scenario!"
echo "Access points:"
echo "  Kafka UI: http://localhost:8090"
echo "  pgAdmin:  http://localhost:8091 (Email: admin@example.com, Password: password)"
echo "  Debezium Connect API: http://localhost:8083"
echo "  ClickHouse HTTP: http://localhost:8123"
echo "  ClickHouse Native: localhost:9000"
echo "  PostgreSQL: localhost:5432 (DB: sourcedb, User: user, Password: password)"

# Optional: Tail logs of all services
# docker-compose logs -f