#!/usr/bin/env bash

set -e
set -o pipefail

echo "============================================================"
echo "🚀 Airbnb Pipeline Bootstrap Starting"
echo "============================================================"

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# ------------------------------------------------------------
# 1. Build Airflow images
# ------------------------------------------------------------
echo ""
echo "🔧 Building Airflow Docker images..."
echo "------------------------------------------------------------"

cd "$PROJECT_ROOT/airflow"
docker compose build
echo "✅ Airflow images built"

# ------------------------------------------------------------
# 2. Initialize Airflow (official way)
# ------------------------------------------------------------
echo ""
echo "🗄️  Initializing Airflow metadata database..."
echo "------------------------------------------------------------"

docker compose up airflow-init
echo "✅ Airflow initialized"

# ------------------------------------------------------------
# 3. Start Airflow services
# ------------------------------------------------------------
echo ""
echo "🚀 Starting Airflow services..."
echo "------------------------------------------------------------"

docker compose up -d
echo "✅ Airflow is running"

# ------------------------------------------------------------
# 4. Build Airbnb Dataflow (Beam) image
# ------------------------------------------------------------
echo ""
echo "🔧 Building Airbnb Dataflow image..."
echo "------------------------------------------------------------"

cd "$PROJECT_ROOT/airbnb_etl/dataflow"

docker build \
  -t airbnb-dataflow:latest \
  .

echo "✅ Airbnb Dataflow image built"

# ------------------------------------------------------------
# 5. Build Airbnb dbt image
# ------------------------------------------------------------
echo ""
echo "🔧 Building Airbnb dbt image..."
echo "------------------------------------------------------------"

cd "$PROJECT_ROOT/airbnb_dbt"

docker build \
  -t airbnb-dbt:latest \
  .

echo "✅ Airbnb dbt image built"

# ------------------------------------------------------------
# Done
# ------------------------------------------------------------
echo ""
echo "============================================================"
echo "🎉 Airbnb Pipeline is READY!"
echo "============================================================"

echo ""
echo "Access Airflow UI at: http://localhost:8080"
echo "Images available:"
echo "  - Airflow (via docker-compose)"
echo "  - airbnb-dataflow:latest"
echo "  - airbnb-dbt:latest"
