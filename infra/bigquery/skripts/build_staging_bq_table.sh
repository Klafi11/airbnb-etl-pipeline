#!/usr/bin/env bash

set -euo pipefail

# Directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Paths relative to script folder
ENV_FILE="$SCRIPT_DIR/../.env"
SQL_FILE="$SCRIPT_DIR/../SQL_skripts/create_stg_airbnb_listings.sql"

# Validate .env file
if [[ ! -f "$ENV_FILE" ]]; then
  echo "❌ .env file not found at: $ENV_FILE"
  exit 1
fi

# Load variables
export $(grep -v '^#' "$ENV_FILE" | xargs)

# Validate variables
if [[ -z "${PROJECT_ID:-}" ]]; then
  echo "❌ PROJECT_ID variable is missing in .env"
  exit 1
fi

if [[ -z "${DATASET_STAGING:-}" ]]; then
  echo "❌ DATASET_STAGING variable is missing in .env"
  exit 1
fi

if [[ -z "${BQ_REGION:-}" ]]; then
  echo "❌ BQ_REGION variable is missing in .env (e.g., EU or US)"
  exit 1
fi

# Validate SQL file
if [[ ! -f "$SQL_FILE" ]]; then
  echo "❌ SQL file not found at: $SQL_FILE"
  exit 1
fi

echo "▶ Creating dataset: $PROJECT_ID:$DATASET_STAGING in region $BQ_REGION"

# Create dataset if not exists
bq --location="$BQ_REGION" mk -d "$PROJECT_ID:$DATASET_STAGING" || true

echo "▶ Running SQL from: $SQL_FILE"

# Substitute variables inside SQL and run it
envsubst < "$SQL_FILE" | bq query --use_legacy_sql=false

echo "✅ BigQuery dataset and tables created successfully!"

