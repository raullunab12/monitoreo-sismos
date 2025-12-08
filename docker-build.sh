#!/usr/bin/env bash
set -e

echo "Building ingestion image..."
docker build -t earthquake-ingestion:local services/ingestion

echo "Building processing image..."
docker build -t earthquake-processing:local services/processing

echo "Building alerts image..."
docker build -t earthquake-alerts:local services/alerts

echo "Building query image..."
docker build -t earthquake-query:local services/query

echo "Done."
