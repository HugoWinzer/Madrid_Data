#!/bin/bash
set -euo pipefail

# Config (change once, keep for all runs)
PROJECT_ID="rfp-database-464609"
REGION="europe-southwest1"
SERVICE="madrid-enricher"
REPO="rfp-docker-repo"

# Artifact Registry setup
gcloud artifacts repositories create "$REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID" || true

# Image name with timestamp
IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPO/${SERVICE}:$(date +%Y%m%d-%H%M%S)"

# Remote build (no local Docker needed)
gcloud builds submit --tag "$IMAGE" .

# Deploy to Cloud Run
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" \
  --region "$REGION" \
  --platform managed \
  --allow-unauthenticated \
  --max-instances=2 \
  --cpu=1 \
  --memory=512Mi \
  --set-env-vars BQ_LOCATION="$REGION" \
  --set-env-vars BQ_TABLE="rfp-database-464609.rfpdata.performing_arts_madrid" \
  --timeout=600

# Quick health checks (optional, if endpoints exist)
URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format 'value(status.url)')
echo "Deployed at $URL"
curl -fsS "$URL/ping" || true
curl -fsS "$URL/ready" || true
