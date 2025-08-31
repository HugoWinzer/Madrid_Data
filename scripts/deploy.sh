#!/usr/bin/env bash
set -euo pipefail

# Fixed to your environment
PROJECT_ID="rfp-database-464609"
REGION="us-central1"
SERVICE="rfp-data-enricher-madrid"
BQ_LOCATION="europe-southwest1"
BQ_TABLE="rfp-database-464609.rfpdata.performing_arts_madrid"

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/${SERVICE}:manual"

echo "Using: PROJECT_ID=$PROJECT_ID REGION=$REGION SERVICE=$SERVICE"
gcloud config set project "$PROJECT_ID"

# Ensure Artifact Registry exists and auth is configured
gcloud artifacts repositories create cloud-run-source-deploy \
  --repository-format=docker --location="$REGION" \
  --description="Images for Madrid Enricher" 2>/dev/null || true
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo -e "\n==> Build & Push"
docker build -t "$IMAGE" .
docker push "$IMAGE"

echo -e "\n==> Deploy to Cloud Run"
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" \
  --region "$REGION" \
  --allow-unauthenticated \
  --set-env-vars "BQ_LOCATION=$BQ_LOCATION,BQ_TABLE=$BQ_TABLE" \
  --cpu 1 --memory 512Mi --timeout 600 \
  --min-instances 0 --max-instances 2

SERVICE_URL=$(gcloud run services describe "$SERVICE" --region "$REGION" --format='value(status.url)')
echo "Service URL: $SERVICE_URL"

echo -e "\n==> Health checks"
curl -sS "$SERVICE_URL/ping"; echo
curl -sS "$SERVICE_URL/ready"; echo

echo -e "\n==> Dry-run preview"
curl -sS "$SERVICE_URL/?limit=3&dry=1" | sed -e 's/.*/\n--- Preview (truncated) ---\n&\n---------------------------/'; echo

# To write updates, run:
# curl -sS "$SERVICE_URL/?limit=3" | jq

