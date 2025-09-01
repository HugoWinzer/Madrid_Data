# path: scripts/deploy.sh
#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="rfp-database-464609"
REGION="europe-southwest1"
SERVICE="madrid-enricher"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}:$(date +%Y%m%d-%H%M%S)"

gcloud config set project "${PROJECT_ID}"

# Ensure secret exists (you will be prompted to paste the rotated key)
if ! gcloud secrets describe OPENAI_API_KEY >/dev/null 2>&1; then
  echo "Creating OPENAI_API_KEY secret..."
  gcloud secrets create OPENAI_API_KEY --replication-policy=automatic
  read -r -p "Paste new OpenAI API key: " OAI
  printf "%s" "$OAI" | gcloud secrets versions add OPENAI_API_KEY --data-file=-
fi

# Build & push
gcloud builds submit --tag "${IMAGE}" .

# Deploy to Cloud Run
gcloud run deploy "${SERVICE}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --allow-unauthenticated \
  --update-env-vars BQ_TABLE=rfp-database-464609.rfpdata.performing_arts_madrid,BQ_LOCATION=europe-southwest1 \
  --update-secrets OPENAI_API_KEY=OPENAI_API_KEY:latest

URL=$(gcloud run services describe "${SERVICE}" --region "${REGION}" --format='value(status.url)')
echo "Service URL: $URL"

# Smoke tests
curl -s "$URL/ping"; echo
curl -s "$URL/ready"; echo
curl -s -X POST "$URL/enrich?batch=25&sleep=0.2&max_batches=50"; echo
