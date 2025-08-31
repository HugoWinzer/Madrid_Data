# Madrid Enricher – Minimal Cloud Run App (GitHub-first)

This app reads rows from BigQuery `rfp-database-464609.rfpdata.performing_arts_madrid`, derives simple defaults, and writes updates.

## Deploy from shell (one-off or ad-hoc)
```bash
chmod +x scripts/deploy.sh
scripts/deploy.sh

