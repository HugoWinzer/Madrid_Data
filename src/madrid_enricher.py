import os
import datetime
import logging
from flask import Flask, request, jsonify
from google.cloud import bigquery

# ---- fixed to your environment ----
DEFAULT_BQ_LOCATION = "europe-southwest1"
DEFAULT_BQ_TABLE = "rfp-database-464609.rfpdata.performing_arts_madrid"
# -----------------------------------

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("madrid_enricher")

BQ_LOCATION = os.getenv("BQ_LOCATION", DEFAULT_BQ_LOCATION)
BQ_TABLE = os.getenv("BQ_TABLE", DEFAULT_BQ_TABLE)
bq_client = bigquery.Client()

@app.get("/ping")
def ping():
    return "pong"

@app.get("/ready")
def ready():
    try:
        # simple query + table existence check
        _ = list(bq_client.query("SELECT 1", job_config=bigquery.QueryJobConfig(location=BQ_LOCATION)).result())
        bq_client.get_table(BQ_TABLE)
        return jsonify({"status": "ok", "bq_location": BQ_LOCATION, "table": BQ_TABLE})
    except Exception as e:
        LOGGER.exception("/ready failed")
        return jsonify({"status": "error", "reason": str(e)}), 500

def _select_rows(limit: int):
    query = f"""
    SELECT name, domain, city, country, capacity, avg_ticket_price, category, sub_category,
           capacity_final, atp, gtv, notes
    FROM `{BQ_TABLE}`
    WHERE (COALESCE(enrichment_status, '') != 'OK') OR gtv IS NULL
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        location=BQ_LOCATION,
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)],
    )
    return list(bq_client.query(query, job_config=job_config).result())

def _derive_updates(row: bigquery.table.Row) -> dict:
    domain = (row.get("domain") or "").lower()
    category = row.get("category")
    sub_category = row.get("sub_category")

    # Simple categorization
    if not category:
        if "wizink" in domain:
            category = "Arena"; sub_category = sub_category or "Indoor Arena"
        elif "teatro" in domain:
            category = "Theater"
        elif "museo" in domain or "prado" in domain:
            category = "Museum"

    capacity_final = row.get("capacity_final") or row.get("capacity")

    atp = row.get("atp")
    if atp is None:
        if category == "Arena":
            atp = 55.0
        elif category == "Theater":
            atp = 40.0
        elif category == "Museum":
            atp = 20.0

    gtv = row.get("gtv")
    if gtv is None and capacity_final is not None and atp is not None:
        try:
            gtv = float(capacity_final) * float(atp)
        except Exception:
            gtv = None

    notes_append = f" [auto:{datetime.date.today().isoformat()}]"

    return {
        "category": category,
        "sub_category": sub_category,
        "capacity_final": capacity_final,
        "atp": atp,
        "gtv": gtv,
        "notes_append": notes_append,
    }

def _update_row(name: str, domain: str, updates: dict) -> int:
    update_sql = f"""
    UPDATE `{BQ_TABLE}` SET
      capacity_final = COALESCE(@capacity_final, capacity_final),
      atp = COALESCE(@atp, atp),
      category = COALESCE(@category, category),
      sub_category = COALESCE(@sub_category, sub_category),
      gtv = COALESCE(@gtv, gtv),
      enrichment_status = 'OK',
      notes = IFNULL(notes, '') || @notes_append,
      last_updated = CURRENT_TIMESTAMP()
    WHERE name = @name AND domain = @domain
    """
    params = [
        bigquery.ScalarQueryParameter("capacity_final", "INT64", updates.get("capacity_final")),
        bigquery.ScalarQueryParameter("atp", "FLOAT64", updates.get("atp")),
        bigquery.ScalarQueryParameter("category", "STRING", updates.get("category")),
        bigquery.ScalarQueryParameter("sub_category", "STRING", updates.get("sub_category")),
        bigquery.ScalarQueryParameter("gtv", "FLOAT64", updates.get("gtv")),
        bigquery.ScalarQueryParameter("notes_append", "STRING", updates.get("notes_append")),
        bigquery.ScalarQueryParameter("name", "STRING", name),
        bigquery.ScalarQueryParameter("domain", "STRING", domain),
    ]
    job = bq_client.query(update_sql, job_config=bigquery.QueryJobConfig(location=BQ_LOCATION, query_parameters=params))
    _ = job.result()
    return getattr(job, "num_dml_affected_rows", 0) or 0

@app.get("/")
def run_enrichment():
    try:
        limit = int(request.args.get("limit", "5"))
        dry = request.args.get("dry", "0").lower() in ("1", "true", "t", "yes", "y")
    except Exception:
        return jsonify({"error": "invalid query params"}), 400

    rows = _select_rows(limit)
    preview = []
    updated = 0

    for r in rows:
        name, domain = r.get("name"), r.get("domain")
        upd = _derive_updates(r)
        preview.append({"name": name, "domain": domain, "derived": {k: v for k, v in upd.items() if k != "notes_append"}})
        if not dry:
            updated += _update_row(name, domain, upd)

    out = {"count": len(rows), "dry": dry}
    if dry:
        out["preview"] = preview
    else:
        out["updated_rows"] = updated
    return jsonify(out)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=True)
