import os, json, time
from typing import Any, Dict, List
from flask import Flask, jsonify, request
from google.cloud import bigquery
from openai import OpenAI
from httpx import HTTPStatusError

# ---------- config from env ----------
PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET    = os.getenv("BQ_DATASET", "rfpdata")
TABLE      = os.getenv("BQ_TABLE_NAME", "performing_arts_madrid")
LOCATION   = os.getenv("BQ_LOCATION", "europe-southwest1")
MODEL      = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # comes from Secret or env

TABLE_FQN  = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"

# Target columns (final Madrid schema)
TARGET_COLS = [
  "category","sub_category","city","site_event_entity","website",
  "owner_fever_new","contacted","ticketing_with","counterpart_for_ticketing_conversation",
  "visitors","visitors_per_event_capacity","atp","gtv",
  "event_size_segment","private_public","rfp","comments",
]
NUMERIC = {"visitors","visitors_per_event_capacity","atp","gtv"}

SYSTEM_PROMPT = (
  "You enrich missing fields for live events.\n"
  f"Return ONLY valid JSON with exactly these keys: {', '.join(TARGET_COLS)}.\n"
  "Rules:\n"
  "- 'contacted' must be 'Yes' or ''.\n"
  "- 'private_public' in {'Private','Public'}.\n"
  "- 'event_size_segment' in {'Diamond','Gold','Silver','Bronze'}.\n"
  "- Numbers must be plain numbers (no commas), URLs must include scheme.\n"
)

USER_TMPL = """Row (empty keys are nulls):
{row}

Fill every missing value. Keep known values consistent with the row. Return JSON only.
"""

app = Flask(__name__)
bq = bigquery.Client(project=PROJECT_ID, location=LOCATION)
oai = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------- helpers ----------
def _null_predicate() -> str:
    return " OR ".join([f"{c} IS NULL" for c in TARGET_COLS])

def _safe_number(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return None

def _coerce_types(d: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k in TARGET_COLS:
        v = d.get(k)
        if k in NUMERIC:
            out[k] = _safe_number(v)
        else:
            out[k] = None if v is None else str(v).strip()
    return out

def _update_row(row_key: str, patch: Dict[str, Any]) -> None:
    # only set columns that are currently NULL for this (name|domain) key
    sets = [f"{k} = COALESCE({k}, @{k})" for k in TARGET_COLS]

    params = [bigquery.ScalarQueryParameter("row_key", "STRING", row_key)]
    for k in TARGET_COLS:
        if k in NUMERIC:
            params.append(bigquery.ScalarQueryParameter(k, "NUMERIC", patch[k]))
        else:
            params.append(bigquery.ScalarQueryParameter(k, "STRING", patch[k]))

    sql = f"""
    UPDATE {TABLE_FQN}
    SET {', '.join(sets)}
    WHERE CONCAT(COALESCE(LOWER(name),''),'|',COALESCE(LOWER(domain),'')) = @row_key
    """
    bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

def _fetch_batch(limit: int) -> List[Dict[str, Any]]:
    sql = f"""
    SELECT
      CONCAT(COALESCE(LOWER(name),''),'|',COALESCE(LOWER(domain),'')) AS _key,
      name, domain, website, city, country, source_url, linkedin_url,
      {', '.join(TARGET_COLS)}
    FROM {TABLE_FQN}
    WHERE {_null_predicate()}
    ORDER BY name NULLS LAST
    LIMIT @lim
    """
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("lim","INT64", limit)]
    ))
    return [dict(r) for r in job.result()]

def _enrich_one(rec: Dict[str, Any]) -> Dict[str, Any]:
    base = {k: rec.get(k) for k in TARGET_COLS}
    prompt = USER_TMPL.format(row=json.dumps({**rec, **base}, ensure_ascii=False))
    resp = oai.chat.completions.create(
        model=MODEL,
        messages=[{"role":"system","content":SYSTEM_PROMPT},
                  {"role":"user","content":prompt}],
        temperature=0.1,
    )
    txt = resp.choices[0].message.content.strip()
    if txt.startswith("```"):
        txt = "\n".join([line for line in txt.split("\n") if not line.startswith("```")])
    data = json.loads(txt)
    return _coerce_types(data)

# ---------- routes ----------
@app.get("/ping")
def ping(): return "pong"

@app.get("/__routes")
def routes():
    return jsonify(["/__routes","/ping","/probe/bq","/probe/openai","/enrich_all_nulls"])

@app.get("/probe/bq")
def probe_bq():
    try:
        list(bq.query("SELECT 1").result())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/probe/openai")
def probe_openai():
    try:
        if not oai:
            return jsonify({"ok": False, "error": "OPENAI_API_KEY missing"}), 500
        r = oai.chat.completions.create(
            model=MODEL, messages=[{"role":"user","content":"ok?"}], temperature=0
        )
        return jsonify({"ok": "ok" in r.choices[0].message.content.lower()})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.post("/enrich_all_nulls")
def enrich():
    if not oai:
        return jsonify({"status":"error","error":"OPENAI_API_KEY missing"}), 500

    try:
        batch = int(request.args.get("batch","25"))
        sleep = float(request.args.get("sleep","0.15"))
        max_batches = int(request.args.get("max_batches","9999"))
    except Exception:
        return jsonify({"status":"error","error":"bad query params"}), 400

    total_updated = 0
    for _ in range(max_batches):
        rows = _fetch_batch(batch)
        if not rows:
            return jsonify({"status":"done","updated":total_updated})

        for r in rows:
            key = r["_key"]
            try:
                patch = _enrich_one(r)
            except HTTPStatusError as he:
                if he.response is not None and he.response.status_code == 429:
                    return jsonify({"status":"stopped_on_rate_limit","updated":total_updated})
                continue
            except Exception:
                continue

            try:
                _update_row(key, patch)
                total_updated += 1
            except Exception:
                pass

            time.sleep(sleep)

    return jsonify({"status":"stopped_on_max_batches","updated":total_updated})
