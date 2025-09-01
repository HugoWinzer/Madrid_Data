import os
import json
import time
from typing import Dict, Any, List

from flask import Flask, jsonify, request
from google.cloud import bigquery

# ==== LLM: Choose one ====
# A) Vertex AI (Gemini) – recommended in GCP
USE_VERTEX = True

# B) OpenAI – set USE_VERTEX=False and export OPENAI_API_KEY
if not USE_VERTEX:
    import openai  # type: ignore

app = Flask(__name__)

PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET = os.getenv("BQ_DATASET", "rfpdata")
TABLE = os.getenv("BQ_TABLE_NAME", "performing_arts_madrid")  # table name only
TABLE_FQN = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"
REGION = os.getenv("BQ_LOCATION", "europe-southwest1")

# ---- Columns we maintain (business schema) ----
TARGET_COLS = [
    "category",
    "sub_category",
    "city",
    "site_event_entity",
    "website",
    "owner_fever_new",
    "contacted",
    "ticketing_with",
    "counterpart_for_ticketing_conversation",
    "visitors",
    "visitors_per_event_capacity",
    "atp",
    "gtv",
    "event_size_segment",
    "private_public",
    "rfp",
    "comments",
]

# Types for light validation/conversion
NUMERIC_FIELDS = {"visitors", "visitors_per_event_capacity", "atp", "gtv"}
BOOLISH_FIELDS = {"contacted"}  # we'll coerce yes/true/y -> "Yes" else blank

# ----------- Helpers -----------
def bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID, location=REGION)

def _coerce_value(col: str, val: Any):
    if val is None:
        return None
    if col in NUMERIC_FIELDS:
        # accept "40,000,000", "40000", 40000 -> numeric string (BigQuery NUMERIC)
        if isinstance(val, (int, float)):
            return val
        s = str(val).replace(",", "").strip()
        try:
            return float(s) if "." in s else int(s)
        except Exception:
            return None
    if col in BOOLISH_FIELDS:
        s = str(val).strip().lower()
        if s in {"yes", "true", "y", "1"}:
            return "Yes"
        if s in {"no", "false", "n", "0"}:
            return "No"
        return None
    # everything else -> string
    return str(val).strip()

def _format_json_for_prompt(row: Dict[str, Any]) -> str:
    # Keep only our target cols; pass current values
    payload = {k: (row.get(k) if row.get(k) not in ("", None) else None) for k in TARGET_COLS}
    return json.dumps(payload, ensure_ascii=False, indent=2)

SYSTEM_RULES = """
You are enriching a structured table of cultural events/venues for business development.
Goal: fill ALL missing fields based on public patterns, domain name, and consistency. Use sensible, conservative estimates. 
- Output STRICT JSON with the exact keys provided.
- Do not invent organizations that conflict with the given site/name/city.
- ticketing_with: list of platforms separated by commas (e.g., "Ticketmaster, TicketOne"), or a single platform.
- counterpart_for_ticketing_conversation: responsible org/person if known; otherwise empty string.
- visitors: total annual visitors or event attendance as a number.
- visitors_per_event_capacity: single-event peak capacity as a number.
- atp: average ticket price (number, in local currency if known).
- gtv: gross ticket value = visitors * atp (approximate).
- event_size_segment: one of ["Diamond","Gold","Silver","Bronze"] based on gtv (Diamond≈very large).
- private_public: "Private" or "Public" (if owner appears municipal/state, use "Public"; else "Private").
- contacted: "Yes" or "".
- owner_fever_new: a short internal owner tag ("Monica","Fran","Other", or "").
- Keep style concise. If unsure, return your best reasonable inference (no "null", no placeholders).
"""

USER_TEMPLATE = """
Here is the row with some empty fields. Fill every empty field.
Return ONLY JSON with these exact keys and string/number values (no arrays):

{json_payload}

Examples of the desired style:
Category: "A - Music & Nightlife"
Sub-Category: "A1 - Festivals"
City: "Milano"
Site/Event/Entity: "I-Days Milano Coca-Cola"
Website: "https://www.idays.it"
Owner Fever NEW: "Other" | "Monica" | "Fran"
Contacted: "Yes" or ""
Ticketing w/: "Ticketmaster, TicketOne"
Counterpart for ticketing conversation: "Associazione Culturale ..."
Visitors: 400000
Visitor per event/capacity: 80000
ATP: 100
GTV: 40000000
"Event" size segment: "Diamond"
Private/ Public: "Private"
RFP: "" or short tag
Comments: short note

IMPORTANT:
- Use numbers for Visitors, Visitor per event/capacity, ATP, GTV.
- No fields may be empty/null unless explicitly instructed to allow "" (string) for contacted/comments when truly unknown; otherwise estimate.
- Keep URLs well-formed (include scheme when possible).
"""

def call_llm_fill(row: Dict[str, Any]) -> Dict[str, Any]:
    # Build prompt
    user_msg = USER_TEMPLATE.format(json_payload=_format_json_for_prompt(row))

    if USE_VERTEX:
        # Vertex AI (Gemini)
        # pip install google-cloud-aiplatform
        from vertexai.generative_models import GenerativeModel
        import vertexai

        vertexai.init(project=PROJECT_ID, location=REGION)
        model = GenerativeModel(os.getenv("GEMINI_MODEL", "gemini-1.5-pro"))
        resp = model.generate_content(
            [
                {"role": "user", "parts": [SYSTEM_RULES.strip()]},
                {"role": "user", "parts": [user_msg.strip()]},
            ],
            generation_config={"temperature": 0.3, "max_output_tokens": 1024},
        )
        text = resp.text
    else:
        # OpenAI
        openai.api_key = os.environ["OPENAI_API_KEY"]
        comp = openai.ChatCompletion.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            temperature=0.3,
            messages=[
                {"role": "system", "content": SYSTEM_RULES},
                {"role": "user", "content": user_msg},
            ],
        )
        text = comp.choices[0].message["content"]

    # Parse JSON
    try:
        j = json.loads(text)
    except Exception:
        # Attempt to extract JSON substring
        start = text.find("{")
        end = text.rfind("}")
        j = json.loads(text[start:end + 1])

    # Coerce types & sanitize
    cleaned = {}
    for col in TARGET_COLS:
        cleaned[col] = _coerce_value(col, j.get(col))
    return cleaned

def fetch_rows_with_nulls(limit: int = 25) -> List[Dict[str, Any]]:
    client = bq()
    # Use your business key: (site_event_entity, city, website). Pull current values for all target columns.
    sql = f"""
    SELECT
      site_event_entity, city, website,
      {", ".join(TARGET_COLS)}
    FROM {TABLE_FQN}
    WHERE (
      {" OR ".join([f"{c} IS NULL" for c in TARGET_COLS])}
      OR TRIM(CAST(contacted AS STRING)) IS NULL
      OR TRIM(CAST(comments AS STRING)) IS NULL
    )
    ORDER BY COALESCE(gtv, 0) DESC
    LIMIT @lim
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("lim", "INT64", limit)]
    ))
    return [dict(r.items()) for r in job.result()]

def update_row(key: Dict[str, str], values: Dict[str, Any]):
    client = bq()
    # Upsert by (site_event_entity, city, website)
    sets = ", ".join([f"{col} = @{col}" for col in TARGET_COLS])
    sql = f"""
    UPDATE {TABLE_FQN}
    SET {sets}
    WHERE site_event_entity = @k_name
      AND COALESCE(city, '') = @k_city
      AND COALESCE(website, '') = @k_site
    """
    params = [bigquery.ScalarQueryParameter(col, "STRING" if col not in NUMERIC_FIELDS else "NUMERIC", values[col])
              for col in TARGET_COLS]
    params.extend([
        bigquery.ScalarQueryParameter("k_name", "STRING", key["site_event_entity"]),
        bigquery.ScalarQueryParameter("k_city", "STRING", key.get("city") or ""),
        bigquery.ScalarQueryParameter("k_site", "STRING", key.get("website") or ""),
    ])

    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()

@app.get("/ping")
def ping():
    return "pong", 200

@app.get("/ready")
def ready():
    _ = bq()  # client init
    return "ok", 200

@app.post("/enrich_all_nulls")
def enrich_all_nulls():
    """Fills every empty cell for rows with any NULLs, in batches."""
    batch = int(request.args.get("batch", "25"))
    max_batches = int(request.args.get("max_batches", "100"))
    sleep_s = float(request.args.get("sleep", "0.1"))
    total_updated = 0

    for i in range(max_batches):
        rows = fetch_rows_with_nulls(limit=batch)
        if not rows:
            break

        for r in rows:
            key = {
                "site_event_entity": r["site_event_entity"],
                "city": r.get("city"),
                "website": r.get("website"),
            }
            # Build input dict for the model (current values)
            current = {c: r.get(c) for c in TARGET_COLS}
            filled = call_llm_fill(current)

            # Ensure mandatory identity fields preserved
            if not filled.get("site_event_entity"):
                filled["site_event_entity"] = key["site_event_entity"]
            if not filled.get("city"):
                filled["city"] = key.get("city") or ""
            if not filled.get("website"):
                filled["website"] = key.get("website") or ""

            update_row(key, filled)
            total_updated += 1
            time.sleep(sleep_s)

    return jsonify({"updated_rows": total_updated}), 200
