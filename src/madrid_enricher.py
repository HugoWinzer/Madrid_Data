import os, json, time
from typing import Dict, Any, List
from flask import Flask, jsonify, request
from google.cloud import bigquery

# -------- Config --------
PROJECT_ID = os.getenv("PROJECT_ID", "rfp-database-464609")
DATASET = os.getenv("BQ_DATASET", "rfpdata")
TABLE = os.getenv("BQ_TABLE_NAME", "performing_arts_madrid")
LOCATION = os.getenv("BQ_LOCATION", "europe-southwest1")
TABLE_FQN = f"`{PROJECT_ID}.{DATASET}.{TABLE}`"

# OpenAI (GPT) only
from openai import OpenAI, RateLimitError
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
client_ai = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

app = Flask(__name__)

TARGET_COLS = [
    "category","sub_category","city","site_event_entity","website",
    "owner_fever_new","contacted","ticketing_with","counterpart_for_ticketing_conversation",
    "visitors","visitors_per_event_capacity","atp","gtv","event_size_segment",
    "private_public","rfp","comments",
]
NUMERIC_FIELDS = {"visitors","visitors_per_event_capacity","atp","gtv"}
BOOLISH_FIELDS = {"contacted"}

SYSTEM_RULES = (
"You enrich a structured table of events/venues. "
"Fill ALL missing fields conservatively; no nulls. "
"Return STRICT JSON with the exact keys provided. "
'contacted must be "Yes" or "" (empty). '
'private_public must be "Private" or "Public". '
'event_size_segment is one of ["Diamond","Gold","Silver","Bronze"]. '
"Numbers must be plain numbers (e.g., 4000000, 85). URLs should include scheme."
)

USER_TMPL = """
Fill every empty field. Return ONLY JSON with these keys:

{keys}

Row (current values; nulls mean empty):
{row_json}

Examples (style):
- Category: "A - Music & Nightlife"
- Sub-Category: "A1 - Festivals"
- City: "Milano"
- Site/Event/Entity: "I-Days Milano Coca-Cola"
- Website: "https://www.idays.it"
- Owner Fever NEW: "Monica" | "Fran" | "Other" | ""
- Contacted: "Yes" or ""
- Ticketing w/: "Ticketmaster, TicketOne"
- Counterpart for ticketing conversation: "Associazione Culturale ..."
- Visitors: 400000
- Visitor per event/capacity: 80000
- ATP: 100
- GTV: 40000000
- "Event" size segment: "Diamond"
- Private/ Public: "Private"
- RFP: "" or short tag
- Comments: short note
"""

def bq():
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)

def _norm_numeric(x):
    if x in (None, ""): return None
    if isinstance(x, (int,float)): return x
    s = str(x).replace(",", "").strip()
    try: return float(s) if "." in s else int(s)
    except: return None

def _coerce(col: str, val: Any):
    if col in NUMERIC_FIELDS: return _norm_numeric(val)
    if col in BOOLISH_FIELDS:
        s = str(val).strip().lower()
        if s in {"yes","y","true","1"}: return "Yes"
        if s in {"no","n","false","0"}: return ""
        return ""
    return "" if val is None else str(val).strip()

def _row_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: (row.get(k) if row.get(k) not in (None,"") else None) for k in TARGET_COLS}

def call_gpt_fill(row: Dict[str, Any]) -> Dict[str, Any]:
    user = USER_TMPL.format(
        keys=json.dumps(TARGET_COLS, ensure_ascii=False),
        row_json=json.dumps(_row_payload(row), ensure_ascii=False, indent=2)
    )
    comp = client_ai.chat.completions.create(
        model=OPENAI_MODEL,
        temperature=0.3,
        messages=[
            {"role":"system","content":SYSTEM_RULES},
            {"role":"user","content":user},
        ]
    )
    txt = comp.choices[0].message.content
    # parse JSON
    try:
        data = json.loads(txt)
    except:
        s = txt[txt.find("{"):txt.rfind("}")+1]
        data = json.loads(s)
    # coerce types / sanitize
    return {c: _coerce(c, data.get(c)) for c in TARGET_COLS}

def fetch_rows_with_nulls(limit:int) -> List[Dict[str,Any]]:
    client = bq()
    sql = f"""
    SELECT
      site_event_entity, city, website,
      {", ".join(TARGET_COLS)}
    FROM {TABLE_FQN}
    WHERE {" OR ".join([f"{c} IS NULL" for c in TARGET_COLS])}
    ORDER BY COALESCE(gtv, 0) DESC
    LIMIT @lim
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("lim","INT64",limit)]
        ),
    )
    return [dict(r.items()) for r in job.result()]

def update_row(key: Dict[str,str], values: Dict[str,Any]):
    client = bq()
    sets = ", ".join([f"{c} = @{c}" for c in TARGET_COLS])
    params = []
    # param types
    for c in TARGET_COLS:
        if c in NUMERIC_FIELDS:
            p = bigquery.ScalarQueryParameter(c, "NUMERIC", values[c])
        else:
            p = bigquery.ScalarQueryParameter(c, "STRING", values[c])
        params.append(p)
    params += [
        bigquery.ScalarQueryParameter("k_name","STRING",key["site_event_entity"]),
        bigquery.ScalarQueryParameter("k_city","STRING",key.get("city") or ""),
        bigquery.ScalarQueryParameter("k_site","STRING",key.get("website") or ""),
    ]
    sql = f"""
    UPDATE {TABLE_FQN}
    SET {sets}
    WHERE site_event_entity = @k_name
      AND COALESCE(city,'') = @k_city
      AND COALESCE(website,'') = @k_site
    """
    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    job.result()

@app.get("/ping")
def ping(): return "pong", 200

@app.get("/ready")
def ready():
    _ = bq()
    return "ok", 200

@app.post("/enrich_all_nulls")
def enrich_all_nulls():
    """
    Runs GPT enrichment until a 429 is encountered, then stops immediately.
    Query args:
      batch (default 25), sleep (default 0.1), max_batches (default 9999)
    """
    batch = int(request.args.get("batch", "25"))
    sleep_s = float(request.args.get("sleep", "0.1"))
    max_batches = int(request.args.get("max_batches", "9999"))

    updated = 0
    batches_run = 0
    stopped_on_429 = False

    try:
        for _ in range(max_batches):
            rows = fetch_rows_with_nulls(limit=batch)
            if not rows:
                break
            for r in rows:
                key = {
                    "site_event_entity": r["site_event_entity"],
                    "city": r.get("city"),
                    "website": r.get("website"),
                }
                current = {c: r.get(c) for c in TARGET_COLS}
                try:
                    filled = call_gpt_fill(current)
                except RateLimitError:
                    stopped_on_429 = True
                    raise
                update_row(key, filled)
                updated += 1
                if sleep_s > 0: time.sleep(sleep_s)
            batches_run += 1
    except RateLimitError:
        pass

    status = "stopped_on_rate_limit" if stopped_on_429 else "ok"
    return jsonify({"status": status, "updated_rows": updated, "batches_run": batches_run}), 200
