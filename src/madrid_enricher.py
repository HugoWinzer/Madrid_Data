import os, json, time
from typing import Any, Dict, List
from flask import Flask, jsonify, request
from google.cloud import bigquery
from openai import OpenAI, RateLimitError

# --- Config ---
PROJECT_ID=os.getenv("PROJECT_ID","rfp-database-464609")
DATASET=os.getenv("BQ_DATASET","rfpdata")
TABLE=os.getenv("BQ_TABLE_NAME","performing_arts_madrid")
LOCATION=os.getenv("BQ_LOCATION","europe-southwest1")
TABLE_FQN=f"`{PROJECT_ID}.{DATASET}.{TABLE}`"
OPENAI_MODEL=os.getenv("OPENAI_MODEL","gpt-4o-mini")
OPENAI_API_KEY=os.environ["OPENAI_API_KEY"]

TARGET_COLS=[
  "category","sub_category","city","site_event_entity","website",
  "owner_fever_new","contacted","ticketing_with","counterpart_for_ticketing_conversation",
  "visitors","visitors_per_event_capacity","atp","gtv","event_size_segment",
  "private_public","rfp","comments",
]
NUMERIC={"visitors","visitors_per_event_capacity","atp","gtv"}
BOOLISH={"contacted"}

SYSTEM_RULES=("You enrich a structured table. Fill ALL missing fields; no nulls. "
"Return STRICT JSON with exactly these keys: "+", ".join(TARGET_COLS)+". "
'contacted must be "Yes" or "" ; private_public is "Private" or "Public" ; '
'event_size_segment one of "Diamond","Gold","Silver","Bronze". '
"Numbers must be plain numbers. URLs include scheme.")

USER_TMPL="""Fill every empty field. Return ONLY JSON with these keys:
{keys}

Row (current values; nulls mean empty):
{row_json}
"""

app=Flask(__name__)
ai=OpenAI(api_key=OPENAI_API_KEY)

def bq(): return bigquery.Client(project=PROJECT_ID, location=LOCATION)

def norm_num(x):
  if x in (None,""): return None
  if isinstance(x,(int,float)): return x
  s=str(x).replace(",","").strip()
  try: return float(s) if "." in s else int(s)
  except: return None

def coerce(col,val):
  if col in NUMERIC: return norm_num(val)
  if col in BOOLISH:
    s=str(val or "").strip().lower()
    return "Yes" if s in {"yes","y","true","1"} else ""
  return "" if val is None else str(val).strip()

def row_payload(r:Dict[str,Any])->Dict[str,Any]:
  return {k:(r.get(k) if r.get(k) not in (None,"") else None) for k in TARGET_COLS}

def call_gpt_fill(current:Dict[str,Any])->Dict[str,Any]:
  user=USER_TMPL.format(keys=json.dumps(TARGET_COLS), row_json=json.dumps(row_payload(current)))
  resp=ai.chat.completions.create(
    model=OPENAI_MODEL, temperature=0.3,
    messages=[{"role":"system","content":SYSTEM_RULES},{"role":"user","content":user}]
  )
  txt=resp.choices[0].message.content
  try:
    data=json.loads(txt)
  except:
    s=txt[txt.find("{"):txt.rfind("}")+1]
    data=json.loads(s)
  return {c:coerce(c,data.get(c)) for c in TARGET_COLS}

def fetch_rows_with_nulls(limit:int)->List[Dict[str,Any]]:
  sql=f"""
  SELECT site_event_entity, city, website, {", ".join(TARGET_COLS)}
  FROM {TABLE_FQN}
  WHERE {" OR ".join([f"{c} IS NULL" for c in TARGET_COLS])}
  ORDER BY COALESCE(gtv,0) DESC
  LIMIT @lim
  """
  job=bq().query(sql, job_config=bigquery.QueryJobConfig(
      query_parameters=[bigquery.ScalarQueryParameter("lim","INT64",limit)]
  ))
  return [dict(r.items()) for r in job.result()]

def update_row(key:Dict[str,str],vals:Dict[str,Any]):
  sets=", ".join([f"{c}=@{c}" for c in TARGET_COLS])
  params=[]
  for c in TARGET_COLS:
    t="NUMERIC" if c in NUMERIC else "STRING"
    params.append(bigquery.ScalarQueryParameter(c,t,vals[c]))
  params += [
    bigquery.ScalarQueryParameter("k_name","STRING",key["site_event_entity"]),
    bigquery.ScalarQueryParameter("k_city","STRING",key.get("city") or ""),
    bigquery.ScalarQueryParameter("k_site","STRING",key.get("website") or ""),
  ]
  sql=f"""
  UPDATE {TABLE_FQN}
  SET {sets}
  WHERE site_event_entity=@k_name AND COALESCE(city,'')=@k_city AND COALESCE(website,'')=@k_site
  """
  bq().query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

@app.get("/ping")
def ping(): return "pong",200

@app.post("/enrich_all_nulls")
def enrich_all_nulls():
  batch=int(request.args.get("batch","25"))
  sleep_s=float(request.args.get("sleep","0.1"))
  max_batches=int(request.args.get("max_batches","9999"))
  updated=0; batches=0; stopped=False
  try:
    for _ in range(max_batches):
      rows=fetch_rows_with_nulls(batch)
      if not rows: break
      for r in rows:
        key={"site_event_entity":r["site_event_entity"],"city":r.get("city"),"website":r.get("website")}
        current={c:r.get(c) for c in TARGET_COLS}
        try:
          filled=call_gpt_fill(current)
        except RateLimitError:
          stopped=True; raise
        update_row(key,filled); updated+=1
        if sleep_s>0: time.sleep(sleep_s)
      batches+=1
  except RateLimitError:
    pass
  return jsonify({"status":("stopped_on_rate_limit" if stopped else "ok"),"updated_rows":updated,"batches_run":batches}),200
EOF
