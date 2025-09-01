# path: src/madrid_enricher.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, jsonify, request
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from openai import OpenAI
from openai import APIStatusError, RateLimitError

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_TABLE = os.getenv("BQ_TABLE", "rfp-database-464609.rfpdata.performing_arts_madrid")
BQ_LOCATION = os.getenv("BQ_LOCATION", "europe-southwest1")
OAI_MODEL = os.getenv("OAI_MODEL", "gpt-4o-mini")

if BQ_TABLE.count(".") != 2:
    raise RuntimeError("BQ_TABLE must be fully-qualified: <project>.<dataset>.<table>")

BQ_PROJECT, BQ_DATASET, BQ_TBL = BQ_TABLE.split(".")

# ---- schema ----
STRING_FIELDS: Tuple[str, ...] = (
    "site_event_entity","city","category","sub_category","website",
    "event_size_segment","private_public","rfp","enrichment_status","comments",
    "avg_ticket_price_source","capacity_source","ticket_vendor_source",
    "owner_fever_new","contacted","ticketing_with","counterpart_for_ticketing_conversation",
)
NUMERIC_FIELDS: Tuple[str, ...] = ("visitors_per_event_capacity","avg_ticket_price","visitors","atp","gtv")
TIMESTAMP_FIELDS: Tuple[str, ...] = ("last_updated",)
TARGET_FIELDS: Tuple[str, ...] = STRING_FIELDS + NUMERIC_FIELDS
KEY_FIELDS: Tuple[str, str, str] = ("site_event_entity", "city", "website")

# ---- utils ----
def _decimal_or_none(v: Any) -> Optional[Decimal]:
    if v in (None, "", "null", "None"):
        return None
    try:
        return Decimal(str(v).replace(",", ""))
    except (InvalidOperation, ValueError, TypeError):
        return None

def _strip_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _jsonify_value(v: Any) -> Any:
    if isinstance(v, Decimal):
        try:
            return float(v)  # keep prompt JSON-safe
        except Exception:
            return str(v)
    return v

def _jsonify_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _jsonify_value(v) for k, v in d.items()}

def _fallback_for(field: str) -> Any:
    # Ensures we never leave NULLs behind if model omits a key
    return Decimal("0") if field in NUMERIC_FIELDS else ""

@dataclass
class RowKey:
    entity: str; city: str; website: str
    @classmethod
    def from_row(cls, row: Dict[str, Any]) -> "RowKey":
        return cls(row.get("site_event_entity") or "", row.get("city") or "", row.get("website") or "")

# ---- clients ----
app = Flask(__name__)
_bq_client = bigquery.Client(project=PROJECT_ID)
_oai_client: Optional[OpenAI] = OpenAI() if os.getenv("OPENAI_API_KEY") else None

# ---- bq helpers ----
def _null_predicate(cols: Iterable[str]) -> str:
    return " OR ".join([f"{col} IS NULL" for col in cols])

def _fetch_batch(limit: int) -> List[Dict[str, Any]]:
    where_any_null = _null_predicate(TARGET_FIELDS)
    sql = f"""
    SELECT {', '.join(KEY_FIELDS + TARGET_FIELDS)}
    FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TBL}`
    WHERE {where_any_null}
    ORDER BY last_updated IS NULL DESC, last_updated ASC
    LIMIT @limit
    """
    job = _bq_client.query(
        sql, location=BQ_LOCATION,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
        ),
    )
    return [dict(row) for row in job]

def _make_prompt(row: Dict[str, Any]) -> List[Dict[str, str]]:
    known_raw = {k: row.get(k) for k in TARGET_FIELDS if row.get(k) not in (None, "")}
    known = _jsonify_dict(known_raw)
    unknown = [k for k in TARGET_FIELDS if row.get(k) in (None, "")]
    system = (
        "You are a careful data enricher for a Madrid performing arts dataset. "
        "Return ONLY compact JSON with keys for ALL missing fields. "
        "Preserve provided values exactly. Prefer EUR for prices."
    )
    user = {
        "task": "Fill all missing fields so none are null.",
        "entity": row.get("site_event_entity"),
        "city": row.get("city"),
        "website": row.get("website"),
        "known_fields": known,
        "need_fields": unknown,
        "output_schema": {"strings": list(STRING_FIELDS), "numerics": list(NUMERIC_FIELDS)},
    }
    return [{"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user)}]

def _enrich_one(row: Dict[str, Any]) -> Dict[str, Any]:
    if _oai_client is None:
        raise RuntimeError("OPENAI_API_KEY missing")
    unknown = [k for k in TARGET_FIELDS if row.get(k) in (None, "")]
    resp = _oai_client.chat.completions.create(
        model=OAI_MODEL, temperature=0.2,
        response_format={"type": "json_object"},
        messages=_make_prompt(row),  # type: ignore[arg-type]
    )
    txt = resp.choices[0].message.content or "{}"
    data: Dict[str, Any] = json.loads(txt)

    patch: Dict[str, Any] = {}
    for k in STRING_FIELDS:
        val = _strip_or_none(data.get(k))
        if val is not None:
            patch[k] = val
    for k in NUMERIC_FIELDS:
        val = _decimal_or_none(data.get(k))
        if val is not None:
            patch[k] = val

    # Backfill any keys the model skipped
    for k in unknown:
        if k not in patch:
            patch[k] = _fallback_for(k)

    return patch

def _update_row(key: RowKey, patch: Dict[str, Any]) -> None:
    # Build update: never overwrite non-null values
    set_parts: List[str] = [f"{col} = COALESCE({col}, @{col})" for col in patch.keys()]
    set_parts += ["enrichment_status = 'enriched'", "last_updated = CURRENT_TIMESTAMP()"]
    set_sql = ", ".join(set_parts) if set_parts else "enrichment_status = 'enriched', last_updated = CURRENT_TIMESTAMP()"

    # IMPORTANT: handle NULL keys with IFNULL on ALL key columns
    sql = f"""
    UPDATE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TBL}`
    SET {set_sql}
    WHERE IFNULL(site_event_entity, '') = @k_entity
      AND IFNULL(city, '') = @k_city
      AND IFNULL(website, '') = @k_website
    """
    params: List[bigquery.ScalarQueryParameter] = [
        bigquery.ScalarQueryParameter("k_entity", "STRING", key.entity),
        bigquery.ScalarQueryParameter("k_city", "STRING", key.city or ""),
        bigquery.ScalarQueryParameter("k_website", "STRING", key.website or ""),
    ]
    for col, val in patch.items():
        typ = "NUMERIC" if col in NUMERIC_FIELDS else "STRING"
        params.append(bigquery.ScalarQueryParameter(col, typ, val))

    _bq_client.query(
        sql, location=BQ_LOCATION,
        job_config=bigquery.QueryJobConfig(query_parameters=params)
    ).result()

# ---- routes ----
@app.get("/ping")
def ping():
    return jsonify({"ok": True})

@app.get("/ready")
def ready():
    ok, errs = True, []
    try:
        _ = _bq_client.query("SELECT 1").result()
    except GoogleAPIError as e:
        ok, errs = False, [f"bq:{e.__class__.__name__}"]
    if _oai_client is None:
        ok, errs = False, errs + ["openai:key_missing"]
    return (jsonify({"ok": ok, "errors": errs}), 200 if ok else 500)

def _run_enrichment(batch: int, sleep: float, max_batches: int) -> Dict[str, Any]:
    updated, batches = 0, 0
    while batches < max_batches:
        rows = _fetch_batch(batch)
        if not rows:
            break
        for r in rows:
            key = RowKey.from_row(r)
            try:
                patch = _enrich_one(r)
            except RateLimitError:
                return {"status":"stopped_on_rate_limit","updated":updated,"batches":batches}
            except APIStatusError:
                continue
            except Exception:
                continue
            try:
                _update_row(key, patch)
                updated += 1
            except GoogleAPIError:
                pass
            time.sleep(sleep)
        batches += 1
    return {
        "status": "done" if batches < max_batches else "stopped_on_max_batches",
        "updated": updated,
        "batches": batches,
    }

@app.post("/enrich")
def enrich():
    if _oai_client is None:
        return jsonify({"status":"error","error":"OPENAI_API_KEY missing"}), 500
    try:
        batch = int(request.args.get("batch","25"))
        sleep = float(request.args.get("sleep","0.15"))
        max_batches = int(request.args.get("max_batches","9999"))
    except Exception:
        return jsonify({"status":"error","error":"bad query params"}), 400
    return jsonify(_run_enrichment(batch, sleep, max_batches))

# Preset for ~200 rows: 10x20 with gentle pacing
@app.post("/enrich_200")
def enrich_200():
    if _oai_client is None:
        return jsonify({"status":"error","error":"OPENAI_API_KEY missing"}), 500
    return jsonify(_run_enrichment(batch=10, sleep=0.2, max_batches=20))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","8080")), debug=False)
