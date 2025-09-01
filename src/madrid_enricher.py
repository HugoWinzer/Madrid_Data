"""Madrid Performing Arts Enricher API




@app.post("/enrich")
def enrich():
if _oai_client is None:
return jsonify({"status": "error", "error": "OPENAI_API_KEY missing"}), 500


try:
batch = int(request.args.get("batch", "25"))
sleep = float(request.args.get("sleep", "0.15"))
max_batches = int(request.args.get("max_batches", "9999"))
except Exception:
return jsonify({"status": "error", "error": "bad query params"}), 400


updated = 0
batches = 0
while batches < max_batches:
rows = _fetch_batch(batch)
if not rows:
break
for r in rows:
key = RowKey.from_row(r)
try:
patch = _enrich_one(r)
except RateLimitError:
return jsonify({
"status": "stopped_on_rate_limit",
"updated": updated,
"batch": batches,
})
except APIStatusError:
continue
except Exception:
continue


if patch:
try:
_update_row(key, patch)
updated += 1
except GoogleAPIError:
pass
time.sleep(sleep)
batches += 1


return jsonify({
"status": "done" if batches < max_batches else "stopped_on_max_batches",
"updated": updated,
"batches": batches,
})




if __name__ == "__main__":
# Local dev server
app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)
