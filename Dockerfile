FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
ENV PYTHONPATH=/app PORT=8080

# non-root
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

CMD ["bash","-lc","gunicorn -w 1 -b 0.0.0.0:${PORT:-8080} --timeout 600 src.madrid_enricher:app"]
