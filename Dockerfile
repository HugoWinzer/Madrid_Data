FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY src/ ./src/
ENV PYTHONPATH=/app PORT=8080

# Non-root user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Cloud Run provides $PORT; default to 8080 if not set
CMD ["bash","-lc","gunicorn -w 1 -b 0.0.0.0:${PORT:-8080} --timeout 600 src.madrid_enricher:app"]
