# Python 3.12+ is REQUIRED, not a preference: polymarket-apis declares
# Requires-Python >=3.12, so on 3.11 pip ignores every published version and fails
# with the confusing "Could not find a version ... (from versions: none)".
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Pre-create logs directory
RUN mkdir -p /app/logs

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

# Environment variables
ENV PORT=8000
ENV SYMBOL=BTCUSDT
ENV POLL_INTERVAL_MS=1000

EXPOSE 8000

# Gunicorn with a uvicorn worker. Exec form (JSON) so the process receives SIGTERM
# directly and shuts the websockets down cleanly; `sh -c` is still needed to expand
# $PORT. NOTE: keep -w 1 — the bot holds all its state in memory in one event loop,
# so a second worker would run a second independent bot against the same wallet.
CMD ["sh", "-c", "exec gunicorn -w 1 -k uvicorn.workers.UvicornWorker main:app --bind 0.0.0.0:$PORT"]
