FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install Python dependencies.
#
# git is required at build time only: trading-py-commons and
# trading-event-schemas are installed from `git+https://` URLs. It is installed
# and purged in a single layer so it never reaches the runtime image.
COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove git \
    && rm -rf /var/lib/apt/lists/*

# Copy application code
COPY robinhood_sync/ ./robinhood_sync/

# Create non-root user
RUN useradd -m -u 1000 appuser
USER appuser

# Default command: run continuous sync
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=15s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

CMD ["python", "-m", "robinhood_sync.main"]
