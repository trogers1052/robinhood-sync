FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY robinhood_sync/ ./robinhood_sync/

# Create non-root user
RUN useradd -m -u 1000 appuser
USER appuser

# Default command: run continuous sync
CMD ["python", "-m", "robinhood_sync.main"]
