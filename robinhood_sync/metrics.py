"""Prometheus metrics for robinhood-sync."""

import logging
import os

logger = logging.getLogger(__name__)

_DEFAULT_PORT = 9095


def start_metrics_server() -> None:
    """Start Prometheus metrics HTTP server on METRICS_PORT (default 9095)."""
    try:
        from prometheus_client import start_http_server
    except ImportError:
        logger.warning("prometheus_client not installed — metrics endpoint disabled")
        return
    port = int(os.environ.get("METRICS_PORT", str(_DEFAULT_PORT)))
    start_http_server(port)
    logger.info(f"Metrics server listening on :{port}/metrics")
