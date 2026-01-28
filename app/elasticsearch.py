"""
Async Elasticsearch client setup (FastAPI + Celery).

Design goals:
- Async-first: use AsyncElasticsearch.
- Safe with Celery concurrency: EACH worker process has its own client instance.
- Avoid event-loop issues: create the client lazily inside async functions.
- Keep FastAPI startup resilient: ES is optional by default (doesn't block startup).

Environment variables:
- ELASTICSEARCH_URL (default: http://localhost:9200)
- ELASTICSEARCH_USERNAME (optional)
- ELASTICSEARCH_PASSWORD (optional)
- ELASTICSEARCH_VERIFY_CERTS (default: false)
- ELASTICSEARCH_REQUIRED (default: false)  -> if true, FastAPI startup fails when ES can't be reached
"""

from __future__ import annotations

import os
import logging
from typing import Any

from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME")
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")
ELASTICSEARCH_VERIFY_CERTS = os.getenv("ELASTICSEARCH_VERIFY_CERTS", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}
ELASTICSEARCH_REQUIRED = os.getenv("ELASTICSEARCH_REQUIRED", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}

# Per-process client instance (NOT shared across Celery processes)
_es: AsyncElasticsearch | None = None


def _build_client() -> AsyncElasticsearch:
    """Create an AsyncElasticsearch client (no network I/O here)."""
    kwargs: dict[str, Any] = {
        "hosts": [ELASTICSEARCH_URL],
        "verify_certs": ELASTICSEARCH_VERIFY_CERTS,
        # Keep timeouts sane for local demos/tests.
        "request_timeout": 30,
        "retry_on_timeout": True,
        "max_retries": 3,
    }

    if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
        kwargs["basic_auth"] = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

    return AsyncElasticsearch(**kwargs)


async def init_es(*, required: bool | None = None) -> AsyncElasticsearch:
    """
    Initialize the async Elasticsearch client for this process.

    If required=False (default), failures to ping ES are logged and startup continues.
    If required=True, failures raise and should fail FastAPI startup.
    """
    global _es
    if _es is None:
        _es = _build_client()

    must_work = ELASTICSEARCH_REQUIRED if required is None else required

    try:
        # Ping validates connectivity & credentials.
        await _es.ping()
        logger.info(f"[Process {os.getpid()}] Elasticsearch reachable at {ELASTICSEARCH_URL}")
    except Exception as e:
        if must_work:
            raise
        logger.warning(
            f"[Process {os.getpid()}] Elasticsearch not reachable at {ELASTICSEARCH_URL}: {e}"
        )

    return _es


async def close_es():
    """Close the async Elasticsearch client for this process."""
    global _es
    if _es is not None:
        await _es.close()
        _es = None
        logger.info(f"[Process {os.getpid()}] Elasticsearch client closed")


def get_es() -> AsyncElasticsearch:
    """Get the client for this process (must have been initialized)."""
    if _es is None:
        raise RuntimeError("Elasticsearch client not initialized. Call ensure_es() first.")
    return _es


async def ensure_es() -> AsyncElasticsearch:
    """Idempotent initializer for tasks/endpoints; safe to call repeatedly."""
    return await init_es(required=False)

