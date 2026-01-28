"""
Async Elasticsearch client setup (FastAPI + Celery).

Connection Architecture (same pattern as MongoDB):
===================================================

Each Celery worker PROCESS has its OWN Elasticsearch client.
Clients are NOT shared across processes.

┌─────────────────────────────────────────────────────────────────────────┐
│                         ELASTICSEARCH CLUSTER                            │
│                    (accepts many HTTP connections)                       │
├─────────────────────────────────────────────────────────────────────────┤
│  Connection Pool 1   Connection Pool 2   Pool 3         Pool 4         │
│  (from Worker 1)     (from Worker 2)     (Worker 3)     (Worker 4)     │
└─────────────────────────────────────────────────────────────────────────┘
         ▲                   ▲                 ▲              ▲
         │                   │                 │              │
    ┌────┴────┐         ┌────┴────┐      ┌────┴────┐    ┌────┴────┐
    │Worker 1 │         │Worker 2 │      │Worker 3 │    │Worker 4 │
    │PID:1001 │         │PID:1002 │      │PID:1003 │    │PID:1004 │
    ├─────────┤         ├─────────┤      ├─────────┤    ├─────────┤
    │ _es     │         │ _es     │      │ _es     │    │ _es     │
    │(Async   │         │(Async   │      │(Async   │    │(Async   │
    │ Client) │         │ Client) │      │ Client) │    │ Client) │
    │         │         │         │      │         │    │         │
    │Event    │         │Event    │      │Event    │    │Event    │
    │Loop     │         │Loop     │      │Loop     │    │Loop     │
    └─────────┘         └─────────┘      └─────────┘    └─────────┘

Key Points:
1. Each forked worker process has its OWN memory space
2. The `_es` variable is SEPARATE in each process (not shared)
3. AsyncElasticsearch uses aiohttp internally with connection pooling
4. Lazy initialization: client created on first `ensure_es()` call
5. Event loop safe: client created inside async context

Why Lazy Initialization?
------------------------
celery-aio-pool creates the event loop AFTER the worker process starts.
If we created the client at import time or in worker_process_init signal,
it would be bound to a different (or no) event loop, causing errors like:
  - "There is no current event loop"
  - "Event loop is closed"
  - "attached to a different loop"

Solution: Create client lazily inside async functions (ensure_es).

Connection Pool (within each process):
--------------------------------------
AsyncElasticsearch uses aiohttp.ClientSession which maintains:
- Keep-alive connections to ES nodes
- Connection pooling (reuses connections for multiple requests)
- Automatic retry on failures

When you do:
    await asyncio.gather(*[es.index(...) for doc in 1000_docs])

Each index() call borrows a connection from the pool, performs the
HTTP request, then returns it. This allows many concurrent ES operations.

Environment variables:
- ELASTICSEARCH_URL (default: http://localhost:9200)
- ELASTICSEARCH_USERNAME (optional)
- ELASTICSEARCH_PASSWORD (optional)
- ELASTICSEARCH_VERIFY_CERTS (default: false)
- ELASTICSEARCH_REQUIRED (default: false)
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

# =============================================================================
# PER-PROCESS CLIENT INSTANCE
# =============================================================================
# Each forked worker process has its OWN _es (NOT shared across processes!)
# This is the same pattern as MongoDB (_client in database.py)

_es: AsyncElasticsearch | None = None
_initialized: bool = False


def _build_client() -> AsyncElasticsearch:
    """
    Create an AsyncElasticsearch client (no network I/O here).
    
    The client uses aiohttp internally, which provides:
    - Connection pooling
    - Keep-alive connections
    - Automatic retries
    """
    kwargs: dict[str, Any] = {
        "hosts": [ELASTICSEARCH_URL],
        "verify_certs": ELASTICSEARCH_VERIFY_CERTS,
        # Timeout settings
        "request_timeout": 60,  # Increased for bulk operations
        "retry_on_timeout": True,
        "max_retries": 3,
    }

    if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
        kwargs["basic_auth"] = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)

    return AsyncElasticsearch(**kwargs)


async def init_es(*, required: bool | None = None) -> AsyncElasticsearch:
    """
    Initialize the async Elasticsearch client for this process.

    This is called:
    - On FastAPI startup (required=False, so won't block if ES is down)
    - On first Celery task that needs ES (via ensure_es())

    If required=False (default), failures to ping ES are logged and startup continues.
    If required=True, failures raise and should fail FastAPI startup.
    """
    global _es, _initialized
    
    if _es is None:
        _es = _build_client()

    must_work = ELASTICSEARCH_REQUIRED if required is None else required
    pid = os.getpid()

    try:
        # Ping validates connectivity & credentials.
        ok = await _es.ping()
        if ok:
            _initialized = True
            logger.info(f"[Process {pid}] Elasticsearch connected: {ELASTICSEARCH_URL}")
        else:
            raise ConnectionError("Ping returned False")
    except Exception as e:
        if must_work:
            raise
        logger.warning(
            f"[Process {pid}] Elasticsearch not reachable at {ELASTICSEARCH_URL}: {e}"
        )

    return _es


async def close_es():
    """Close the async Elasticsearch client for this process."""
    global _es, _initialized
    if _es is not None:
        pid = os.getpid()
        await _es.close()
        _es = None
        _initialized = False
        logger.info(f"[Process {pid}] Elasticsearch client closed")


def get_es() -> AsyncElasticsearch:
    """Get the client for this process (must have been initialized)."""
    if _es is None:
        raise RuntimeError(
            "Elasticsearch client not initialized. "
            "Call ensure_es() first (in Celery tasks) or init_es() (in FastAPI)."
        )
    return _es


async def ensure_es() -> AsyncElasticsearch:
    """
    Ensure Elasticsearch client is initialized for the current worker process.
    
    This is the recommended way to get ES access in Celery tasks.
    It's idempotent - safe to call multiple times.
    
    Usage in tasks:
        @celery_app.task(bind=True)
        async def my_task(self):
            es = await ensure_es()
            await es.index(index="foo", body={"bar": 1})
    """
    global _initialized
    
    if not _initialized or _es is None:
        await init_es(required=False)
    
    return _es
