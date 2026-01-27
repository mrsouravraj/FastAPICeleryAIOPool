"""
Celery application configuration with async support via celery-aio-pool.

Connection Architecture:
========================
With concurrency=4, Celery creates 4 SEPARATE worker processes.
Each process has its OWN MongoDB connection (not shared).

┌─────────────────────────────────────────────────────────────────────┐
│                         CELERY MAIN PROCESS                         │
│                      (manages worker processes)                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
         ┌──────────────┬───────────┴───────────┬──────────────┐
         ▼              ▼                       ▼              ▼
┌─────────────┐  ┌─────────────┐        ┌─────────────┐  ┌─────────────┐
│  Worker 1   │  │  Worker 2   │        │  Worker 3   │  │  Worker 4   │
│  (Process)  │  │  (Process)  │        │  (Process)  │  │  (Process)  │
├─────────────┤  ├─────────────┤        ├─────────────┤  ├─────────────┤
│ Event Loop  │  │ Event Loop  │        │ Event Loop  │  │ Event Loop  │
│ MongoDB Conn│  │ MongoDB Conn│        │ MongoDB Conn│  │ MongoDB Conn│
│ (own pool)  │  │ (own pool)  │        │ (own pool)  │  │ (own pool)  │
└─────────────┘  └─────────────┘        └─────────────┘  └─────────────┘

Each worker process:
- Has its own Python interpreter (forked)
- Has its own asyncio event loop (managed by celery-aio-pool)
- Has its own Motor client with connection pool
- Connections are NOT shared across processes (OS limitation)

Motor's connection pool (within each process):
- Default maxPoolSize=100 connections per process
- Connections are reused for multiple async operations
- asyncio.gather() reuses connections from the pool
"""
import os
import asyncio
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create Celery app
celery_app = Celery(
    "worker",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["app.tasks"],
)

# Celery configuration optimized for async workloads
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes max
    result_expires=3600,  # Results expire after 1 hour
    
    # Important for async concurrency:
    # Higher prefetch allows more tasks to be picked up at once
    worker_prefetch_multiplier=10,
    
    # Number of worker PROCESSES (each has own DB connection)
    worker_concurrency=4,
    
    # Disable task rate limits for testing
    worker_disable_rate_limits=True,
)


# =============================================================================
# WORKER LIFECYCLE SIGNALS
# =============================================================================

@worker_process_init.connect
def on_worker_process_init(**kwargs):
    """
    Called when each worker PROCESS starts.
    
    This runs ONCE per process, before any tasks execute.
    Each of the 4 worker processes will call this independently.
    
    Note: celery-aio-pool manages its own event loop, so we need to
    initialize the database lazily on the first async task.
    We set a flag here to indicate initialization is needed.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Get the process ID to show each worker is separate
    pid = os.getpid()
    logger.info(f"Worker process {pid} starting - DB will be initialized on first task")
    
    # We can't run async code here directly because celery-aio-pool
    # hasn't set up its event loop yet. Instead, we'll use the
    # lazy initialization in database.py which will run on first task.
    
    # Set environment variable so database.py knows to log connection info
    os.environ["CELERY_WORKER_PID"] = str(pid)


@worker_process_shutdown.connect  
def on_worker_process_shutdown(**kwargs):
    """
    Called when each worker PROCESS shuts down.
    Clean up database connections.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    pid = os.getpid()
    logger.info(f"Worker process {pid} shutting down - closing DB connection")
    
    # The database module handles cleanup
    try:
        from app.database import close_db_sync
        close_db_sync()
    except Exception as e:
        logger.warning(f"Error closing DB in process {pid}: {e}")
