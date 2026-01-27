"""
MongoDB database configuration using Motor and Beanie.

Connection Architecture Explained:
==================================

Q: How does one Beanie connection work with 4 concurrent Celery processes?
A: It DOESN'T - each process has its OWN connection!

┌─────────────────────────────────────────────────────────────────────────┐
│                              MONGODB SERVER                              │
│                        (accepts many connections)                        │
├─────────────────────────────────────────────────────────────────────────┤
│  Connection 1    Connection 2    Connection 3    Connection 4           │
│  (from Worker1)  (from Worker2)  (from Worker3)  (from Worker4)         │
└─────────────────────────────────────────────────────────────────────────┘
         ▲              ▲               ▲               ▲
         │              │               │               │
    ┌────┴────┐    ┌────┴────┐    ┌────┴────┐    ┌────┴────┐
    │ Worker1 │    │ Worker2 │    │ Worker3 │    │ Worker4 │
    │ PID:101 │    │ PID:102 │    │ PID:103 │    │ PID:104 │
    │         │    │         │    │         │    │         │
    │ _client │    │ _client │    │ _client │    │ _client │
    │ (Motor) │    │ (Motor) │    │ (Motor) │    │ (Motor) │
    └─────────┘    └─────────┘    └─────────┘    └─────────┘

Key Points:
1. Each worker PROCESS is forked - gets its own memory space
2. The `_client` variable is separate in each process
3. Motor client has a connection POOL (default 100 connections)
4. asyncio.gather() in a task uses connections from THAT process's pool
5. Connections are NOT shared across processes (OS/Python limitation)

Why This Works Well:
- MongoDB handles many concurrent connections efficiently
- Motor's pool reuses connections within each process
- 4 processes × 100 pool = 400 potential connections (way more than needed)
- Each process handles one Celery task at a time (but with async concurrency WITHIN)
"""
import os
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from beanie import init_beanie

from app.models import ALL_MODELS

logger = logging.getLogger(__name__)

# MongoDB configuration
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "celery_aio_demo")

# Per-process client instance
# Each forked worker process has its OWN _client (not shared!)
_client: AsyncIOMotorClient | None = None
_initialized: bool = False


async def init_db() -> AsyncIOMotorClient:
    """
    Initialize the database connection and Beanie ODM.
    
    This creates a Motor client with a connection pool.
    The pool allows multiple concurrent async operations
    to share connections efficiently.
    """
    global _client, _initialized
    
    if _initialized and _client is not None:
        return _client
    
    pid = os.getpid()
    worker_pid = os.environ.get("CELERY_WORKER_PID", "unknown")
    
    # Create Motor client with connection pool settings
    _client = AsyncIOMotorClient(
        MONGODB_URL,
        # Connection pool settings (per process)
        maxPoolSize=100,      # Max connections in pool
        minPoolSize=10,       # Keep at least 10 ready
        maxIdleTimeMS=30000,  # Close idle connections after 30s
        
        # Timeout settings
        connectTimeoutMS=5000,
        serverSelectionTimeoutMS=5000,
    )
    
    # Initialize Beanie with document models
    await init_beanie(
        database=_client[DATABASE_NAME],
        document_models=ALL_MODELS,
    )
    
    _initialized = True
    
    logger.info(
        f"[Process {pid}] MongoDB connected: {MONGODB_URL}/{DATABASE_NAME} "
        f"(pool: min=10, max=100)"
    )
    
    return _client


async def close_db():
    """
    Close the database connection.
    Called on application/worker shutdown.
    """
    global _client, _initialized
    
    if _client:
        pid = os.getpid()
        _client.close()
        _client = None
        _initialized = False
        logger.info(f"[Process {pid}] MongoDB connection closed")


def close_db_sync():
    """
    Synchronous version of close_db for signal handlers.
    """
    global _client, _initialized
    
    if _client:
        pid = os.getpid()
        _client.close()
        _client = None
        _initialized = False
        logger.info(f"[Process {pid}] MongoDB connection closed (sync)")


def get_client() -> AsyncIOMotorClient:
    """Get the MongoDB client instance for this process."""
    if _client is None:
        raise RuntimeError(
            "Database not initialized. Call init_db() first. "
            "In Celery tasks, use ensure_db() from tasks.py"
        )
    return _client


async def ensure_db():
    """
    Ensure database is initialized for the current worker process.
    
    This is the recommended way to get DB access in Celery tasks.
    It's idempotent - safe to call multiple times.
    
    Usage in tasks:
        @celery_app.task(bind=True)
        async def my_task(self):
            await ensure_db()
            # Now you can use Beanie models
            user = await User.find_one(...)
    """
    global _initialized
    
    if not _initialized:
        await init_db()


# =============================================================================
# CONNECTION POOL EXPLAINED
# =============================================================================
"""
Motor's Connection Pool (within each process):

┌──────────────────────────────────────────────────────────────────┐
│                    WORKER PROCESS (e.g., PID 101)                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │                   Motor Client (_client)                 │   │
│   │                                                          │   │
│   │   ┌─────────────────────────────────────────────────┐   │   │
│   │   │              CONNECTION POOL                     │   │   │
│   │   │  ┌────┐ ┌────┐ ┌────┐ ┌────┐ ┌────┐            │   │   │
│   │   │  │Conn│ │Conn│ │Conn│ │Conn│ │Conn│  ...       │   │   │
│   │   │  │ 1  │ │ 2  │ │ 3  │ │ 4  │ │ 5  │  (up to    │   │   │
│   │   │  └────┘ └────┘ └────┘ └────┘ └────┘   100)     │   │   │
│   │   └─────────────────────────────────────────────────┘   │   │
│   └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│   When you do:                                                   │
│     await asyncio.gather(*[doc.insert() for doc in docs])       │
│                                                                  │
│   Each insert() borrows a connection from the pool,              │
│   does the DB operation, then returns it to the pool.            │
│   This allows 100 concurrent DB operations with 100 connections. │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
"""
