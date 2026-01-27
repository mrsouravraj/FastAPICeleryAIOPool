# FastAPI + Celery AIO Pool + MongoDB

A production-ready example demonstrating how to handle **both CPU-heavy and I/O-heavy tasks** efficiently using:
- **FastAPI** - Async REST API
- **Celery + celery-aio-pool** - Async background task execution
- **MongoDB + Beanie** - Async database operations
- **Redis** - Message broker and result backend

## Performance Results

| Test | Items | Time | Speedup |
|------|-------|------|---------|
| Concurrent HTTP | 25 URLs (2s each) | **1.24s** | **40x** |
| Bulk DB Insert | 2,000 users | **0.70s** | **28x** |
| CPU Compute | 100M iterations | 2.56s | - |
| HTTP + DB Store | 15 URLs + DB | **1.06s** | **21x** |
| Mixed Workload | CPU + DB + HTTP | 3.26s | All concurrent |

## Quick Start

### Prerequisites
- Python 3.10+
- MongoDB (running on localhost:27017)
- Redis (running on localhost:6379)

### Installation

```bash
# Clone and setup
git clone <repo>
cd FastAPICeleryAIOPool

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Start Services

```bash
# Terminal 1: Start Celery Worker
CELERY_CUSTOM_WORKER_POOL='celery_aio_pool.pool:AsyncIOPool' \
  celery -A app.celery_app worker --pool=custom --loglevel=info --concurrency=4

# Terminal 2: Start FastAPI
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Terminal 3: Run Tests
python test_comprehensive.py
```

### Or use the scripts

```bash
chmod +x run_worker.sh run_api.sh
./run_worker.sh  # Terminal 1
./run_api.sh     # Terminal 2
```

## Project Structure

```
FastAPICeleryAIOPool/
├── app/
│   ├── __init__.py
│   ├── celery_app.py      # Celery configuration
│   ├── database.py        # MongoDB/Beanie setup
│   ├── models.py          # Beanie document models
│   ├── main.py            # FastAPI application
│   └── tasks.py           # Async Celery tasks
├── requirements.txt
├── run_worker.sh
├── run_api.sh
├── test_comprehensive.py  # Full test suite (CPU + I/O)
├── test_mongodb.py        # MongoDB-focused tests
└── README.md
```

## Understanding celery-aio-pool

### Key Insight

**The async benefit is WITHIN a single task, not across multiple tasks.**

```
┌─────────────────────────────────────────────────────────────────┐
│ WRONG EXPECTATION:                                              │
│   Submit 10 separate tasks → All run concurrently               │
│                                                                 │
│ CORRECT UNDERSTANDING:                                          │
│   Submit 1 task that does 10 operations → Those 10 operations   │
│   run concurrently WITHIN that task using asyncio.gather()      │
└─────────────────────────────────────────────────────────────────┘
```

### When to Use

**Ideal for:**
- Tasks that make multiple HTTP API calls
- Tasks that query multiple databases/services
- ETL pipelines with concurrent data sources
- Bulk database operations

**Not ideal for:**
- Simple single-operation tasks
- Pure CPU-bound work (use prefork pool instead)

## Task Patterns

### 1. I/O-Bound: Concurrent HTTP Requests

```python
@celery_app.task(bind=True)
async def fetch_multiple_urls(self, urls: list[str]) -> dict:
    async with httpx.AsyncClient() as client:
        # All requests run IN PARALLEL
        tasks = [client.get(url) for url in urls]
        responses = await asyncio.gather(*tasks)
        return [r.json() for r in responses]
```

**Result:** 25 URLs (2s each) completed in 1.24s = **40x speedup**

### 2. I/O-Bound: Bulk Database Operations

```python
@celery_app.task(bind=True)
async def bulk_create_users(self, users_data: list[dict]) -> dict:
    await ensure_db()
    
    async def create_one(data):
        user = User(**data)
        await user.insert()
        return str(user.id)
    
    # All inserts run CONCURRENTLY
    results = await asyncio.gather(*[create_one(d) for d in users_data])
    return {"created": len(results)}
```

**Result:** 2,000 users created in 0.70s = **2,847 users/second**

### 3. CPU-Bound: Using ThreadPoolExecutor

```python
@celery_app.task(bind=True)
async def cpu_compute(self, n: int) -> dict:
    loop = asyncio.get_running_loop()
    
    # Runs in thread pool - doesn't block event loop
    result = await loop.run_in_executor(None, compute_sum_of_squares, n)
    return {"result": result}
```

**Why ThreadPoolExecutor?**
- Releases the event loop during CPU work
- Allows other async operations to proceed
- ProcessPoolExecutor doesn't work well in Celery workers

### 4. Hybrid: I/O + CPU Combined

```python
@celery_app.task(bind=True)
async def fetch_and_process(self, url: str, iterations: int) -> dict:
    # Step 1: I/O (async)
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
    
    # Step 2: CPU (in executor)
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, process_data, response.content)
    
    return {"content_length": len(response.content), "result": result}
```

### 5. Hybrid: HTTP + Database

```python
@celery_app.task(bind=True)
async def fetch_and_store(self, urls: list[str]) -> dict:
    await ensure_db()
    
    async def fetch_and_save(url):
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
        
        record = DataRecord(url=url, data=response.json())
        await record.insert()
        return str(record.id)
    
    # ALL fetch + store operations run concurrently
    results = await asyncio.gather(*[fetch_and_save(url) for url in urls])
    return {"stored": len(results)}
```

**Result:** 15 URLs fetched + stored in 1.06s = **21x speedup**

## API Endpoints

### Database Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/db/jobs/create` | POST | Create a job |
| `/db/jobs/process` | POST | Process a job |
| `/db/jobs/stats` | GET | Get job statistics |
| `/db/users/create` | POST | Create a user |
| `/db/users/bulk-create` | POST | Bulk create users |
| `/db/jobs` | GET | List jobs |
| `/db/users` | GET | List users |

### I/O Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/io/fetch` | POST | Fetch single URL |
| `/io/fetch-multiple` | POST | Fetch multiple URLs concurrently |
| `/io/sleep` | POST | Async sleep |

### CPU Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cpu/compute` | POST | Heavy computation |
| `/cpu/compress` | POST | Data compression |

### Hybrid Operations
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/hybrid/fetch-process` | POST | Fetch then process |
| `/hybrid/fetch-and-store` | POST | Fetch and store in DB |

### Task Management
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/tasks/{id}` | GET | Get task status/result |
| `/tasks/{id}` | DELETE | Cancel task |

## Beanie Models

```python
# app/models.py

class Job(Document):
    celery_task_id: Indexed(str, unique=True)
    job_type: Indexed(str)
    status: str
    input_data: dict
    result: Optional[JobResult]
    created_at: datetime
    completed_at: Optional[datetime]

class User(Document):
    email: Indexed(str, unique=True)
    username: Indexed(str, unique=True)
    full_name: str
    is_active: bool

class DataRecord(Document):
    source: Indexed(str)
    record_type: Indexed(str)
    data: dict
    is_processed: bool

class APICallLog(Document):
    task_id: Indexed(str)
    url: str
    status_code: int
    response_time_ms: float
```

## Configuration

### Celery (app/celery_app.py)

```python
celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    worker_prefetch_multiplier=10,  # Allow more task prefetching
    worker_concurrency=4,           # Number of worker processes
    task_time_limit=300,            # 5 minute timeout
)
```

### MongoDB (app/database.py)

```python
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
DATABASE_NAME = os.getenv("DATABASE_NAME", "celery_aio_demo")
```

## Common Patterns

### Pattern 1: Batch Processing

```python
async def process_batch(items: list, batch_size: int = 100):
    """Process items in batches to avoid memory issues."""
    results = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        batch_results = await asyncio.gather(*[process(item) for item in batch])
        results.extend(batch_results)
    return results
```

### Pattern 2: Retry with Backoff

```python
async def fetch_with_retry(url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient() as client:
                return await client.get(url)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
```

### Pattern 3: Semaphore for Rate Limiting

```python
semaphore = asyncio.Semaphore(10)  # Max 10 concurrent

async def rate_limited_fetch(url: str):
    async with semaphore:
        async with httpx.AsyncClient() as client:
            return await client.get(url)

# Use in gather
results = await asyncio.gather(*[rate_limited_fetch(url) for url in urls])
```

## Test Suite

```bash
# Comprehensive test (CPU + I/O + Hybrid)
python test_comprehensive.py

# MongoDB-focused tests
python test_mongodb.py

# Individual pattern tests
python test_correct_usage.py
```

## Troubleshooting

### Tasks stuck in PENDING
- Check if Celery worker is running
- Verify Redis connection: `redis-cli ping`

### MongoDB connection errors
- Ensure MongoDB is running: `mongosh --eval "db.runCommand({ping:1})"`
- Check connection string in environment

### ProcessPoolExecutor errors
- Don't use ProcessPoolExecutor in Celery workers
- Use ThreadPoolExecutor instead

### Event loop blocking
- Ensure CPU work uses `run_in_executor()`
- Add `await` to all async operations

## Best Practices Summary

| Task Type | Solution | Pattern |
|-----------|----------|---------|
| Multiple HTTP calls | `asyncio.gather()` | Concurrent I/O |
| Bulk DB operations | `asyncio.gather()` + Beanie | Concurrent I/O |
| CPU computation | `run_in_executor()` | Non-blocking CPU |
| Mixed I/O + CPU | Combine patterns | Hybrid |
| Rate limiting | `asyncio.Semaphore` | Controlled concurrency |

## License

MIT
