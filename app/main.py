"""
FastAPI application with MongoDB (Beanie) and Celery integration.
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
from celery.result import AsyncResult

from app.celery_app import celery_app
from app.database import init_db, close_db
from app.elasticsearch import init_es, close_es


# =============================================================================
# Application Lifespan - Initialize/Close Database
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup, close on shutdown."""
    await init_db()
    # Elasticsearch is optional by default (won't block startup if not running).
    await init_es(required=False)
    yield
    await close_es()
    await close_db()


app = FastAPI(
    title="FastAPI + Celery AIO Pool + MongoDB + Elasticsearch",
    description="Async Celery tasks with MongoDB (Beanie) + Elasticsearch (async client)",
    version="3.0.0",
    lifespan=lifespan,
)


# =============================================================================
# Request Models
# =============================================================================

class CreateJobRequest(BaseModel):
    name: str
    job_type: str = "general"
    description: str | None = None
    input_data: dict = {}


class ProcessJobRequest(BaseModel):
    job_id: str
    simulate_duration: float = 2.0


class CreateUserRequest(BaseModel):
    email: EmailStr
    username: str
    full_name: str
    bio: str | None = None


class BulkCreateUsersRequest(BaseModel):
    users: list[CreateUserRequest]


class FetchAndStoreRequest(BaseModel):
    urls: list[str] = ["https://httpbin.org/get", "https://httpbin.org/ip"]
    source: str = "api_fetch"


class URLRequest(BaseModel):
    url: str = "https://httpbin.org/get"


class MultiURLRequest(BaseModel):
    urls: list[str] = ["https://httpbin.org/get", "https://httpbin.org/ip"]


class SleepRequest(BaseModel):
    seconds: float = 1.0


class ComputeRequest(BaseModel):
    n: int = 10_000_000


class HybridRequest(BaseModel):
    url: str = "https://httpbin.org/get"
    process_iterations: int = 1_000_000


class ElasticsearchIndexRequest(BaseModel):
    index: str = "demo_docs"
    doc_id: str | None = None
    document: dict


class ElasticsearchBulkIndexRequest(BaseModel):
    index: str = "demo_docs"
    documents: list[dict]


class ElasticsearchSearchRequest(BaseModel):
    index: str = "demo_docs"
    query: dict
    size: int = 10


# =============================================================================
# Response Models
# =============================================================================

class TaskResponse(BaseModel):
    task_id: str
    status: str
    task_type: str


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict | None = None
    error: str | None = None


# =============================================================================
# Health & Info
# =============================================================================

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "fastapi-celery-mongodb"}


@app.get("/info")
async def info():
    return {
        "version": "3.0.0",
        "features": {
            "mongodb": "Beanie ODM for async MongoDB operations",
            "elasticsearch": "AsyncElasticsearch client for async search/indexing",
            "celery": "celery-aio-pool for async task execution",
            "patterns": [
                "Concurrent HTTP requests within tasks",
                "Concurrent database operations within tasks",
                "Hybrid I/O + CPU tasks",
            ],
        },
        "endpoints": {
            "database": ["/db/jobs/create", "/db/jobs/process", "/db/users/create"],
            "io": ["/io/fetch", "/io/fetch-multiple", "/io/sleep"],
            "cpu": ["/cpu/compute", "/cpu/compress"],
            "hybrid": ["/hybrid/fetch-process", "/hybrid/fetch-and-store"],
            "elasticsearch": ["/es/health", "/es/index", "/es/bulk-index", "/es/search"],
        },
    }


# =============================================================================
# Database Task Endpoints (MongoDB)
# =============================================================================

@app.post("/db/jobs/create", response_model=TaskResponse)
async def create_job_endpoint(request: CreateJobRequest):
    """Create a new job in MongoDB."""
    from app.tasks import create_job
    task = create_job.delay(
        name=request.name,
        job_type=request.job_type,
        input_data=request.input_data,
        description=request.description,
    )
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


@app.post("/db/jobs/process", response_model=TaskResponse)
async def process_job_endpoint(request: ProcessJobRequest):
    """Process an existing job."""
    from app.tasks import process_job
    task = process_job.delay(
        job_id=request.job_id,
        simulate_duration=request.simulate_duration,
    )
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


@app.get("/db/jobs/stats", response_model=TaskResponse)
async def get_job_stats_endpoint():
    """Get job statistics from MongoDB."""
    from app.tasks import get_job_stats
    task = get_job_stats.delay()
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


@app.post("/db/users/create", response_model=TaskResponse)
async def create_user_endpoint(request: CreateUserRequest):
    """Create a new user in MongoDB."""
    from app.tasks import create_user
    task = create_user.delay(
        email=request.email,
        username=request.username,
        full_name=request.full_name,
        bio=request.bio,
    )
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


@app.post("/db/users/bulk-create", response_model=TaskResponse)
async def bulk_create_users_endpoint(request: BulkCreateUsersRequest):
    """
    Create multiple users concurrently.
    Demonstrates parallel database operations in one task.
    """
    from app.tasks import bulk_create_users
    users_data = [u.model_dump() for u in request.users]
    task = bulk_create_users.delay(users_data)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


@app.post("/db/cleanup", response_model=TaskResponse)
async def cleanup_endpoint(days_old: int = 7):
    """Clean up old records from MongoDB."""
    from app.tasks import cleanup_old_records
    task = cleanup_old_records.delay(days_old)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="db")


# =============================================================================
# Hybrid Endpoints (HTTP + Database)
# =============================================================================

@app.post("/hybrid/fetch-and-store", response_model=TaskResponse)
async def fetch_and_store_endpoint(request: FetchAndStoreRequest):
    """
    Fetch data from URLs and store in MongoDB.
    Demonstrates concurrent HTTP + concurrent DB operations.
    """
    from app.tasks import fetch_and_store
    task = fetch_and_store.delay(urls=request.urls, source=request.source)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="hybrid")


@app.post("/hybrid/fetch-process", response_model=TaskResponse)
async def fetch_and_process_endpoint(request: HybridRequest):
    """Fetch URL then process data."""
    from app.tasks import fetch_and_process
    task = fetch_and_process.delay(request.url, request.process_iterations)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="hybrid")


# =============================================================================
# I/O Task Endpoints
# =============================================================================

@app.post("/io/fetch", response_model=TaskResponse)
async def fetch_url_endpoint(request: URLRequest):
    """Fetch a URL asynchronously."""
    from app.tasks import fetch_url
    task = fetch_url.delay(request.url)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="io")


@app.post("/io/fetch-multiple", response_model=TaskResponse)
async def fetch_multiple_endpoint(request: MultiURLRequest):
    """Fetch multiple URLs concurrently."""
    from app.tasks import fetch_multiple_urls
    task = fetch_multiple_urls.delay(request.urls)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="io")


@app.post("/io/sleep", response_model=TaskResponse)
async def sleep_endpoint(request: SleepRequest):
    """Async sleep."""
    from app.tasks import async_sleep_task
    task = async_sleep_task.delay(request.seconds)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="io")


# =============================================================================
# CPU Task Endpoints
# =============================================================================

@app.post("/cpu/compute", response_model=TaskResponse)
async def compute_endpoint(request: ComputeRequest):
    """CPU computation in ThreadPoolExecutor."""
    from app.tasks import cpu_compute
    task = cpu_compute.delay(request.n)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="cpu")


@app.post("/cpu/compress", response_model=TaskResponse)
async def compress_endpoint(text: str = "Hello World! ", repeat: int = 10000):
    """Compress data in ThreadPoolExecutor."""
    from app.tasks import compress_data
    task = compress_data.delay(text, repeat)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="cpu")


# =============================================================================
# Task Management
# =============================================================================

@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str):
    """Get task status and result."""
    result = AsyncResult(task_id, app=celery_app)
    
    response = TaskStatusResponse(task_id=task_id, status=result.status)
    
    if result.ready():
        if result.successful():
            response.result = result.result
        else:
            response.error = str(result.result)
    
    return response


@app.delete("/tasks/{task_id}")
async def revoke_task(task_id: str):
    """Cancel a task."""
    celery_app.control.revoke(task_id, terminate=True)
    return {"task_id": task_id, "status": "REVOKED"}


# =============================================================================
# Elasticsearch (Async) - via Celery tasks
# =============================================================================

@app.get("/es/health")
async def elasticsearch_health():
    """Direct ES ping from the API process (not Celery)."""
    from app.elasticsearch import ensure_es, ELASTICSEARCH_URL

    try:
        es = await ensure_es()
        ok = await es.ping()
        return {"ok": bool(ok), "url": ELASTICSEARCH_URL}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Elasticsearch not reachable: {e}")


@app.post("/es/index", response_model=TaskResponse)
async def elasticsearch_index_document(request: ElasticsearchIndexRequest):
    """Index a single document via Celery task."""
    from app.tasks import es_index_document

    task = es_index_document.delay(request.index, request.document, request.doc_id)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="elasticsearch")


@app.post("/es/bulk-index", response_model=TaskResponse)
async def elasticsearch_bulk_index(request: ElasticsearchBulkIndexRequest):
    """Bulk index documents concurrently within one Celery task."""
    from app.tasks import es_bulk_index

    task = es_bulk_index.delay(request.index, request.documents)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="elasticsearch")


@app.post("/es/search", response_model=TaskResponse)
async def elasticsearch_search(request: ElasticsearchSearchRequest):
    """Search via Celery task."""
    from app.tasks import es_search

    task = es_search.delay(request.index, request.query, request.size)
    return TaskResponse(task_id=task.id, status="PENDING", task_type="elasticsearch")


# =============================================================================
# Direct Database Queries (for verification)
# =============================================================================

@app.get("/db/jobs")
async def list_jobs(limit: int = 10):
    """List recent jobs directly from MongoDB."""
    from app.models import Job
    jobs = await Job.find_all().sort(-Job.created_at).limit(limit).to_list()
    return {
        "count": len(jobs),
        "jobs": [
            {
                "id": str(j.id),
                "name": j.name,
                "job_type": j.job_type,
                "status": j.status,
                "created_at": j.created_at.isoformat(),
            }
            for j in jobs
        ],
    }


@app.get("/db/users")
async def list_users(limit: int = 10):
    """List users directly from MongoDB."""
    from app.models import User
    users = await User.find_all().sort(-User.created_at).limit(limit).to_list()
    return {
        "count": len(users),
        "users": [
            {
                "id": str(u.id),
                "email": u.email,
                "username": u.username,
                "full_name": u.full_name,
            }
            for u in users
        ],
    }


@app.get("/db/records")
async def list_records(limit: int = 10):
    """List data records directly from MongoDB."""
    from app.models import DataRecord
    records = await DataRecord.find_all().sort(-DataRecord.fetched_at).limit(limit).to_list()
    return {
        "count": len(records),
        "records": [
            {
                "id": str(r.id),
                "source": r.source,
                "record_type": r.record_type,
                "is_processed": r.is_processed,
                "fetched_at": r.fetched_at.isoformat(),
            }
            for r in records
        ],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
