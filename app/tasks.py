"""
Celery tasks with MongoDB (Beanie) integration.

Demonstrates async database operations within Celery tasks.

Connection Architecture:
========================
Each Celery worker PROCESS has its own:
- Python interpreter (forked)
- asyncio event loop (managed by celery-aio-pool)  
- Motor client with connection pool
- ThreadPoolExecutor for CPU work

When you call ensure_db(), it initializes the database
connection for THIS process only. Other processes have
their own connections.
"""
import asyncio
import concurrent.futures
import time
import os
from datetime import datetime
from typing import Any

import httpx
from app.celery_app import celery_app
from app.database import ensure_db  # Import from database module


# =============================================================================
# THREAD EXECUTOR for CPU work (per-process)
# =============================================================================

# Each forked process has its own executor (not shared)
_thread_executor: concurrent.futures.ThreadPoolExecutor | None = None


def get_thread_executor(max_workers: int = 8) -> concurrent.futures.ThreadPoolExecutor:
    """
    Get or create a ThreadPoolExecutor for this process.
    
    Each worker process has its own executor.
    This is used to run CPU-bound work without blocking the event loop.
    """
    global _thread_executor
    if _thread_executor is None:
        pid = os.getpid()
        _thread_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"cpu-worker-{pid}"
        )
    return _thread_executor


# =============================================================================
# CPU HELPER FUNCTIONS
# =============================================================================

def _compute_sum_of_squares(n: int) -> int:
    total = 0
    for i in range(n):
        total += i * i
    return total


def _compress_data(text: str, repeat: int) -> dict:
    import zlib
    data = (text * repeat).encode('utf-8')
    original_size = len(data)
    compressed = zlib.compress(data, level=9)
    compressed_size = len(compressed)
    return {
        "original_size": original_size,
        "compressed_size": compressed_size,
        "ratio": f"{(1 - compressed_size/original_size)*100:.1f}%",
    }


# =============================================================================
# MONGODB I/O TASKS - Using Beanie
# =============================================================================

@celery_app.task(bind=True, name="tasks.db.create_job")
async def create_job(
    self,
    name: str,
    job_type: str,
    input_data: dict = None,
    description: str = None,
) -> dict:
    """
    Create a new job record in MongoDB.
    Demonstrates async document creation.
    """
    await ensure_db()
    from app.models import Job, JobStatus
    
    job = Job(
        celery_task_id=self.request.id,
        name=name,
        job_type=job_type,
        description=description,
        input_data=input_data or {},
        status=JobStatus.PENDING.value,
    )
    
    await job.insert()
    
    return {
        "task_id": self.request.id,
        "job_id": str(job.id),
        "name": job.name,
        "status": job.status,
        "created_at": job.created_at.isoformat(),
    }


@celery_app.task(bind=True, name="tasks.db.process_job")
async def process_job(self, job_id: str, simulate_duration: float = 2.0) -> dict:
    """
    Process a job - update status and simulate work.
    Demonstrates async document updates.
    """
    await ensure_db()
    from app.models import Job, JobStatus, JobResult
    from beanie import PydanticObjectId
    
    # Find the job
    job = await Job.get(PydanticObjectId(job_id))
    if not job:
        return {"error": f"Job {job_id} not found"}
    
    # Update to processing
    job.status = JobStatus.PROCESSING.value
    job.started_at = datetime.utcnow()
    await job.save()
    
    # Simulate processing
    await asyncio.sleep(simulate_duration)
    
    # Complete the job
    job.status = JobStatus.COMPLETED.value
    job.completed_at = datetime.utcnow()
    job.result = JobResult(
        data={"processed": True, "input": job.input_data},
        execution_time_ms=job.duration_ms,
    )
    await job.save()
    
    return {
        "task_id": self.request.id,
        "job_id": str(job.id),
        "status": job.status,
        "duration_ms": job.duration_ms,
    }


@celery_app.task(bind=True, name="tasks.db.create_user")
async def create_user(
    self,
    email: str,
    username: str,
    full_name: str,
    bio: str = None,
) -> dict:
    """
    Create a new user in MongoDB.
    """
    await ensure_db()
    from app.models import User
    
    # Check if user exists
    existing = await User.find_one(
        {"$or": [{"email": email}, {"username": username}]}
    )
    if existing:
        return {
            "error": "User with this email or username already exists",
            "existing_user_id": str(existing.id),
        }
    
    user = User(
        email=email,
        username=username,
        full_name=full_name,
        bio=bio,
    )
    await user.insert()
    
    return {
        "task_id": self.request.id,
        "user_id": str(user.id),
        "email": user.email,
        "username": user.username,
    }


@celery_app.task(bind=True, name="tasks.db.bulk_create_users")
async def bulk_create_users(self, users_data: list[dict]) -> dict:
    """
    Create multiple users concurrently.
    Demonstrates parallel async database operations.
    """
    await ensure_db()
    from app.models import User
    
    start = time.time()
    
    async def create_one(data: dict) -> dict:
        try:
            existing = await User.find_one(
                {"$or": [{"email": data["email"]}, {"username": data["username"]}]}
            )
            if existing:
                return {"email": data["email"], "status": "exists"}
            
            user = User(**data)
            await user.insert()
            return {"email": data["email"], "status": "created", "id": str(user.id)}
        except Exception as e:
            return {"email": data.get("email"), "status": "error", "error": str(e)}
    
    # Create all users concurrently
    tasks = [create_one(data) for data in users_data]
    results = await asyncio.gather(*tasks)
    
    elapsed = time.time() - start
    
    created = sum(1 for r in results if r.get("status") == "created")
    
    return {
        "task_id": self.request.id,
        "total": len(users_data),
        "created": created,
        "results": results,
        "elapsed_seconds": elapsed,
    }


@celery_app.task(bind=True, name="tasks.db.fetch_and_store")
async def fetch_and_store(self, urls: list[str], source: str) -> dict:
    """
    Fetch data from multiple URLs and store in MongoDB.
    Demonstrates hybrid I/O: HTTP + Database operations.
    """
    await ensure_db()
    from app.models import DataRecord, APICallLog
    
    start = time.time()
    
    async def fetch_and_save(url: str) -> dict:
        fetch_start = time.time()
        try:
            async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
                response = await client.get(url)
                fetch_time = (time.time() - fetch_start) * 1000
                
                # Log the API call
                log = APICallLog(
                    task_id=self.request.id,
                    url=url,
                    method="GET",
                    status_code=response.status_code,
                    response_time_ms=fetch_time,
                    response_size_bytes=len(response.content),
                )
                await log.insert()
                
                # Store the data record
                record = DataRecord(
                    source=source,
                    record_type="http_response",
                    data={
                        "url": url,
                        "status_code": response.status_code,
                        "content_length": len(response.content),
                        "headers": dict(response.headers),
                    },
                    is_processed=True,
                    processed_at=datetime.utcnow(),
                    processing_task_id=self.request.id,
                )
                await record.insert()
                
                return {
                    "url": url,
                    "status": "success",
                    "record_id": str(record.id),
                    "fetch_time_ms": fetch_time,
                }
        except Exception as e:
            return {"url": url, "status": "error", "error": str(e)}
    
    # Fetch all URLs and store concurrently
    tasks = [fetch_and_save(url) for url in urls]
    results = await asyncio.gather(*tasks)
    
    elapsed = time.time() - start
    success_count = sum(1 for r in results if r.get("status") == "success")
    
    return {
        "task_id": self.request.id,
        "source": source,
        "total_urls": len(urls),
        "successful": success_count,
        "results": results,
        "elapsed_seconds": elapsed,
    }


@celery_app.task(bind=True, name="tasks.db.get_job_stats")
async def get_job_stats(self) -> dict:
    """
    Get statistics about jobs in the database.
    Demonstrates async aggregation queries.
    """
    await ensure_db()
    from app.models import Job, JobStatus
    
    # Count jobs by status
    total = await Job.count()
    pending = await Job.find(Job.status == JobStatus.PENDING.value).count()
    processing = await Job.find(Job.status == JobStatus.PROCESSING.value).count()
    completed = await Job.find(Job.status == JobStatus.COMPLETED.value).count()
    failed = await Job.find(Job.status == JobStatus.FAILED.value).count()
    
    # Get recent jobs
    recent_jobs = await Job.find_all().sort(-Job.created_at).limit(5).to_list()
    
    return {
        "task_id": self.request.id,
        "stats": {
            "total": total,
            "pending": pending,
            "processing": processing,
            "completed": completed,
            "failed": failed,
        },
        "recent_jobs": [
            {
                "id": str(j.id),
                "name": j.name,
                "status": j.status,
                "created_at": j.created_at.isoformat(),
            }
            for j in recent_jobs
        ],
    }


@celery_app.task(bind=True, name="tasks.db.cleanup_old_records")
async def cleanup_old_records(self, days_old: int = 7) -> dict:
    """
    Clean up old records from the database.
    Demonstrates bulk delete operations.
    """
    await ensure_db()
    from app.models import APICallLog, DataRecord
    from datetime import timedelta
    
    cutoff = datetime.utcnow() - timedelta(days=days_old)
    
    # Delete old API call logs
    logs_result = await APICallLog.find(
        APICallLog.timestamp < cutoff
    ).delete()
    
    # Delete old data records
    records_result = await DataRecord.find(
        DataRecord.fetched_at < cutoff
    ).delete()
    
    return {
        "task_id": self.request.id,
        "cutoff_date": cutoff.isoformat(),
        "deleted": {
            "api_call_logs": logs_result.deleted_count if logs_result else 0,
            "data_records": records_result.deleted_count if records_result else 0,
        },
    }


# =============================================================================
# ORIGINAL I/O TASKS
# =============================================================================

@celery_app.task(bind=True, name="tasks.io.fetch_url")
async def fetch_url(self, url: str) -> dict:
    """Fetch content from a URL."""
    start = time.time()
    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        response = await client.get(url)
        elapsed = time.time() - start
        return {
            "task_id": self.request.id,
            "url": url,
            "status_code": response.status_code,
            "content_length": len(response.content),
            "elapsed_seconds": elapsed,
        }


@celery_app.task(bind=True, name="tasks.io.fetch_multiple")
async def fetch_multiple_urls(self, urls: list[str]) -> dict:
    """Fetch multiple URLs concurrently."""
    start = time.time()
    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        tasks = [client.get(url) for url in urls]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        results = []
        for url, resp in zip(urls, responses):
            if isinstance(resp, Exception):
                results.append({"url": url, "error": str(resp)})
            else:
                results.append({
                    "url": url,
                    "status_code": resp.status_code,
                    "content_length": len(resp.content),
                })
        
        elapsed = time.time() - start
        return {
            "task_id": self.request.id,
            "total_urls": len(urls),
            "results": results,
            "elapsed_seconds": elapsed,
        }


@celery_app.task(bind=True, name="tasks.io.async_sleep")
async def async_sleep_task(self, seconds: float) -> dict:
    """Async sleep."""
    start = time.time()
    await asyncio.sleep(seconds)
    elapsed = time.time() - start
    return {
        "task_id": self.request.id,
        "message": f"Slept for {seconds} seconds",
        "seconds": seconds,
        "elapsed_seconds": elapsed,
    }


# =============================================================================
# CPU TASKS
# =============================================================================

@celery_app.task(bind=True, name="tasks.cpu.compute")
async def cpu_compute(self, n: int) -> dict:
    """CPU computation using ThreadPoolExecutor."""
    start = time.time()
    loop = asyncio.get_running_loop()
    executor = get_thread_executor()
    
    result = await loop.run_in_executor(executor, _compute_sum_of_squares, n)
    
    elapsed = time.time() - start
    return {
        "task_id": self.request.id,
        "n": n,
        "sum_of_squares": result,
        "elapsed_seconds": elapsed,
        "execution": "ThreadPoolExecutor",
    }


@celery_app.task(bind=True, name="tasks.cpu.compress")
async def compress_data(self, text: str, repeat: int = 1000) -> dict:
    """Compress data using ThreadPoolExecutor."""
    start = time.time()
    loop = asyncio.get_running_loop()
    executor = get_thread_executor()
    
    result = await loop.run_in_executor(executor, _compress_data, text, repeat)
    
    elapsed = time.time() - start
    return {
        "task_id": self.request.id,
        **result,
        "elapsed_seconds": elapsed,
    }


# =============================================================================
# HYBRID TASKS
# =============================================================================

@celery_app.task(bind=True, name="tasks.hybrid.fetch_and_process")
async def fetch_and_process(self, url: str, process_iterations: int = 1000000) -> dict:
    """Fetch data then process it."""
    start = time.time()
    
    # I/O: fetch
    io_start = time.time()
    async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
        response = await client.get(url)
        content = response.content
    io_time = time.time() - io_start
    
    # CPU: process
    cpu_start = time.time()
    loop = asyncio.get_running_loop()
    executor = get_thread_executor()
    compute_n = len(content) + process_iterations
    process_result = await loop.run_in_executor(
        executor, _compute_sum_of_squares, compute_n
    )
    cpu_time = time.time() - cpu_start
    
    return {
        "task_id": self.request.id,
        "url": url,
        "content_length": len(content),
        "process_result": process_result,
        "timing": {
            "io_seconds": io_time,
            "cpu_seconds": cpu_time,
            "total_seconds": time.time() - start,
        },
    }


@celery_app.task(bind=True, name="tasks.sync.message")
def sync_task(self, message: str) -> dict:
    """Synchronous task."""
    time.sleep(1)
    return {
        "task_id": self.request.id,
        "message": f"Processed: {message}",
    }
