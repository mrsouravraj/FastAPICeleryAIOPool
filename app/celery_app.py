"""
Celery application configuration with async support via celery-aio-pool.
"""
import os
from celery import Celery

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
    
    # Allow concurrent task execution
    worker_concurrency=4,
    
    # Disable task rate limits for testing
    worker_disable_rate_limits=True,
)
