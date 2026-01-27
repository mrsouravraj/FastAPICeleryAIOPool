#!/bin/bash
# Script to run Celery worker with celery-aio-pool

# Set the custom worker pool environment variable
export CELERY_CUSTOM_WORKER_POOL='celery_aio_pool.pool:AsyncIOPool'

# Run celery worker with the custom async pool
celery -A app.celery_app worker --pool=custom --loglevel=info --concurrency=4
