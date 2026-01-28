"""
Elasticsearch STRESS TEST via Celery (1 million documents).

This test demonstrates:
1. Bulk indexing 1,000,000 documents via Celery tasks
2. How multiple Celery workers process batches concurrently
3. Connection pool efficiency within each worker

Architecture:
=============
┌────────────────────────────────────────────────────────────────────────┐
│                           TEST SCRIPT                                   │
│        Submits 100 bulk-index tasks (10,000 docs each)                 │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                     Submit via HTTP to FastAPI
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         FASTAPI SERVER                                  │
│                    Enqueues tasks to Redis                              │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                          Redis Queue
                                    ▼
┌─────────────┬─────────────┬─────────────┬─────────────┐
│  Worker 1   │  Worker 2   │  Worker 3   │  Worker 4   │
│  (Process)  │  (Process)  │  (Process)  │  (Process)  │
├─────────────┼─────────────┼─────────────┼─────────────┤
│ _es client  │ _es client  │ _es client  │ _es client  │
│ (own conn)  │ (own conn)  │ (own conn)  │ (own conn)  │
└─────────────┴─────────────┴─────────────┴─────────────┘
                                    │
                        Each worker indexes
                        its batch to ES
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                       ELASTICSEARCH CLUSTER                             │
│                     Receives ~25 batches per worker                     │
└────────────────────────────────────────────────────────────────────────┘

Requires:
- FastAPI running on http://localhost:8000
- Celery worker running with celery-aio-pool (--concurrency=4 recommended)
- Elasticsearch running on http://localhost:9200

Run:
  python test_elasticsearch_stress.py
"""

from __future__ import annotations

import time
import uuid
import concurrent.futures
from typing import Any

import httpx


API = "http://localhost:8000"
TOTAL_DOCS = 1_000_000  # 1 million documents
BATCH_SIZE = 10_000     # 10K docs per Celery task
NUM_BATCHES = TOTAL_DOCS // BATCH_SIZE


def wait_for_task(task_id: str, timeout: float = 300.0) -> dict:
    """Wait for a Celery task to complete."""
    start = time.time()
    while True:
        try:
            r = httpx.get(f"{API}/tasks/{task_id}", timeout=10.0)
            r.raise_for_status()
            data = r.json()
            if data["status"] in ("SUCCESS", "FAILURE", "REVOKED"):
                return data
        except Exception as e:
            print(f"  Error polling task {task_id[:8]}: {e}")
        
        if time.time() - start > timeout:
            return {"task_id": task_id, "status": "TIMEOUT"}
        time.sleep(2.0)


def submit_bulk_task(index: str, batch_num: int) -> str:
    """Submit a bulk index task and return task_id."""
    docs = [
        {
            "batch": batch_num,
            "doc_num": i,
            "message": f"Document {batch_num * BATCH_SIZE + i}",
            "timestamp": time.time(),
            "group": "stress_test",
        }
        for i in range(BATCH_SIZE)
    ]
    
    r = httpx.post(
        f"{API}/es/bulk-index",
        json={"index": index, "documents": docs},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()["task_id"]


def main():
    print("=" * 70)
    print("ELASTICSEARCH STRESS TEST - 1 MILLION DOCUMENTS VIA CELERY")
    print("=" * 70)
    print(f"""
Configuration:
  Total documents:     {TOTAL_DOCS:,}
  Batch size:          {BATCH_SIZE:,}
  Number of batches:   {NUM_BATCHES}
  Each batch = 1 Celery task
""")

    # Health checks
    print("Checking services...")
    r = httpx.get(f"{API}/health", timeout=10.0)
    r.raise_for_status()
    print("  ✓ FastAPI is healthy")

    r = httpx.get(f"{API}/es/health", timeout=10.0)
    if r.status_code != 200 or not r.json().get("ok"):
        print("  ✗ Elasticsearch not reachable!")
        print(r.text)
        return
    print("  ✓ Elasticsearch is reachable:", r.json()["url"])

    # Create unique index for this test
    index = f"stress_test_{uuid.uuid4().hex[:8]}"
    print(f"\nIndex: {index}")

    # ==========================================================================
    # PHASE 1: Submit all bulk tasks concurrently
    # ==========================================================================
    print(f"\n{'='*70}")
    print("PHASE 1: Submitting {0} bulk-index tasks...".format(NUM_BATCHES))
    print("=" * 70)

    submit_start = time.time()
    task_ids: list[str] = []

    # Submit tasks using thread pool for concurrent HTTP requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(submit_bulk_task, index, batch_num)
            for batch_num in range(NUM_BATCHES)
        ]
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            task_id = future.result()
            task_ids.append(task_id)
            if (i + 1) % 10 == 0:
                print(f"  Submitted {i + 1}/{NUM_BATCHES} tasks...")

    submit_time = time.time() - submit_start
    print(f"\n  ✓ All {NUM_BATCHES} tasks submitted in {submit_time:.2f}s")
    print(f"  Submit rate: {NUM_BATCHES / submit_time:.1f} tasks/second")

    # ==========================================================================
    # PHASE 2: Wait for all tasks to complete
    # ==========================================================================
    print(f"\n{'='*70}")
    print("PHASE 2: Waiting for all tasks to complete...")
    print("=" * 70)
    print("  (Celery workers are processing batches concurrently)")

    wait_start = time.time()
    results: list[dict] = []
    completed = 0
    failed = 0

    # Poll tasks using thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(wait_for_task, task_id, 600.0): task_id
            for task_id in task_ids
        }
        
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            
            if result["status"] == "SUCCESS":
                completed += 1
            else:
                failed += 1
            
            total_done = completed + failed
            if total_done % 10 == 0:
                elapsed = time.time() - wait_start
                print(f"  Progress: {total_done}/{NUM_BATCHES} "
                      f"(✓{completed} ✗{failed}) - {elapsed:.1f}s elapsed")

    total_time = time.time() - wait_start

    # ==========================================================================
    # RESULTS
    # ==========================================================================
    print(f"\n{'='*70}")
    print("RESULTS")
    print("=" * 70)

    # Count indexed documents
    total_indexed = 0
    for r in results:
        if r["status"] == "SUCCESS" and r.get("result"):
            total_indexed += r["result"].get("indexed", 0)

    print(f"""
  Documents requested:   {TOTAL_DOCS:,}
  Documents indexed:     {total_indexed:,}
  
  Tasks submitted:       {NUM_BATCHES}
  Tasks succeeded:       {completed}
  Tasks failed:          {failed}
  
  Submit time:           {submit_time:.2f}s
  Processing time:       {total_time:.2f}s
  Total time:            {submit_time + total_time:.2f}s
  
  Throughput:            {total_indexed / total_time:,.0f} docs/second
  
  Index name:            {index}
""")

    # Verify with a search
    print("Verifying with search...")
    r = httpx.post(
        f"{API}/es/search",
        json={"index": index, "query": {"match": {"group": "stress_test"}}, "size": 5},
        timeout=30.0,
    )
    r.raise_for_status()
    task_id = r.json()["task_id"]
    search_result = wait_for_task(task_id, 60.0)
    
    if search_result["status"] == "SUCCESS":
        hits = search_result.get("result", {}).get("hits", [])
        print(f"  ✓ Search returned {len(hits)} sample hits")
        for h in hits[:3]:
            print(f"    - {h.get('source', {}).get('message', 'N/A')}")
    else:
        print(f"  ✗ Search failed: {search_result}")

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
