#!/usr/bin/env python3
"""
CORRECT demonstration of celery-aio-pool benefits.

The power of celery-aio-pool is running concurrent async operations
WITHIN a single task, not running multiple Celery tasks concurrently.
"""
import time
import httpx

BASE_URL = "http://localhost:8000"


def wait_for_task(task_id: str, timeout: int = 120) -> dict:
    """Poll until task completes."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = httpx.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
            result = response.json()
            if result["status"] in ("SUCCESS", "FAILURE"):
                return result
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(1)
    return {"status": "TIMEOUT", "task_id": task_id}


def submit(endpoint: str, data: dict) -> dict:
    """Submit a task."""
    response = httpx.post(f"{BASE_URL}{endpoint}", json=data, timeout=10)
    return response.json()


def test_concurrent_within_task():
    """
    CORRECT USAGE: Multiple async operations within ONE task.
    
    This demonstrates the real power of celery-aio-pool.
    A single task fetches 5 URLs concurrently.
    """
    print("\n" + "=" * 70)
    print("TEST: Concurrent Operations WITHIN a Single Task")
    print("=" * 70)
    print("\nSubmitting ONE task that fetches 5 URLs concurrently...")
    print("(This is the correct way to use celery-aio-pool)")
    
    urls = [
        "https://httpbin.org/delay/2",  # Each takes 2 seconds
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
        "https://httpbin.org/delay/2",
    ]
    
    start = time.time()
    
    result = submit("/io/fetch-multiple", {"urls": urls})
    print(f"  Task ID: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=30)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        print(f"\n  Status: SUCCESS")
        print(f"  URLs fetched: {r.get('total_urls', 0)}")
        print(f"  Task elapsed: {r.get('elapsed_seconds', 0):.2f}s")
        print(f"  Total time: {elapsed:.2f}s")
        
        print(f"\n  Sequential would take: ~10s (5 URLs × 2s each)")
        print(f"  Actual time: {elapsed:.2f}s")
        
        if elapsed < 8:
            print("  ✓ SUCCESS: Async concurrent fetching works!")
            print("  ✓ All 5 URLs were fetched IN PARALLEL within one task!")
        else:
            print("  ✗ Took longer than expected")
    else:
        print(f"  Status: {task_result.get('status')}")
        print(f"  Error: {task_result.get('error')}")


def test_hybrid_workflow():
    """
    Test the hybrid fetch-and-process pattern.
    """
    print("\n" + "=" * 70)
    print("TEST: Hybrid I/O + CPU Task")
    print("=" * 70)
    print("\nSubmitting hybrid task: Fetch URL (I/O) → Process data (CPU)")
    
    start = time.time()
    
    result = submit("/hybrid/fetch-process", {
        "url": "https://httpbin.org/bytes/10240",
        "process_iterations": 10_000_000
    })
    print(f"  Task ID: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"])
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        timing = r.get("timing", {})
        print(f"\n  Status: SUCCESS")
        print(f"  Content fetched: {r.get('content_length', 0)} bytes")
        print(f"  I/O time: {timing.get('io_seconds', 0):.3f}s")
        print(f"  CPU time: {timing.get('cpu_seconds', 0):.3f}s")
        print(f"  Total: {timing.get('total_seconds', 0):.3f}s")
        print("  ✓ Hybrid task completed - I/O and CPU work in one task!")
    else:
        print(f"  Status: {task_result.get('status')}")


def test_cpu_in_executor():
    """
    Test that CPU work doesn't block when using executor.
    """
    print("\n" + "=" * 70)
    print("TEST: CPU Work with ThreadPoolExecutor")
    print("=" * 70)
    print("\nSubmitting CPU task (100M iterations)...")
    
    start = time.time()
    
    result = submit("/cpu/compute", {"n": 100_000_000})
    print(f"  Task ID: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"])
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        print(f"\n  Status: SUCCESS")
        print(f"  Iterations: {r.get('n', 0):,}")
        print(f"  Result: {r.get('sum_of_squares', 0)}")
        print(f"  Compute time: {r.get('elapsed_seconds', 0):.2f}s")
        print(f"  Execution: {r.get('execution', 'N/A')}")
        print("  ✓ CPU work ran in ThreadPoolExecutor!")
        print("    (Event loop was free during computation)")
    else:
        print(f"  Status: {task_result.get('status')}")


def main():
    print("=" * 70)
    print("celery-aio-pool: CORRECT Usage Demonstration")
    print("=" * 70)
    print("""
The key insight:
  - celery-aio-pool runs async code WITHIN a task
  - It does NOT run multiple tasks concurrently
  - For concurrent work, use asyncio.gather() INSIDE your task
""")
    
    try:
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: API not responding")
            return
        print("✓ API is healthy\n")
        
        test_concurrent_within_task()
        test_hybrid_workflow()
        test_cpu_in_executor()
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS")
        print("=" * 70)
        print("""
1. ASYNC BENEFIT IS WITHIN A TASK:
   One task can do many concurrent I/O operations using asyncio.gather()

2. USE CASES FOR celery-aio-pool:
   - Fetch multiple APIs concurrently in one task
   - Query multiple databases in parallel
   - ETL pipelines with concurrent data sources

3. FOR MULTIPLE INDEPENDENT TASKS:
   Use multiple workers (--concurrency=N)
   Each worker handles one task at a time

4. FOR CPU WORK:
   Use run_in_executor() to avoid blocking the event loop
   Or use separate workers with prefork pool for heavy CPU
""")
        
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to API at", BASE_URL)


if __name__ == "__main__":
    main()
