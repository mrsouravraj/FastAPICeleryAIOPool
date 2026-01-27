#!/usr/bin/env python3
"""
Demonstrates the difference between CPU-blocking and non-blocking tasks
in celery-aio-pool.
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
            print(f"  Error polling {task_id[:8]}: {e}")
        time.sleep(1)
    return {"status": "TIMEOUT", "task_id": task_id, "result": {}}


def submit_task(endpoint: str, data: dict) -> str:
    """Submit a task and return its ID."""
    response = httpx.post(f"{BASE_URL}{endpoint}", json=data, timeout=10)
    return response.json()["task_id"]


def test_blocking_behavior():
    """
    Test: Shows how CPU-blocking task stalls other tasks.
    """
    print("\n" + "=" * 70)
    print("TEST 1: CPU-BLOCKING TASK (BAD - blocks event loop)")
    print("=" * 70)
    print("\nSubmitting 1 CPU-blocking task + 3 I/O tasks...")
    
    start = time.time()
    
    # Submit CPU blocking task first
    cpu_task_id = submit_task("/demo/cpu-blocking", {"iterations": 30_000_000})
    print(f"  CPU task: {cpu_task_id[:8]}...")
    
    # Submit I/O tasks
    io_task_ids = []
    for i in range(3):
        tid = submit_task("/demo/io-task", {"task_name": f"io_{i}", "delay": 1.0})
        io_task_ids.append(tid)
        print(f"  I/O task {i}: {tid[:8]}...")
    
    print("\nWaiting for all tasks to complete...")
    
    # Wait for CPU task
    cpu_result = wait_for_task(cpu_task_id)
    cpu_time = time.time() - start
    
    # Wait for I/O tasks
    io_results = []
    for tid in io_task_ids:
        result = wait_for_task(tid)
        io_results.append(result)
    
    total_time = time.time() - start
    
    print("\nResults:")
    if cpu_result.get('result'):
        print(f"  CPU task: {cpu_result['status']}, "
              f"elapsed={cpu_result['result'].get('elapsed_seconds', 'N/A'):.2f}s")
    else:
        print(f"  CPU task: {cpu_result.get('status', 'UNKNOWN')}")
    
    for i, result in enumerate(io_results):
        if result.get('result'):
            print(f"  I/O task {i}: {result['status']}, "
                  f"elapsed={result['result'].get('elapsed_seconds', 'N/A'):.2f}s")
        else:
            print(f"  I/O task {i}: {result.get('status', 'UNKNOWN')}")
    
    print(f"\n  TOTAL TIME: {total_time:.2f}s")
    print("  NOTE: If I/O tasks waited for CPU task, they were BLOCKED!")


def test_nonblocking_behavior():
    """
    Test: Shows how using run_in_executor prevents blocking.
    """
    print("\n" + "=" * 70)
    print("TEST 2: CPU-NONBLOCKING TASK (GOOD - uses run_in_executor)")
    print("=" * 70)
    print("\nSubmitting 1 CPU-nonblocking task + 3 I/O tasks...")
    
    start = time.time()
    
    # Submit CPU non-blocking task
    cpu_task_id = submit_task("/demo/cpu-nonblocking", {"iterations": 30_000_000})
    print(f"  CPU task: {cpu_task_id[:8]}...")
    
    # Submit I/O tasks
    io_task_ids = []
    for i in range(3):
        tid = submit_task("/demo/io-task", {"task_name": f"io_{i}", "delay": 1.0})
        io_task_ids.append(tid)
        print(f"  I/O task {i}: {tid[:8]}...")
    
    print("\nWaiting for all tasks to complete...")
    
    # Wait for all tasks
    cpu_result = wait_for_task(cpu_task_id)
    io_results = [wait_for_task(tid) for tid in io_task_ids]
    
    total_time = time.time() - start
    
    print("\nResults:")
    if cpu_result.get('result'):
        print(f"  CPU task: {cpu_result['status']}, "
              f"elapsed={cpu_result['result'].get('elapsed_seconds', 'N/A'):.2f}s")
    else:
        print(f"  CPU task: {cpu_result.get('status', 'UNKNOWN')}")
    
    for i, result in enumerate(io_results):
        if result.get('result'):
            print(f"  I/O task {i}: {result['status']}, "
                  f"elapsed={result['result'].get('elapsed_seconds', 'N/A'):.2f}s")
        else:
            print(f"  I/O task {i}: {result.get('status', 'UNKNOWN')}")
    
    print(f"\n  TOTAL TIME: {total_time:.2f}s")
    print("  I/O tasks should complete in ~1s regardless of CPU task!")


def test_many_io_tasks():
    """
    Test: Shows that many I/O tasks run concurrently.
    """
    print("\n" + "=" * 70)
    print("TEST 3: MANY I/O TASKS (demonstrates async concurrency)")
    print("=" * 70)
    print("\nSubmitting 10 I/O tasks, each with 2 second delay...")
    
    start = time.time()
    
    # Submit 10 I/O tasks
    task_ids = []
    for i in range(10):
        tid = submit_task("/demo/io-task", {"task_name": f"io_{i}", "delay": 2.0})
        task_ids.append(tid)
        print(f"  I/O task {i}: {tid[:8]}...")
    
    print("\nWaiting for all tasks to complete...")
    
    # Wait for all
    results = [wait_for_task(tid) for tid in task_ids]
    
    total_time = time.time() - start
    completed = sum(1 for r in results if r.get('status') == 'SUCCESS')
    
    print(f"\n  Completed: {completed}/10")
    print(f"  TOTAL TIME: {total_time:.2f}s")
    print(f"  Sequential would take: 20s")
    
    if total_time < 15:
        print("  SUCCESS: Tasks ran concurrently!")
    else:
        print("  NOTE: Some blocking may have occurred")


def main():
    print("=" * 70)
    print("celery-aio-pool: CPU-bound vs I/O-bound Task Demonstration")
    print("=" * 70)
    
    try:
        # Check API is running
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: API not responding")
            return
        print("API is healthy!")
        
        test_many_io_tasks()
        test_nonblocking_behavior()
        test_blocking_behavior()
        
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print("""
Key Takeaways:

1. CPU-BLOCKING TASKS (BAD):
   - async def without await blocks the event loop
   - All other tasks on the same worker wait
   
2. CPU-NONBLOCKING TASKS (GOOD):
   - Use asyncio.run_in_executor() for CPU work
   - The event loop continues handling other tasks
   
3. I/O TASKS (IDEAL FOR ASYNC):
   - Network requests, database queries, file I/O
   - Many can run concurrently with await

4. BEST PRACTICES:
   - Use celery-aio-pool for I/O-bound workloads
   - For CPU-bound: use run_in_executor or separate workers
""")
        
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to API at", BASE_URL)
        print("Make sure FastAPI and Celery worker are running.")


if __name__ == "__main__":
    main()
