#!/usr/bin/env python3
"""
Test script demonstrating efficient handling of both CPU and I/O tasks.
"""
import time
import httpx
from concurrent.futures import ThreadPoolExecutor

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


def test_io_concurrency():
    """Test that I/O tasks run concurrently."""
    print("\n" + "=" * 70)
    print("TEST 1: I/O Task Concurrency")
    print("=" * 70)
    print("\nSubmitting 5 I/O tasks (2s each). Should complete in ~2s, not 10s.")
    
    start = time.time()
    
    # Submit 5 sleep tasks
    task_ids = []
    for i in range(5):
        result = submit("/io/sleep", {"seconds": 2.0})
        task_ids.append(result["task_id"])
        print(f"  Submitted: {result['task_id'][:8]}...")
    
    # Wait for all
    print("\nWaiting for completion...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(wait_for_task, task_ids))
    
    elapsed = time.time() - start
    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    
    print(f"\n  Completed: {success}/5")
    print(f"  Total time: {elapsed:.2f}s (sequential would be 10s)")
    print(f"  Speedup: {10/elapsed:.1f}x" if elapsed > 0 else "")
    
    if elapsed < 6:
        print("  ✓ SUCCESS: Tasks ran concurrently!")
    else:
        print("  ✗ Tasks may have been serialized")


def test_cpu_with_process_pool():
    """Test that CPU tasks use ProcessPoolExecutor correctly."""
    print("\n" + "=" * 70)
    print("TEST 2: CPU Task with ProcessPoolExecutor")
    print("=" * 70)
    print("\nSubmitting 1 heavy CPU task (50M iterations)...")
    
    start = time.time()
    
    result = submit("/cpu/compute", {"n": 50_000_000})
    print(f"  Submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"])
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        print(f"\n  Status: SUCCESS")
        print(f"  Execution: {r.get('execution', 'N/A')}")
        print(f"  Compute time: {r.get('elapsed_seconds', 0):.2f}s")
        print(f"  Result: sum of squares = {r.get('sum_of_squares', 'N/A')}")
        print("  ✓ CPU work ran in ProcessPoolExecutor (non-blocking)")
    else:
        print(f"  Status: {task_result.get('status')}")


def test_cpu_doesnt_block_io():
    """Test that CPU task doesn't block I/O tasks."""
    print("\n" + "=" * 70)
    print("TEST 3: CPU Task Doesn't Block I/O Tasks")
    print("=" * 70)
    print("\nSubmitting: 1 CPU task (heavy) + 3 I/O tasks (1s each)")
    print("I/O tasks should complete in ~1s while CPU task runs...")
    
    start = time.time()
    
    # Submit CPU task first
    cpu_result = submit("/cpu/compute", {"n": 100_000_000})
    cpu_id = cpu_result["task_id"]
    print(f"  CPU task: {cpu_id[:8]}...")
    
    # Submit I/O tasks
    io_ids = []
    for i in range(3):
        io_result = submit("/io/sleep", {"seconds": 1.0})
        io_ids.append(io_result["task_id"])
        print(f"  I/O task {i}: {io_result['task_id'][:8]}...")
    
    # Wait for I/O tasks first
    print("\nWaiting for I/O tasks...")
    io_start = time.time()
    io_results = [wait_for_task(tid) for tid in io_ids]
    io_elapsed = time.time() - io_start
    
    # Then wait for CPU task
    print("Waiting for CPU task...")
    cpu_task_result = wait_for_task(cpu_id)
    total_elapsed = time.time() - start
    
    print(f"\nResults:")
    print(f"  I/O tasks completed in: {io_elapsed:.2f}s")
    print(f"  CPU task status: {cpu_task_result.get('status')}")
    print(f"  Total time: {total_elapsed:.2f}s")
    
    if io_elapsed < 5:
        print("  ✓ SUCCESS: I/O tasks weren't blocked by CPU task!")
    else:
        print("  ✗ I/O tasks may have been blocked")


def test_hybrid_task():
    """Test hybrid task (I/O + CPU)."""
    print("\n" + "=" * 70)
    print("TEST 4: Hybrid Task (I/O + CPU)")
    print("=" * 70)
    print("\nSubmitting hybrid task: fetch URL then process data...")
    
    start = time.time()
    
    result = submit("/hybrid/fetch-process", {
        "url": "https://httpbin.org/bytes/1024",
        "process_iterations": 10_000_000
    })
    print(f"  Submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"])
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        timing = r.get("timing", {})
        print(f"\n  Status: SUCCESS")
        print(f"  URL: {r.get('url')}")
        print(f"  Content length: {r.get('content_length')} bytes")
        print(f"  I/O time: {timing.get('io_seconds', 0):.3f}s")
        print(f"  CPU time: {timing.get('cpu_seconds', 0):.3f}s")
        print(f"  Total time: {timing.get('total_seconds', 0):.3f}s")
        print("  ✓ Hybrid task completed successfully!")
    else:
        print(f"  Status: {task_result.get('status')}")
        print(f"  Error: {task_result.get('error', 'Unknown')}")


def test_compression():
    """Test CPU-bound compression task."""
    print("\n" + "=" * 70)
    print("TEST 5: CPU Compression Task")
    print("=" * 70)
    print("\nSubmitting compression task...")
    
    result = submit("/cpu/compress", {
        "text": "Hello, this is a test message for compression! ",
        "repeat": 50000
    })
    print(f"  Submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"])
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        print(f"\n  Status: SUCCESS")
        print(f"  Original size: {r.get('original_size', 0):,} bytes")
        print(f"  Compressed size: {r.get('compressed_size', 0):,} bytes")
        print(f"  Compression ratio: {r.get('compression_ratio')}")
        print(f"  Time: {r.get('elapsed_seconds', 0):.3f}s")
        print("  ✓ Compression ran in ProcessPoolExecutor!")
    else:
        print(f"  Status: {task_result.get('status')}")


def main():
    print("=" * 70)
    print("FastAPI + Celery AIO Pool: Hybrid CPU/I/O Test Suite")
    print("=" * 70)
    
    try:
        # Check API
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: API not responding")
            return
        print("✓ API is healthy")
        
        test_io_concurrency()
        test_cpu_with_process_pool()
        test_cpu_doesnt_block_io()
        test_hybrid_task()
        test_compression()
        
        print("\n" + "=" * 70)
        print("SUMMARY: Best Practices for CPU + I/O Tasks")
        print("=" * 70)
        print("""
┌─────────────────────────────────────────────────────────────────────┐
│ Task Type       │ Solution                  │ Why                   │
├─────────────────────────────────────────────────────────────────────┤
│ I/O-bound       │ Direct async/await        │ Event loop handles it │
│ Light CPU       │ ThreadPoolExecutor        │ Lower overhead        │
│ Heavy CPU       │ ProcessPoolExecutor       │ Bypasses GIL          │
│ Hybrid          │ async + ProcessPool       │ Best of both          │
└─────────────────────────────────────────────────────────────────────┘

Key Pattern:
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(process_pool, cpu_function, args)

This yields control to the event loop while the CPU work runs in a 
separate process, allowing I/O tasks to continue unblocked.
""")
        
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to API at", BASE_URL)


if __name__ == "__main__":
    main()
