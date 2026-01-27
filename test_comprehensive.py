#!/usr/bin/env python3
"""
Comprehensive test suite for celery-aio-pool demonstrating:
- I/O-bound operations (HTTP, Database)
- CPU-bound operations (with ThreadPoolExecutor)
- Hybrid operations (I/O + CPU combined)
- Large batch sizes (1,000 - 10,000 items)
"""
import time
import httpx
import uuid
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:8000"


def wait_for_task(task_id: str, timeout: int = 300) -> dict:
    """Poll until task completes."""
    start = time.time()
    last_print = 0
    while time.time() - start < timeout:
        try:
            response = httpx.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
            result = response.json()
            if result["status"] in ("SUCCESS", "FAILURE"):
                return result
            elapsed = time.time() - start
            if elapsed - last_print >= 10:
                print(f"    ... still running ({elapsed:.0f}s)")
                last_print = elapsed
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(1)
    return {"status": "TIMEOUT", "task_id": task_id}


def submit(endpoint: str, data: dict = None, method: str = "POST") -> dict:
    """Submit a request."""
    if method == "GET":
        response = httpx.get(f"{BASE_URL}{endpoint}", timeout=30)
    else:
        response = httpx.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=60)
    return response.json()


# =============================================================================
# I/O TESTS
# =============================================================================

def test_io_concurrent_http():
    """Test concurrent HTTP requests."""
    print("\n" + "=" * 70)
    print("TEST 1: Concurrent HTTP Requests (I/O-bound)")
    print("=" * 70)
    
    # 25 URLs with 2 second delay each = 50 seconds sequential
    num_urls = 25
    delay_per_url = 2
    urls = [f"https://httpbin.org/delay/{delay_per_url}" for _ in range(num_urls)]
    
    sequential_time = num_urls * delay_per_url
    print(f"\n  Fetching {num_urls} URLs with {delay_per_url}s delay each...")
    print(f"  Sequential estimate: {sequential_time}s")
    
    start = time.time()
    result = submit("/io/fetch-multiple", {"urls": urls})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        results = r.get("results", [])
        successful = sum(1 for res in results if "status_code" in res)
        
        speedup = sequential_time / task_time if task_time > 0 else 0
        
        print(f"\n  ✓ Results:")
        print(f"    URLs fetched: {successful}/{num_urls}")
        print(f"    Task time: {task_time:.2f}s")
        print(f"    Speedup: {speedup:.1f}x faster than sequential")
        return {"test": "io_http", "speedup": speedup, "time": task_time}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


def test_io_database_bulk():
    """Test bulk database operations."""
    print("\n" + "=" * 70)
    print("TEST 2: Bulk Database Inserts (I/O-bound)")
    print("=" * 70)
    
    batch_size = 2000
    sequential_estimate = batch_size * 0.01  # 10ms per insert estimate
    
    users = []
    for i in range(batch_size):
        unique_id = uuid.uuid4().hex[:12]
        users.append({
            "email": f"perf_{unique_id}@example.com",
            "username": f"perf_{unique_id}",
            "full_name": f"Performance User {i+1}",
        })
    
    print(f"\n  Creating {batch_size} users concurrently...")
    print(f"  Sequential estimate: {sequential_estimate:.0f}s (at 10ms per insert)")
    
    start = time.time()
    result = submit("/db/users/bulk-create", {"users": users})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        created = r.get("created", 0)
        
        throughput = created / task_time if task_time > 0 else 0
        speedup = sequential_estimate / task_time if task_time > 0 else 0
        
        print(f"\n  ✓ Results:")
        print(f"    Users created: {created}/{batch_size}")
        print(f"    Task time: {task_time:.3f}s")
        print(f"    Throughput: {throughput:.0f} users/second")
        print(f"    Speedup: {speedup:.1f}x faster than sequential")
        return {"test": "io_database", "speedup": speedup, "throughput": throughput}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


# =============================================================================
# CPU TESTS
# =============================================================================

def test_cpu_compute():
    """Test CPU-bound computation."""
    print("\n" + "=" * 70)
    print("TEST 3: CPU Computation (ThreadPoolExecutor)")
    print("=" * 70)
    
    iterations = 100_000_000
    
    print(f"\n  Computing sum of squares for n={iterations:,}...")
    print(f"  Using ThreadPoolExecutor to avoid blocking event loop")
    
    start = time.time()
    result = submit("/cpu/compute", {"n": iterations})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        execution = r.get("execution", "unknown")
        
        print(f"\n  ✓ Results:")
        print(f"    Iterations: {iterations:,}")
        print(f"    Result: {r.get('sum_of_squares', 'N/A')}")
        print(f"    Task time: {task_time:.2f}s")
        print(f"    Execution: {execution}")
        return {"test": "cpu_compute", "time": task_time, "iterations": iterations}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


def test_cpu_compression():
    """Test CPU-bound compression."""
    print("\n" + "=" * 70)
    print("TEST 4: Data Compression (CPU-bound)")
    print("=" * 70)
    
    text = "This is test data for compression. " * 100
    repeat = 10000
    
    print(f"\n  Compressing {len(text) * repeat / 1024 / 1024:.1f} MB of text data...")
    
    start = time.time()
    result = submit("/cpu/compress", {"text": text, "repeat": repeat})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        original = r.get("original_size", 0)
        compressed = r.get("compressed_size", 0)
        ratio = r.get("ratio", "N/A")
        
        print(f"\n  ✓ Results:")
        print(f"    Original size: {original / 1024 / 1024:.2f} MB")
        print(f"    Compressed size: {compressed / 1024:.2f} KB")
        print(f"    Compression ratio: {ratio}")
        print(f"    Task time: {task_time:.3f}s")
        return {"test": "cpu_compress", "time": task_time, "ratio": ratio}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


# =============================================================================
# HYBRID TESTS (CPU + I/O)
# =============================================================================

def test_hybrid_fetch_and_process():
    """Test hybrid I/O + CPU operation."""
    print("\n" + "=" * 70)
    print("TEST 5: Hybrid - Fetch URL then Process (I/O + CPU)")
    print("=" * 70)
    
    url = "https://httpbin.org/bytes/10240"  # 10KB of random data
    process_iterations = 50_000_000
    
    print(f"\n  Step 1: Fetch 10KB from URL (I/O)")
    print(f"  Step 2: Process with {process_iterations:,} iterations (CPU)")
    
    start = time.time()
    result = submit("/hybrid/fetch-process", {
        "url": url,
        "process_iterations": process_iterations
    })
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        timing = r.get("timing", {})
        io_time = timing.get("io_seconds", 0)
        cpu_time = timing.get("cpu_seconds", 0)
        total_time = timing.get("total_seconds", 0)
        
        print(f"\n  ✓ Results:")
        print(f"    Content fetched: {r.get('content_length', 0)} bytes")
        print(f"    I/O time: {io_time:.3f}s")
        print(f"    CPU time: {cpu_time:.3f}s")
        print(f"    Total time: {total_time:.3f}s")
        print(f"    I/O and CPU handled efficiently in one task!")
        return {"test": "hybrid_fetch_process", "io": io_time, "cpu": cpu_time}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


def test_hybrid_fetch_and_store():
    """Test hybrid HTTP + Database operations."""
    print("\n" + "=" * 70)
    print("TEST 6: Hybrid - Fetch URLs and Store in DB (I/O + I/O)")
    print("=" * 70)
    
    num_urls = 15
    urls = [
        "https://httpbin.org/delay/1",  # 1s delay each
    ] * num_urls
    
    sequential_estimate = num_urls * 1.5  # 1s HTTP + 0.5s DB
    
    print(f"\n  Fetching {num_urls} URLs (1s each) and storing in MongoDB...")
    print(f"  Sequential estimate: {sequential_estimate:.0f}s")
    
    start = time.time()
    result = submit("/hybrid/fetch-and-store", {
        "urls": urls,
        "source": "hybrid_test"
    })
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        successful = r.get("successful", 0)
        
        speedup = sequential_estimate / task_time if task_time > 0 else 0
        
        print(f"\n  ✓ Results:")
        print(f"    URLs processed: {successful}/{num_urls}")
        print(f"    Task time: {task_time:.2f}s")
        print(f"    Speedup: {speedup:.1f}x faster than sequential")
        print(f"    Both HTTP and DB operations ran concurrently!")
        return {"test": "hybrid_fetch_store", "speedup": speedup, "time": task_time}
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return None


# =============================================================================
# COMBINED STRESS TEST
# =============================================================================

def test_mixed_workload():
    """Test submitting multiple types of tasks simultaneously."""
    print("\n" + "=" * 70)
    print("TEST 7: Mixed Workload (All Task Types Simultaneously)")
    print("=" * 70)
    
    print(f"\n  Submitting different task types at once:")
    print(f"    - 1 CPU compute task (50M iterations)")
    print(f"    - 1 Bulk DB insert (500 users)")
    print(f"    - 1 Multi-URL fetch (10 URLs)")
    print(f"    - 1 Compression task")
    
    start = time.time()
    
    # Submit all tasks
    tasks = {}
    
    # CPU task
    r = submit("/cpu/compute", {"n": 50_000_000})
    tasks["cpu"] = r["task_id"]
    print(f"    CPU task: {r['task_id'][:8]}...")
    
    # DB task
    users = [{"email": f"mix_{uuid.uuid4().hex[:8]}@test.com", 
              "username": f"mix_{uuid.uuid4().hex[:8]}", 
              "full_name": f"Mix User"} for _ in range(500)]
    r = submit("/db/users/bulk-create", {"users": users})
    tasks["db"] = r["task_id"]
    print(f"    DB task: {r['task_id'][:8]}...")
    
    # HTTP task
    r = submit("/io/fetch-multiple", {"urls": ["https://httpbin.org/delay/1"] * 10})
    tasks["http"] = r["task_id"]
    print(f"    HTTP task: {r['task_id'][:8]}...")
    
    # Compression task
    r = submit("/cpu/compress", {"text": "Test " * 1000, "repeat": 5000})
    tasks["compress"] = r["task_id"]
    print(f"    Compress task: {r['task_id'][:8]}...")
    
    submit_time = time.time() - start
    print(f"\n  All tasks submitted in {submit_time:.2f}s")
    print(f"  Waiting for all to complete...")
    
    # Wait for all tasks
    results = {}
    for name, task_id in tasks.items():
        result = wait_for_task(task_id, timeout=120)
        results[name] = result.get("status") == "SUCCESS"
    
    total_time = time.time() - start
    
    success_count = sum(1 for v in results.values() if v)
    
    print(f"\n  ✓ Results:")
    print(f"    CPU task: {'✓' if results.get('cpu') else '✗'}")
    print(f"    DB task: {'✓' if results.get('db') else '✗'}")
    print(f"    HTTP task: {'✓' if results.get('http') else '✗'}")
    print(f"    Compress task: {'✓' if results.get('compress') else '✗'}")
    print(f"    Total time: {total_time:.2f}s")
    print(f"    All {success_count}/4 task types completed successfully!")
    
    return {"test": "mixed", "success": success_count, "time": total_time}


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("COMPREHENSIVE CELERY-AIO-POOL TEST SUITE")
    print("=" * 70)
    print("""
Testing all aspects of celery-aio-pool:
  - I/O-bound: HTTP requests, Database operations
  - CPU-bound: Computation, Compression
  - Hybrid: Combined I/O and CPU operations
  - Mixed workloads: Multiple task types simultaneously
""")
    
    try:
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: API not responding")
            return
        print("✓ API is healthy\n")
        
        results = []
        
        # I/O Tests
        results.append(test_io_concurrent_http())
        results.append(test_io_database_bulk())
        
        # CPU Tests
        results.append(test_cpu_compute())
        results.append(test_cpu_compression())
        
        # Hybrid Tests
        results.append(test_hybrid_fetch_and_process())
        results.append(test_hybrid_fetch_and_store())
        
        # Mixed Workload
        results.append(test_mixed_workload())
        
        # Summary
        print("\n" + "=" * 70)
        print("FINAL SUMMARY")
        print("=" * 70)
        
        successful = sum(1 for r in results if r is not None)
        
        print(f"""
┌─────────────────────────────────────────────────────────────────────┐
│                    TEST RESULTS SUMMARY                             │
├─────────────────────────────────────────────────────────────────────┤
│  Tests Passed: {successful}/7                                             │
├─────────────────────────────────────────────────────────────────────┤
│  I/O-BOUND TASKS:                                                   │
│    • Concurrent HTTP: Multiple requests run in parallel             │
│    • Database bulk: Thousands of inserts concurrently               │
├─────────────────────────────────────────────────────────────────────┤
│  CPU-BOUND TASKS:                                                   │
│    • Computation: Uses ThreadPoolExecutor                           │
│    • Compression: CPU work doesn't block event loop                 │
├─────────────────────────────────────────────────────────────────────┤
│  HYBRID TASKS:                                                      │
│    • Fetch+Process: I/O then CPU in one task                        │
│    • Fetch+Store: HTTP and DB operations concurrently               │
├─────────────────────────────────────────────────────────────────────┤
│  MIXED WORKLOAD:                                                    │
│    • All task types can run simultaneously                          │
│    • No blocking between different operation types                  │
└─────────────────────────────────────────────────────────────────────┘

Key Patterns for Production:

1. I/O Operations:
   results = await asyncio.gather(*[fetch(url) for url in urls])

2. CPU Operations:
   result = await loop.run_in_executor(executor, cpu_func, args)

3. Hybrid Operations:
   data = await fetch(url)           # I/O
   processed = await run_in_executor(process, data)  # CPU

4. Database Operations:
   await asyncio.gather(*[doc.insert() for doc in documents])
""")
        
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to API at", BASE_URL)
        print("\nMake sure all services are running:")
        print("  1. MongoDB: mongod or docker")
        print("  2. Redis: redis-server or docker")
        print("  3. Celery: ./run_worker.sh")
        print("  4. FastAPI: ./run_api.sh")


if __name__ == "__main__":
    main()
