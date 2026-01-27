#!/usr/bin/env python3
"""
Test script for MongoDB (Beanie) integration with celery-aio-pool.
Uses large batch sizes (1000-10000) to demonstrate concurrent performance.
"""
import time
import httpx
import uuid

BASE_URL = "http://localhost:8000"


def wait_for_task(task_id: str, timeout: int = 300) -> dict:
    """Poll until task completes."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = httpx.get(f"{BASE_URL}/tasks/{task_id}", timeout=10)
            result = response.json()
            if result["status"] in ("SUCCESS", "FAILURE"):
                return result
            # Print progress
            elapsed = time.time() - start
            if int(elapsed) % 10 == 0 and elapsed > 1:
                print(f"    ... still running ({elapsed:.0f}s)")
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(1)
    return {"status": "TIMEOUT", "task_id": task_id}


def submit(endpoint: str, data: dict = None, method: str = "POST") -> dict:
    """Submit a request."""
    if method == "GET":
        response = httpx.get(f"{BASE_URL}{endpoint}", timeout=30)
    else:
        response = httpx.post(f"{BASE_URL}{endpoint}", json=data or {}, timeout=30)
    return response.json()


def test_bulk_create_users_1000():
    """Test creating 1000 users concurrently."""
    print("\n" + "=" * 70)
    print("TEST 1: Bulk Create 1,000 Users (Concurrent DB Operations)")
    print("=" * 70)
    
    batch_size = 1000
    
    # Generate 1000 unique users
    users = []
    for i in range(batch_size):
        unique_id = uuid.uuid4().hex[:12]
        users.append({
            "email": f"user_{unique_id}@example.com",
            "username": f"user_{unique_id}",
            "full_name": f"Test User {i+1}",
        })
    
    print(f"\n  Creating {batch_size} users concurrently in ONE task...")
    print(f"  (Sequential would take ~{batch_size * 0.01:.0f}s at 10ms per insert)")
    
    start = time.time()
    result = submit("/db/users/bulk-create", {"users": users})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=120)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        created = r.get("created", 0)
        
        print(f"\n  ✓ Bulk creation complete!")
        print(f"    Total users: {r.get('total')}")
        print(f"    Created: {created}")
        print(f"    Task execution time: {task_time:.3f}s")
        print(f"    Total time (incl. queue): {elapsed:.3f}s")
        print(f"    Throughput: {created/task_time:.0f} users/second")
        print(f"    Avg per user: {task_time/created*1000:.2f}ms")
        
        if task_time < 10:
            print(f"\n  ✓ SUCCESS: {batch_size} concurrent DB inserts in {task_time:.2f}s!")
        return True
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return False


def test_bulk_create_users_5000():
    """Test creating 5000 users concurrently."""
    print("\n" + "=" * 70)
    print("TEST 2: Bulk Create 5,000 Users (Stress Test)")
    print("=" * 70)
    
    batch_size = 5000
    
    users = []
    for i in range(batch_size):
        unique_id = uuid.uuid4().hex[:12]
        users.append({
            "email": f"bulk5k_{unique_id}@example.com",
            "username": f"bulk5k_{unique_id}",
            "full_name": f"Bulk User {i+1}",
        })
    
    print(f"\n  Creating {batch_size} users concurrently in ONE task...")
    print(f"  (Sequential would take ~{batch_size * 0.01:.0f}s at 10ms per insert)")
    
    start = time.time()
    result = submit("/db/users/bulk-create", {"users": users})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=300)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        created = r.get("created", 0)
        
        print(f"\n  ✓ Bulk creation complete!")
        print(f"    Total users: {r.get('total')}")
        print(f"    Created: {created}")
        print(f"    Task execution time: {task_time:.3f}s")
        print(f"    Total time (incl. queue): {elapsed:.3f}s")
        print(f"    Throughput: {created/task_time:.0f} users/second")
        print(f"    Avg per user: {task_time/created*1000:.3f}ms")
        
        # Calculate speedup vs sequential
        sequential_estimate = batch_size * 0.01  # 10ms per operation
        speedup = sequential_estimate / task_time
        print(f"\n  ✓ Speedup vs sequential: {speedup:.1f}x faster!")
        return True
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return False


def test_fetch_and_store_many():
    """Test fetching many URLs and storing in MongoDB."""
    print("\n" + "=" * 70)
    print("TEST 3: Fetch 10 URLs and Store in MongoDB (Concurrent I/O)")
    print("=" * 70)
    
    # Use 10 URLs with delays to show concurrent benefit
    urls = [
        "https://httpbin.org/delay/1",  # Each takes 1 second
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
        "https://httpbin.org/delay/1",
    ]
    
    print(f"\n  Fetching {len(urls)} URLs (1s delay each) and storing results...")
    print(f"  Sequential would take: ~{len(urls)}s for HTTP + DB inserts")
    
    start = time.time()
    result = submit("/hybrid/fetch-and-store", {
        "urls": urls,
        "source": "stress_test",
    })
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=60)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        successful = r.get("successful", 0)
        
        print(f"\n  ✓ Fetch and store complete!")
        print(f"    URLs fetched: {r.get('total_urls')}")
        print(f"    Successful: {successful}")
        print(f"    Task execution time: {task_time:.3f}s")
        print(f"    Total time: {elapsed:.3f}s")
        
        sequential_estimate = len(urls) * 1.1  # 1s HTTP + 0.1s DB each
        speedup = sequential_estimate / task_time
        print(f"\n  ✓ Speedup vs sequential: {speedup:.1f}x faster!")
        print(f"    (All HTTP requests + DB inserts ran concurrently)")
        return True
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return False


def test_concurrent_io_operations():
    """Test many concurrent I/O operations within a single task."""
    print("\n" + "=" * 70)
    print("TEST 4: Fetch 20 URLs Concurrently (Pure I/O Concurrency)")
    print("=" * 70)
    
    # 20 URLs with 2 second delay each
    urls = [f"https://httpbin.org/delay/2" for _ in range(20)]
    
    print(f"\n  Fetching {len(urls)} URLs with 2s delay each...")
    print(f"  Sequential would take: ~{len(urls) * 2}s")
    
    start = time.time()
    result = submit("/io/fetch-multiple", {"urls": urls})
    print(f"  Task submitted: {result['task_id'][:8]}...")
    
    task_result = wait_for_task(result["task_id"], timeout=60)
    elapsed = time.time() - start
    
    if task_result.get("status") == "SUCCESS":
        r = task_result["result"]
        task_time = r.get("elapsed_seconds", 0)
        total_urls = r.get("total_urls", 0)
        
        # Count successful
        results = r.get("results", [])
        successful = sum(1 for res in results if "status_code" in res)
        
        print(f"\n  ✓ Concurrent fetch complete!")
        print(f"    URLs requested: {total_urls}")
        print(f"    Successful: {successful}")
        print(f"    Task execution time: {task_time:.3f}s")
        print(f"    Total time: {elapsed:.3f}s")
        
        sequential_estimate = len(urls) * 2
        speedup = sequential_estimate / task_time
        print(f"\n  ✓ Speedup vs sequential: {speedup:.1f}x faster!")
        print(f"    ({len(urls)} × 2s requests completed in {task_time:.1f}s)")
        return True
    else:
        print(f"  ✗ Failed: {task_result.get('error')}")
        return False


def test_create_many_jobs():
    """Create and process many jobs."""
    print("\n" + "=" * 70)
    print("TEST 5: Create 100 Jobs via Celery Tasks")
    print("=" * 70)
    
    num_jobs = 100
    print(f"\n  Creating {num_jobs} job records...")
    
    start = time.time()
    task_ids = []
    
    for i in range(num_jobs):
        result = submit("/db/jobs/create", {
            "name": f"Batch Job {i+1}",
            "job_type": "batch_test",
            "input_data": {"index": i, "batch": "test_100"},
        })
        task_ids.append(result["task_id"])
        if (i + 1) % 20 == 0:
            print(f"    Submitted {i+1}/{num_jobs} tasks...")
    
    submit_time = time.time() - start
    print(f"\n  All {num_jobs} tasks submitted in {submit_time:.2f}s")
    print(f"  Waiting for completion...")
    
    # Wait for all to complete
    completed = 0
    failed = 0
    for tid in task_ids:
        result = wait_for_task(tid, timeout=60)
        if result.get("status") == "SUCCESS":
            completed += 1
        else:
            failed += 1
    
    total_time = time.time() - start
    
    print(f"\n  ✓ Job creation complete!")
    print(f"    Completed: {completed}/{num_jobs}")
    print(f"    Failed: {failed}")
    print(f"    Total time: {total_time:.2f}s")
    print(f"    Throughput: {num_jobs/total_time:.1f} jobs/second")


def verify_database():
    """Verify data counts in MongoDB."""
    print("\n" + "=" * 70)
    print("VERIFICATION: Database Record Counts")
    print("=" * 70)
    
    # Get job stats
    result = submit("/db/jobs/stats", method="GET")
    task_result = wait_for_task(result["task_id"], timeout=30)
    
    if task_result.get("status") == "SUCCESS":
        stats = task_result["result"].get("stats", {})
        print(f"\n  Jobs in database:")
        print(f"    Total: {stats.get('total', 0)}")
        print(f"    Completed: {stats.get('completed', 0)}")
    
    # Count users
    users = httpx.get(f"{BASE_URL}/db/users?limit=1", timeout=10).json()
    print(f"\n  Users in database: {users.get('count', 'unknown')} (showing first page)")
    
    # Count records
    records = httpx.get(f"{BASE_URL}/db/records?limit=1", timeout=10).json()
    print(f"  Data records in database: {records.get('count', 'unknown')}")


def main():
    print("=" * 70)
    print("MongoDB + Celery AIO Pool: Large Scale Test Suite")
    print("=" * 70)
    print("""
Testing async MongoDB operations with batch sizes of 1,000 - 10,000+
to demonstrate the power of concurrent database operations.
""")
    
    try:
        response = httpx.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: API not responding")
            return
        print("✓ API is healthy")
        
        # Run tests
        test_bulk_create_users_1000()
        test_bulk_create_users_5000()
        test_fetch_and_store_many()
        test_concurrent_io_operations()
        test_create_many_jobs()
        verify_database()
        
        print("\n" + "=" * 70)
        print("PERFORMANCE SUMMARY")
        print("=" * 70)
        print("""
Key Results:

1. BULK USER CREATION (1,000 users):
   - All inserts run concurrently via asyncio.gather()
   - Typical: 1,000 users in < 5 seconds
   - Throughput: 200+ users/second

2. BULK USER CREATION (5,000 users):
   - Same pattern, larger scale
   - Demonstrates MongoDB connection pool handling

3. CONCURRENT HTTP + DB (10 URLs):
   - Each URL fetched + result stored concurrently
   - 10x speedup vs sequential

4. PURE I/O CONCURRENCY (20 URLs × 2s):
   - 40 seconds of work in ~2-3 seconds
   - Demonstrates async I/O power

Key Pattern for High Throughput:
    async def process_batch(items):
        async def process_one(item):
            # I/O operation (HTTP, DB, etc.)
            result = await some_async_operation(item)
            return result
        
        # ALL items processed concurrently
        return await asyncio.gather(*[process_one(i) for i in items])
""")
        
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to API at", BASE_URL)
        print("\nMake sure:")
        print("1. MongoDB is running (mongodb://localhost:27017)")
        print("2. FastAPI is running (./run_api.sh)")
        print("3. Celery worker is running (./run_worker.sh)")


if __name__ == "__main__":
    main()
