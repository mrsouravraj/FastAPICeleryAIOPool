#!/usr/bin/env python3
"""
Test script to verify the FastAPI + Celery AIO Pool setup.
Run this after starting Redis, the Celery worker, and the FastAPI app.
"""
import time
import httpx

BASE_URL = "http://localhost:8000"


def wait_for_task(task_id: str, timeout: int = 60) -> dict:
    """Poll task status until complete or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        response = httpx.get(f"{BASE_URL}/tasks/{task_id}")
        result = response.json()
        if result["status"] == "SUCCESS":
            return result
        if result["status"] == "FAILURE":
            print(f"  Task FAILED: {result.get('error', 'Unknown error')}")
            return result
        print(f"  Task {task_id[:8]}... status: {result['status']}")
        time.sleep(2)
    raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")


def test_health():
    """Test health endpoint."""
    print("\n1. Testing health endpoint...")
    response = httpx.get(f"{BASE_URL}/health")
    assert response.status_code == 200
    print(f"   Health: {response.json()}")


def test_async_fetch_url():
    """Test async URL fetch task."""
    print("\n2. Testing async URL fetch task...")
    response = httpx.post(
        f"{BASE_URL}/tasks/fetch-url",
        json={"url": "https://httpbin.org/get"}
    )
    assert response.status_code == 200
    task_data = response.json()
    print(f"   Task submitted: {task_data['task_id']}")
    
    result = wait_for_task(task_data["task_id"])
    print(f"   Result: status_code={result['result']['status_code']}, "
          f"content_length={result['result']['content_length']}")
    assert result["status"] == "SUCCESS"
    assert result["result"]["status_code"] == 200


def test_async_sleep():
    """Test async sleep task."""
    print("\n3. Testing async sleep task (3 seconds)...")
    response = httpx.post(
        f"{BASE_URL}/tasks/sleep",
        json={"seconds": 3}
    )
    assert response.status_code == 200
    task_data = response.json()
    print(f"   Task submitted: {task_data['task_id']}")
    
    start = time.time()
    result = wait_for_task(task_data["task_id"])
    elapsed = time.time() - start
    print(f"   Result: {result['result']['message']} (took {elapsed:.1f}s)")
    assert result["status"] == "SUCCESS"


def test_parallel_fetch():
    """Test parallel URL fetch task."""
    print("\n4. Testing parallel URL fetch task...")
    urls = [
        "https://httpbin.org/get",
        "https://httpbin.org/ip",
        "https://httpbin.org/headers",
    ]
    response = httpx.post(
        f"{BASE_URL}/tasks/parallel-fetch",
        json={"urls": urls}
    )
    assert response.status_code == 200
    task_data = response.json()
    print(f"   Task submitted: {task_data['task_id']}")
    
    result = wait_for_task(task_data["task_id"])
    print(f"   Fetched {result['result']['total_urls']} URLs in parallel:")
    for r in result["result"]["results"]:
        print(f"     - {r['url']}: status={r.get('status_code', 'error')}")
    assert result["status"] == "SUCCESS"


def test_compute_task():
    """Test async compute task."""
    print("\n5. Testing async compute task...")
    response = httpx.post(
        f"{BASE_URL}/tasks/compute",
        json={"n": 10000}
    )
    assert response.status_code == 200
    task_data = response.json()
    print(f"   Task submitted: {task_data['task_id']}")
    
    result = wait_for_task(task_data["task_id"])
    print(f"   Sum of squares for n={result['result']['n']}: "
          f"{result['result']['sum_of_squares']}")
    assert result["status"] == "SUCCESS"


def test_sync_task():
    """Test synchronous task (also supported by celery-aio-pool)."""
    print("\n6. Testing sync task...")
    response = httpx.post(
        f"{BASE_URL}/tasks/sync-message",
        json={"message": "Hello from sync task!"}
    )
    assert response.status_code == 200
    task_data = response.json()
    print(f"   Task submitted: {task_data['task_id']}")
    
    result = wait_for_task(task_data["task_id"])
    print(f"   Result: {result['result']['message']}")
    assert result["status"] == "SUCCESS"


def main():
    print("=" * 60)
    print("FastAPI + Celery AIO Pool Test Suite")
    print("=" * 60)
    
    try:
        test_health()
        # Test simpler tasks first that don't depend on external services
        test_async_sleep()
        test_compute_task()
        test_sync_task()
        # Then test HTTP-based tasks (may fail due to network/SSL issues)
        try:
            test_async_fetch_url()
            test_parallel_fetch()
        except Exception as e:
            print(f"\n   Note: HTTP tasks may fail due to network/SSL issues: {e}")
        
        print("\n" + "=" * 60)
        print("All core tests passed!")
        print("=" * 60)
    except httpx.ConnectError:
        print("\nERROR: Cannot connect to the API.")
        print("Make sure the FastAPI app is running on http://localhost:8000")
    except Exception as e:
        print(f"\nERROR: {e}")
        raise


if __name__ == "__main__":
    main()
