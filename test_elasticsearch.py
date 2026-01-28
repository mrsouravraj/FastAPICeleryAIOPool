"""
Elasticsearch (async) integration test via FastAPI + Celery.

Requires:
- FastAPI running on http://localhost:8000
- Celery worker running with celery-aio-pool
- Elasticsearch running on http://localhost:9200 (default)

Run:
  python test_elasticsearch.py
"""

from __future__ import annotations

import time
import uuid
import httpx


API = "http://localhost:8000"


def wait_for_task(task_id: str, timeout: float = 60.0) -> dict:
    start = time.time()
    while True:
        r = httpx.get(f"{API}/tasks/{task_id}", timeout=10.0)
        r.raise_for_status()
        data = r.json()
        if data["status"] in ("SUCCESS", "FAILURE", "REVOKED"):
            return data
        if time.time() - start > timeout:
            return {"task_id": task_id, "status": "TIMEOUT"}
        time.sleep(1.0)


def main():
    print("=" * 70)
    print("ELASTICSEARCH ASYNC TEST (FastAPI + Celery)")
    print("=" * 70)

    # API health
    r = httpx.get(f"{API}/health", timeout=10.0)
    r.raise_for_status()
    print("✓ API is healthy")

    # ES health (direct from API process)
    r = httpx.get(f"{API}/es/health", timeout=10.0)
    if r.status_code != 200:
        print("✗ Elasticsearch not reachable from API. Details:")
        print(r.text)
        return
    print("✓ Elasticsearch is reachable:", r.json())

    index = f"demo_docs_{uuid.uuid4().hex[:8]}"

    # Bulk index via Celery task
    docs = [{"message": f"hello {i}", "group": "demo"} for i in range(50)]
    r = httpx.post(
        f"{API}/es/bulk-index",
        json={"index": index, "documents": docs},
        timeout=30.0,
    )
    r.raise_for_status()
    task_id = r.json()["task_id"]
    print("Bulk index task:", task_id)
    bulk_res = wait_for_task(task_id, timeout=60.0)
    print("Bulk index result status:", bulk_res["status"])
    if bulk_res["status"] != "SUCCESS":
        print(bulk_res)
        return

    # Search via Celery task
    query = {"match": {"group": "demo"}}
    r = httpx.post(
        f"{API}/es/search",
        json={"index": index, "query": query, "size": 5},
        timeout=30.0,
    )
    r.raise_for_status()
    task_id = r.json()["task_id"]
    print("Search task:", task_id)
    search_res = wait_for_task(task_id, timeout=60.0)
    print("Search result status:", search_res["status"])
    if search_res["status"] != "SUCCESS":
        print(search_res)
        return

    hits = (search_res.get("result") or {}).get("hits", [])
    print(f"✓ Search hits returned: {len(hits)}")
    for h in hits:
        print(" -", h.get("source"))


if __name__ == "__main__":
    main()

