#!/usr/bin/env python3
"""
populate_catalog.py — Create 1,000 namespaces with 10 tables each (10,000 tables total).

This simulates an enterprise-scale catalog for realistic listNamespaces and
listTables benchmark measurements.

Tables are empty — we are testing catalog metadata performance, not data queries.

Usage:
  make populate
  docker compose exec python python /scripts/populate_catalog.py
"""

import os, sys, time, requests
from concurrent.futures import ThreadPoolExecutor, as_completed

IRC_URI       = os.environ["IRC_URI"]
NAMESPACE_COUNT = 1000
TABLES_PER_NS   = 10
WORKERS         = 20   # parallel threads for faster population

TABLE_SCHEMA = {
    "type": "struct",
    "fields": [
        {"id": 1, "name": "id",        "type": "long",   "required": False},
        {"id": 2, "name": "value",     "type": "string", "required": False},
        {"id": 3, "name": "amount",    "type": "double", "required": False},
        {"id": 4, "name": "created_at","type": "timestamp", "required": False},
    ]
}

def create_namespace(ns):
    resp = requests.post(f"{IRC_URI}/v1/namespaces",
                         json={"namespace": [ns]}, timeout=30)
    return resp.status_code in (200, 201, 409)

def create_table(ns, tname):
    resp = requests.post(
        f"{IRC_URI}/v1/namespaces/{ns}/tables",
        json={"name": tname, "schema": TABLE_SCHEMA},
        timeout=30
    )
    return resp.status_code in (200, 201)

def populate_namespace(ns_index):
    ns = f"dept_{ns_index:04d}"
    if not create_namespace(ns):
        return ns, 0
    count = 0
    for t in range(TABLES_PER_NS):
        tname = f"table_{t:02d}"
        if create_table(ns, tname):
            count += 1
    return ns, count

def main():
    print(f"\n{'='*60}")
    print(f"  Gravitino IRC — Catalog Population")
    print(f"{'='*60}")
    print(f"  IRC:        {IRC_URI}")
    print(f"  Namespaces: {NAMESPACE_COUNT}")
    print(f"  Tables:     {NAMESPACE_COUNT * TABLES_PER_NS:,} total ({TABLES_PER_NS} per namespace)")
    print(f"  Workers:    {WORKERS} parallel threads\n")

    # Verify connectivity
    resp = requests.get(f"{IRC_URI}/v1/config", timeout=10)
    if resp.status_code != 200:
        print(f"  ERROR: IRC not reachable")
        sys.exit(1)
    print("  ✓ IRC reachable\n")

    t0 = time.time()
    completed = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(populate_namespace, i): i
                   for i in range(NAMESPACE_COUNT)}
        for future in as_completed(futures):
            ns, count = future.result()
            completed += 1
            if count < TABLES_PER_NS:
                failed += 1
            if completed % 100 == 0:
                elapsed = time.time() - t0
                rate = completed / elapsed
                remaining = (NAMESPACE_COUNT - completed) / rate
                print(f"  {completed}/{NAMESPACE_COUNT} namespaces "
                      f"({completed * TABLES_PER_NS:,} tables) "
                      f"— {rate:.1f} ns/s — {remaining:.0f}s remaining")

    elapsed = time.time() - t0
    total_tables = (completed - failed) * TABLES_PER_NS

    print(f"\n{'='*60}")
    print(f"  Population complete")
    print(f"  Namespaces: {completed - failed:,} succeeded, {failed} failed")
    print(f"  Tables:     {total_tables:,} created")
    print(f"  Time:       {elapsed:.1f}s")
    print(f"  Rate:       {completed/elapsed:.1f} namespaces/sec")
    print(f"\n  Run: make benchmark")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
