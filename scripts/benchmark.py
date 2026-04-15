#!/usr/bin/env python3
"""
benchmark.py — Gravitino IRC Benchmark Suite

Covers all factors in RBC's Polaris comparison table:

  Tier 1 — Catalog API Read Latency    (Metadata Read Latency, Catalog Listing, Caching)
  Tier 2 — Catalog API Write Latency   (Metadata Write Latency, Table Commits)
  Tier 3 — Concurrent Load             (Concurrent Queries, Scalability)
  Tier 4 — Cold Start                  (Cold Start)
  Tier 5 — Trino Query Latency         (real analytical queries on 41M rows)

Output: Console tables + /tmp/benchmark_results.json

Usage:
  make benchmark
  docker compose exec python python /scripts/benchmark.py
"""

import os, sys, json, time, statistics, threading, uuid
import requests, trino
from datetime import datetime
from tabulate import tabulate

# ── Config ────────────────────────────────────────────────────────────────────
IRC_URI    = os.environ["IRC_URI"]
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
NAMESPACE  = "nyc_taxi"
TABLE_NAME = "yellow_trips"

READ_RUNS       = 10   # Tier 1 read ops
WRITE_RUNS      = 5    # Tier 2 write ops
CONCURRENT_N    = 20   # Tier 3 concurrent threads
QUERY_RUNS      = 5    # Tier 5 Trino queries

BENCH_NS    = "benchmark_scratch"   # scratch namespace for write tests
COMMIT_TABLE = "bench_commits"       # table used for INSERT commit test


# ── Helpers ───────────────────────────────────────────────────────────────────

def timed_rest(method, url, **kwargs):
    t0 = time.perf_counter()
    resp = requests.request(method, url, timeout=30, **kwargs)
    ms = (time.perf_counter() - t0) * 1000
    return resp.status_code, ms, resp

def stats(timings, label, runs=None):
    n = runs or len(timings)
    s = sorted(timings)
    return {
        "label":      label,
        "runs":       n,
        "min_ms":     round(min(timings), 2),
        "median_ms":  round(statistics.median(timings), 2),
        "p95_ms":     round(s[max(0, int(len(s) * 0.95) - 1)], 2),
        "max_ms":     round(max(timings), 2),
    }

def print_result(r):
    print(f"    median: {r['median_ms']}ms  p95: {r['p95_ms']}ms  min: {r['min_ms']}ms  max: {r['max_ms']}ms")

def trino_conn():
    return trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT,
        user="benchmark", catalog="gravitino_irc", schema=NAMESPACE,
    )

def trino_exec(sql):
    conn = trino_conn()
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    ms = (time.perf_counter() - t0) * 1000
    conn.close()
    return ms, rows


# ── Tier 1: Catalog API Read Latency ─────────────────────────────────────────
# Covers: Metadata Read Latency, Catalog Listing, Caching, Network Hops

def run_tier1():
    print(f"\n{'='*60}")
    print(f"  TIER 1 — Catalog API Read Latency  ({READ_RUNS} runs each)")
    print(f"  Covers: Metadata Read Latency, Catalog Listing, Caching")
    print(f"{'='*60}")

    base = IRC_URI
    results = []

    ops = [
        ("GET /v1/config",                    "GET", f"{base}/v1/config"),
        ("listNamespaces",                    "GET", f"{base}/v1/namespaces"),
        (f"getNamespace ({NAMESPACE})",       "GET", f"{base}/v1/namespaces/{NAMESPACE}"),
        (f"listTables ({NAMESPACE})",         "GET", f"{base}/v1/namespaces/{NAMESPACE}/tables"),
        (f"loadTable (cold)",                 "GET", f"{base}/v1/namespaces/{NAMESPACE}/tables/{TABLE_NAME}"),
    ]

    for label, method, url in ops:
        print(f"\n[R] {label} ...")
        timings = []
        for _ in range(READ_RUNS):
            status, ms, _ = timed_rest(method, url)
            if status not in (200, 204):
                print(f"    WARNING: HTTP {status}")
            timings.append(ms)
        r = stats(timings, label)
        results.append(r)
        print_result(r)

    # Warm path — loadTable 10 consecutive times
    print(f"\n[R] loadTable warm path (10 consecutive) ...")
    timings = []
    for _ in range(10):
        _, ms, _ = timed_rest("GET", f"{base}/v1/namespaces/{NAMESPACE}/tables/{TABLE_NAME}")
        timings.append(ms)
    r = stats(timings, "loadTable (warm path)")
    results.append(r)
    print_result(r)

    return results


# ── Tier 2: Catalog API Write Latency ────────────────────────────────────────
# Covers: Metadata Write Latency, Table Commits

# ── Tier 2: Catalog API Write Latency ────────────────────────────────────────
# Covers: Metadata Write Latency, Table Commits

def run_tier2():
    print(f"\n{'='*60}")
    print(f"  TIER 2 — Catalog API Write Latency  ({WRITE_RUNS} runs each)")
    print(f"  Covers: Metadata Write Latency, Table Commits")
    print(f"{'='*60}")

    base = IRC_URI
    results = []

    # Ensure scratch namespace exists via IRC REST
    resp = requests.post(f"{base}/v1/namespaces",
                         json={"namespace": [BENCH_NS]}, timeout=10)
    if resp.status_code not in (200, 201, 409):
        print(f"  WARNING: Could not create scratch namespace, HTTP {resp.status_code}: {resp.text}")

    # Ensure Trino schema exists with S3 location
    # Trino requires an explicit location on the schema to write Iceberg tables.
    S3_BUCKET = os.environ.get("S3_BUCKET", "mhoerth-gravitino-benchmark")
    schema_location = f"s3://{S3_BUCKET}/{BENCH_NS}/"
    try:
        _conn = trino_conn()
        _cur = _conn.cursor()
        # Drop and recreate to ensure location is set correctly
        try:
            _cur.execute(f"DROP SCHEMA IF EXISTS gravitino_irc.{BENCH_NS}")
            _cur.fetchall()
        except: pass
        _cur.execute(
            f"CREATE SCHEMA IF NOT EXISTS gravitino_irc.{BENCH_NS} "
            f"WITH (location = '{schema_location}')"
        )
        _cur.fetchall()
        _conn.close()
        print(f"  ✓ Scratch schema ready: {BENCH_NS} -> {schema_location}")
    except Exception as e:
        print(f"  WARNING: Trino schema setup: {e}")

    # 1. CREATE namespace
    print(f"\n[W] createNamespace ...")
    timings = []
    for i in range(WRITE_RUNS):
        ns = f"bench_ns_{i}_{uuid.uuid4().hex[:6]}"
        status, ms, _ = timed_rest("POST", f"{base}/v1/namespaces",
                                   json={"namespace": [ns]})
        timings.append(ms)
        requests.delete(f"{base}/v1/namespaces/{ns}", timeout=10)
    r = stats(timings, "createNamespace")
    results.append(r); print_result(r)

    # 2. CREATE table
    print(f"\n[W] createTable ...")
    table_schema = {
        "type": "struct",
        "fields": [
            {"id": 1, "name": "id",    "type": "long",   "required": False},
            {"id": 2, "name": "value", "type": "string", "required": False},
        ]
    }
    timings = []
    for i in range(WRITE_RUNS):
        tname = f"bench_tbl_{i}_{uuid.uuid4().hex[:6]}"
        status, ms, _ = timed_rest(
            "POST",
            f"{base}/v1/namespaces/{BENCH_NS}/tables",
            json={"name": tname, "schema": table_schema}
        )
        timings.append(ms)
    r = stats(timings, "createTable")
    results.append(r); print_result(r)

    # 3. Trino INSERT commit — times a single-row Iceberg commit via Trino
    print(f"\n[W] Trino INSERT commit ({WRITE_RUNS} runs) ...")
    try:
        setup_conn = trino_conn()
        setup_cur = setup_conn.cursor()
        setup_cur.execute(
            f"CREATE TABLE IF NOT EXISTS gravitino_irc.{BENCH_NS}.{COMMIT_TABLE} "
            f"(id BIGINT, value VARCHAR) WITH (format = 'PARQUET')"
        )
        setup_cur.fetchall()
        setup_conn.close()
        print(f"  ✓ Commit test table ready")

        timings = []
        for i in range(WRITE_RUNS):
            ms, _ = trino_exec(
                f"INSERT INTO gravitino_irc.{BENCH_NS}.{COMMIT_TABLE} "
                f"VALUES ({i}, 'benchmark_row_{i}')"
            )
            timings.append(ms)
        r = stats(timings, "Trino INSERT commit")
        results.append(r); print_result(r)
    except Exception as e:
        print(f"  ERROR: INSERT commit test failed: {e}")

    # Note: benchmark_scratch namespace left in place intentionally.
    return results


# ── Tier 3: Concurrent Load ───────────────────────────────────────────────────
# Covers: Concurrent Queries, Scalability

def run_tier3():
    print(f"\n{'='*60}")
    print(f"  TIER 3 — Concurrent Load  ({CONCURRENT_N} simultaneous threads)")
    print(f"  Covers: Concurrent Queries, Scalability")
    print(f"{'='*60}")

    base = IRC_URI
    url = f"{base}/v1/namespaces/{NAMESPACE}/tables/{TABLE_NAME}"
    results = []

    for n_threads in [1, 5, 10, 20]:
        print(f"\n[C] {n_threads} concurrent loadTable calls ...")
        timings = []
        errors = []
        barrier = threading.Barrier(n_threads)

        def worker():
            barrier.wait()  # all threads start simultaneously
            try:
                _, ms, _ = timed_rest("GET", url)
                timings.append(ms)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        t_wall0 = time.perf_counter()
        for t in threads: t.start()
        for t in threads: t.join()
        wall_ms = (time.perf_counter() - t_wall0) * 1000

        if timings:
            r = stats(timings, f"loadTable ({n_threads} concurrent)")
            r["wall_ms"] = round(wall_ms, 2)
            r["errors"] = len(errors)
            results.append(r)
            print(f"    median: {r['median_ms']}ms  p95: {r['p95_ms']}ms  "
                  f"wall: {r['wall_ms']}ms  errors: {r['errors']}")
        else:
            print(f"    FAILED — {errors}")

    return results


# ── Tier 4: Cold Start ────────────────────────────────────────────────────────
# Covers: Cold Start
# Note: measures first-call latency after a cache-busting table rename cycle.
# Full cold start (JVM restart) requires make cold-start outside the container.

def run_tier4():
    print(f"\n{'='*60}")
    print(f"  TIER 4 — Cold Start / First-Call Latency")
    print(f"  Covers: Cold Start (warm JVM, cold metadata cache)")
    print(f"{'='*60}")

    base = IRC_URI
    results = []

    # We approximate cold start by loading a freshly created table for the
    # first time — the IRC has no cached metadata for it yet.
    print(f"\n[S] First loadTable on a new table (cold metadata) ...")
    cold_timings = []
    warm_timings = []

    for i in range(5):
        # Create a fresh table
        tname = f"cold_start_{i}_{uuid.uuid4().hex[:6]}"
        table_schema = {
            "type": "struct",
            "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]
        }
        ns_resp = requests.post(f"{base}/v1/namespaces",
                                json={"namespace": [BENCH_NS]}, timeout=10)
        requests.post(f"{base}/v1/namespaces/{BENCH_NS}/tables",
                      json={"name": tname, "schema": table_schema}, timeout=10)

        # First load — cold
        _, ms_cold, _ = timed_rest("GET", f"{base}/v1/namespaces/{BENCH_NS}/tables/{tname}")
        cold_timings.append(ms_cold)

        # Second load — warm
        _, ms_warm, _ = timed_rest("GET", f"{base}/v1/namespaces/{BENCH_NS}/tables/{tname}")
        warm_timings.append(ms_warm)

        # Cleanup
        requests.delete(f"{base}/v1/namespaces/{BENCH_NS}/tables/{tname}", timeout=10)

    r_cold = stats(cold_timings, "loadTable first call (cold)")
    r_warm = stats(warm_timings, "loadTable second call (warm)")
    results.extend([r_cold, r_warm])

    print(f"  Cold  — ", end=""); print_result(r_cold)
    print(f"  Warm  — ", end=""); print_result(r_warm)
    print(f"\n  Note: Full JVM cold start requires 'make cold-start' (restarts IRC container)")

    # Cleanup
    try:
        requests.delete(f"{base}/v1/namespaces/{BENCH_NS}", timeout=10)
    except: pass

    return results


# ── Tier 5: Trino Query Latency ───────────────────────────────────────────────

def run_tier5():
    print(f"\n{'='*60}")
    print(f"  TIER 5 — Trino Query Latency  ({QUERY_RUNS} runs each)")
    print(f"  Dataset: {NAMESPACE}.{TABLE_NAME} (~41M rows, AWS S3)")
    print(f"{'='*60}")

    conn = trino_conn()
    cur = conn.cursor()
    results = []

    queries = [
        ("SHOW SCHEMAS",          "SHOW SCHEMAS IN gravitino_irc"),
        ("SHOW TABLES",           f"SHOW TABLES IN gravitino_irc.{NAMESPACE}"),
        ("DESCRIBE table",        f"DESCRIBE gravitino_irc.{NAMESPACE}.{TABLE_NAME}"),
        ("COUNT(*) — 41M rows",   f"SELECT COUNT(*) FROM gravitino_irc.{NAMESPACE}.{TABLE_NAME}"),
        ("payment_type aggregation", f"""
            SELECT payment_type, COUNT(*), ROUND(AVG(total_amount), 2)
            FROM gravitino_irc.{NAMESPACE}.{TABLE_NAME}
            GROUP BY payment_type ORDER BY 2 DESC"""),
        ("avg fare by month", f"""
            SELECT MONTH(tpep_pickup_datetime) AS month,
                   COUNT(*) AS trips,
                   ROUND(AVG(fare_amount), 2) AS avg_fare
            FROM gravitino_irc.{NAMESPACE}.{TABLE_NAME}
            WHERE YEAR(tpep_pickup_datetime) = 2024
            GROUP BY MONTH(tpep_pickup_datetime) ORDER BY 1"""),
        ("top pickup locations", f"""
            SELECT PULocationID, COUNT(*) AS trips
            FROM gravitino_irc.{NAMESPACE}.{TABLE_NAME}
            GROUP BY PULocationID ORDER BY 2 DESC LIMIT 10"""),
    ]

    for label, sql in queries:
        print(f"\n[Q] {label} ...")
        timings = []
        for i in range(QUERY_RUNS):
            t0 = time.perf_counter()
            cur.execute(sql)
            rows = cur.fetchall()
            ms = (time.perf_counter() - t0) * 1000
            timings.append(ms)
            if i == 0:
                print(f"    Run 1: {rows[:2]}{'...' if len(rows) > 2 else ''}")
        r = stats(timings, label)
        results.append(r)
        print_result(r)

    conn.close()
    return results


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(t1, t2, t3, t4, t5):
    print(f"\n{'='*60}")
    print(f"  BENCHMARK RESULTS — Gravitino IRC 1.2.0")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  Dataset: ~41M rows, AWS S3 us-east-2, MySQL backend")
    print(f"{'='*60}")

    hdrs = ["Operation", "Runs", "Min (ms)", "Median (ms)", "P95 (ms)", "Max (ms)"]

    def table_rows(results):
        return [[r["label"], r["runs"], r["min_ms"], r["median_ms"],
                 r["p95_ms"], r["max_ms"]] for r in results]

    print(f"\n--- Tier 1: Catalog API Read Latency ---")
    print(tabulate(table_rows(t1), headers=hdrs, tablefmt="github"))

    print(f"\n--- Tier 2: Catalog API Write Latency ---")
    print(tabulate(table_rows(t2), headers=hdrs, tablefmt="github"))

    print(f"\n--- Tier 3: Concurrent Load ---")
    hdrs3 = ["Operation", "Threads", "Min (ms)", "Median (ms)", "P95 (ms)", "Wall (ms)", "Errors"]
    t3_rows = [[r["label"], r["runs"], r["min_ms"], r["median_ms"],
                r["p95_ms"], r.get("wall_ms",""), r.get("errors",0)] for r in t3]
    print(tabulate(t3_rows, headers=hdrs3, tablefmt="github"))

    print(f"\n--- Tier 4: Cold Start ---")
    print(tabulate(table_rows(t4), headers=hdrs, tablefmt="github"))

    print(f"\n--- Tier 5: Trino Query Latency ---")
    print(tabulate(table_rows(t5), headers=hdrs, tablefmt="github"))

    output = {
        "gravitino_version": "1.2.0",
        "timestamp_utc": datetime.utcnow().isoformat(),
        "dataset": f"{NAMESPACE}.{TABLE_NAME}",
        "tier1_read_latency": t1,
        "tier2_write_latency": t2,
        "tier3_concurrent": t3,
        "tier4_cold_start": t4,
        "tier5_trino_queries": t5,
    }
    with open("/tmp/benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n  Results saved to /tmp/benchmark_results.json")
    print(f"{'='*60}\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\nGravitino IRC Benchmark — starting")
    print(f"IRC:   {IRC_URI}")
    print(f"Trino: {TRINO_HOST}:{TRINO_PORT}")

    try:
        resp = requests.get(f"{IRC_URI}/v1/config", timeout=10)
        resp.raise_for_status()
        print(f"✓ IRC reachable")
    except Exception as e:
        print(f"✗ Cannot reach IRC: {e}")
        sys.exit(1)

    t1 = run_tier1()
    t2 = run_tier2()
    t3 = run_tier3()
    t4 = run_tier4()
    t5 = run_tier5()
    print_report(t1, t2, t3, t4, t5)


if __name__ == "__main__":
    main()
