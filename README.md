# Gravitino IRC Benchmark

Measures Apache Gravitino 1.2.0 Iceberg REST Catalog (IRC) performance against AWS S3.

## What this measures

**Tier 1 — Catalog API latency** (direct REST calls to IRC):
- `GET /v1/config` — config fetch
- `GET /v1/namespaces` — list namespaces
- `GET /v1/namespaces/{ns}` — get namespace
- `GET /v1/namespaces/{ns}/tables` — list tables
- `GET /v1/namespaces/{ns}/tables/{table}` — loadTable (the key IRC operation)
- loadTable warm path — 10 consecutive calls

**Tier 2 — Trino query latency** (via IRC):
- SHOW SCHEMAS, SHOW TABLES, DESCRIBE (metadata operations)
- COUNT(*) full scan
- Payment type aggregation
- Average fare by month
- Top pickup locations

Each operation is run multiple times; results report min, median, P95, and max.

## Stack

| Component | Version |
|-----------|---------|
| Gravitino IRC | 1.2.0 |
| MySQL (catalog backend) | 8.0 |
| Trino | 469 |
| Object store | AWS S3 (us-east-2) |
| Dataset | NYC Taxi yellow trips 2024 (12 months, ~650MB, ~41M rows) |

## Prerequisites

- EC2 instance in `us-east-2` (recommended: `m5.2xlarge`)
- Docker + Docker Compose installed
- AWS credentials with S3 read/write on your benchmark bucket
- AWS CLI installed

## Setup

### 1. Clone and configure

```bash
git clone https://github.com/markhoerth/gravitino-irc-benchmark.git
cd gravitino-irc-benchmark
cp .env.example .env
# Edit .env — set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET
```

### 2. Upload NYC Taxi 2024 data to S3

Download Yellow Taxi Trip Records for all 12 months of 2024 from:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

We use 2024 specifically because it has a consistent schema across all 12 months.
The loader expects files named `yellow_tripdata_2024-01.parquet` through
`yellow_tripdata_2024-12.parquet`.

```bash
aws s3 sync ~/data/nyc_taxi_2024/ s3://YOUR_BUCKET/raw/nyc_taxi/
```

### 3. Start services

```bash
make up
```

### 4. Load data

```bash
make load-data
```

Reads each Parquet file from S3 and writes it into a proper Iceberg table
via Gravitino IRC (~41M rows, takes about 40 seconds on m5.2xlarge).

### 5. Run benchmark

```bash
make benchmark
```

Results are printed to console and saved to `benchmark_results.json`.

## Other commands

```bash
make trino-shell    # interactive Trino SQL shell
make logs-irc       # tail Gravitino IRC logs
make status         # show service health
make down-clean     # stop and wipe all volumes
```
