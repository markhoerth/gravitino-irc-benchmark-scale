#!/usr/bin/env python3
"""
load_nyc_taxi.py — Load NYC Taxi 2023 + 2024 data into an Iceberg table via Gravitino IRC.

Handles schema normalization between years:
  - Renames 2023 'airport_fee' -> 'Airport_fee' to match 2024
  - Casts 2023 columns to match 2024 types

Expected S3 layout:
  s3://BUCKET/raw/nyc_taxi/2023/yellow_tripdata_2023-*.parquet
  s3://BUCKET/raw/nyc_taxi/2024/yellow_tripdata_2024-*.parquet

Upload data:
  aws s3 sync ~/data/nyc_taxi_2023/ s3://YOUR_BUCKET/raw/nyc_taxi/2023/
  aws s3 sync ~/data/nyc_taxi_2024/ s3://YOUR_BUCKET/raw/nyc_taxi/2024/
"""

import os, sys, time, io
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.rest import RestCatalog

IRC_URI    = os.environ["IRC_URI"]
S3_BUCKET  = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
NAMESPACE  = "nyc_taxi"
TABLE_NAME = "yellow_trips"

# Cast 2023 columns to match 2024 schema
CAST_2023 = {
    'VendorID':           pa.int32(),
    'passenger_count':    pa.int64(),
    'RatecodeID':         pa.int64(),
    'store_and_fwd_flag': pa.large_utf8(),
    'PULocationID':       pa.int32(),
    'DOLocationID':       pa.int32(),
}

def list_parquet_keys(bucket, prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return sorted(keys)

def read_parquet_from_s3(bucket, key):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pq.read_table(io.BytesIO(obj["Body"].read()))

def normalize_2023(table):
    """Rename airport_fee -> Airport_fee and cast types to match 2024 schema."""
    # Rename column
    new_names = ['Airport_fee' if c == 'airport_fee' else c for c in table.column_names]
    table = table.rename_columns(new_names)
    # Cast types
    new_fields = []
    for i, field in enumerate(table.schema):
        if field.name in CAST_2023:
            new_fields.append(table.column(i).cast(CAST_2023[field.name]))
        else:
            new_fields.append(table.column(i))
    return pa.table(dict(zip(new_names, new_fields)))

def main():
    print(f"\n{'='*60}")
    print(f"  Gravitino IRC — NYC Taxi Data Loader (2023 + 2024)")
    print(f"{'='*60}")
    print(f"  IRC:   {IRC_URI}")
    print(f"  Table: {NAMESPACE}.{TABLE_NAME}\n")

    print("Connecting to Gravitino IRC...")
    catalog = RestCatalog("gravitino_irc", uri=IRC_URI)
    print("  ✓ Connected")

    print(f"\nCreating namespace '{NAMESPACE}'...")
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"  ✓ Created")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"  ✓ Already exists")
        else:
            raise

    # Collect all files from both years
    all_keys = []
    for year in ["2024", "2023"]:
        prefix = f"raw/nyc_taxi/{year}/"
        keys = list_parquet_keys(S3_BUCKET, prefix)
        print(f"  Found {len(keys)} files for {year}")
        all_keys.extend([(year, k) for k in keys])

    if not all_keys:
        print(f"  ERROR: No files found. Upload data first.")
        sys.exit(1)

    # Use 2024 file to infer schema (canonical)
    print(f"\nInferring schema from 2024 data...")
    first_2024 = next(k for y, k in all_keys if y == "2024")
    first_table = read_parquet_from_s3(S3_BUCKET, first_2024)
    print(f"  ✓ {len(first_table.schema)} columns")

    full_name = f"{NAMESPACE}.{TABLE_NAME}"
    print(f"\nCreating table '{full_name}'...")
    try:
        table = catalog.create_table_if_not_exists(
            identifier=full_name,
            schema=first_table.schema,
        )
        print(f"  ✓ Table ready")
    except Exception as e:
        print(f"  Loading existing table...")
        table = catalog.load_table(full_name)
        print(f"  ✓ Loaded")

    print(f"\nAppending {len(all_keys)} files to Iceberg table...")
    total_rows = 0
    t_total = time.time()

    for i, (year, key) in enumerate(all_keys):
        fname = key.split("/")[-1]
        t0 = time.time()
        arrow_table = read_parquet_from_s3(S3_BUCKET, key)
        if year == "2023":
            arrow_table = normalize_2023(arrow_table)
        table.append(arrow_table)
        elapsed = time.time() - t0
        total_rows += len(arrow_table)
        print(f"  [{i+1:2d}/{len(all_keys)}] {fname} — {len(arrow_table):,} rows in {elapsed:.1f}s")

    print(f"\n  ✓ {total_rows:,} total rows in {time.time()-t_total:.1f}s")
    print(f"\n{'='*60}")
    print(f"  Load complete. Run: make benchmark")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
