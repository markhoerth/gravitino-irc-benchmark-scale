#!/usr/bin/env python3
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
RAW_PREFIX = "raw/nyc_taxi/"

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

def main():
    print(f"\n{'='*60}")
    print(f"  Gravitino IRC — NYC Taxi Data Loader")
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

    print(f"\nListing Parquet files...")
    keys = list_parquet_keys(S3_BUCKET, RAW_PREFIX)
    if not keys:
        print(f"  ERROR: No files found at s3://{S3_BUCKET}/{RAW_PREFIX}")
        sys.exit(1)
    print(f"  ✓ Found {len(keys)} files")

    print(f"\nInferring schema from first file...")
    first_table = read_parquet_from_s3(S3_BUCKET, keys[0])
    print(f"  ✓ {len(first_table.schema)} columns, {len(first_table):,} rows")

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

    print(f"\nAppending files to Iceberg table...")
    total_rows = 0
    t_total = time.time()
    for i, key in enumerate(keys):
        fname = key.split("/")[-1]
        t0 = time.time()
        arrow_table = read_parquet_from_s3(S3_BUCKET, key)
        table.append(arrow_table)
        elapsed = time.time() - t0
        total_rows += len(arrow_table)
        print(f"  [{i+1:2d}/{len(keys)}] {fname} — {len(arrow_table):,} rows in {elapsed:.1f}s")

    print(f"\n  ✓ {total_rows:,} total rows in {time.time()-t_total:.1f}s")
    print(f"\n{'='*60}")
    print(f"  Load complete. Run: make benchmark")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
