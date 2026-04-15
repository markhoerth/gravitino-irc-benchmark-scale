.PHONY: up down down-clean build load-data benchmark trino-shell logs status

# ── Environment ───────────────────────────────────────────────────────────────
# Copy .env.example to .env and fill in S3_BUCKET, S3_PREFIX, and AWS credentials.
# On EC2 with an IAM instance role, leave AWS_ACCESS_KEY_ID and
# AWS_SECRET_ACCESS_KEY blank — the instance role provides credentials.

up:
	@echo "Starting Gravitino IRC benchmark environment..."
	docker compose up -d
	@echo ""
	@echo "Services:"
	@echo "  IRC:   http://localhost:$$(grep IRC_PORT .env | cut -d= -f2)/iceberg"
	@echo "  Trino: http://localhost:$$(grep TRINO_PORT .env | cut -d= -f2)"
	@echo ""
	@echo "Next steps:"
	@echo "  make load-data   — upload data to S3 and register as Iceberg table"
	@echo "  make benchmark   — run benchmark suite"

down:
	docker compose down

down-clean:
	docker compose down -v --remove-orphans
	docker image rm -f gravitino-irc-benchmark:1.2.0 2>/dev/null || true

build:
	docker compose build

# ── Data Loading ──────────────────────────────────────────────────────────────
# Before running load-data:
#   1. Upload parquet files to S3:
#      aws s3 sync ~/data/nyc_taxi_2024/ s3://${S3_BUCKET}/${S3_PREFIX}/raw/nyc_taxi/
#   2. Then run:
#      make load-data
load-data:
	docker compose exec python python /scripts/load_nyc_taxi.py

# ── Benchmark ─────────────────────────────────────────────────────────────────
benchmark:
	docker compose exec python python /scripts/benchmark.py

# ── Trino shell ───────────────────────────────────────────────────────────────
trino-shell:
	docker compose exec trino trino \
		--server http://localhost:8080 \
		--catalog gravitino_irc \
		--schema nyc_taxi

# ── Logs ──────────────────────────────────────────────────────────────────────
logs:
	docker compose logs -f

logs-irc:
	docker compose logs -f gravitino-irc

logs-trino:
	docker compose logs -f trino

# ── Status ────────────────────────────────────────────────────────────────────
status:
	docker compose ps
