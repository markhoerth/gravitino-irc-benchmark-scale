.PHONY: up down down-clean build load-data populate benchmark trino-shell logs status

up:
	@echo "Starting Gravitino IRC scale benchmark environment..."
	docker compose up -d
	@echo ""
	@echo "Services:"
	@echo "  IRC:      http://localhost:$$(grep IRC_PORT .env | cut -d= -f2)/iceberg"
	@echo "  Trino:    http://localhost:$$(grep TRINO_PORT .env | cut -d= -f2)"
	@echo "  Postgres: localhost:$$(grep POSTGRES_PORT .env | cut -d= -f2)"
	@echo ""
	@echo "Next steps:"
	@echo "  make load-data   — load 2023 + 2024 NYC taxi data (~80M rows)"
	@echo "  make populate    — create 1,000 namespaces / 10,000 tables"
	@echo "  make benchmark   — run full benchmark suite"

down:
	docker compose down

down-clean:
	docker compose down -v --remove-orphans
	docker image rm -f gravitino-irc-scale:1.2.0 2>/dev/null || true

build:
	docker compose build

# ── Data Loading ──────────────────────────────────────────────────────────────
# Before running load-data, upload both years to S3:
#   aws s3 sync ~/data/nyc_taxi_2023/ s3://${S3_BUCKET}/raw/nyc_taxi/2023/
#   aws s3 sync ~/data/nyc_taxi_2024/ s3://${S3_BUCKET}/raw/nyc_taxi/2024/
load-data:
	docker compose exec python python /scripts/load_nyc_taxi.py

# ── Catalog Population ────────────────────────────────────────────────────────
# Creates 1,000 namespaces with 10 tables each (10,000 tables total).
# Run after load-data to simulate enterprise-scale catalog.
populate:
	docker compose exec python python /scripts/populate_catalog.py

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
