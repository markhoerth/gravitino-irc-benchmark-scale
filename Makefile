.PHONY: up down restart logs ps clean reset \
        spark-sql trino-sql psql \
        help

COMPOSE = docker compose

# ============================================================
# Gravitino Quickstart — Makefile
# ============================================================

## Start all services, rebuild changed images, reset postgres volume (standard workflow after git pull)
up:
	@echo "Removing postgres volume for clean init..."
	docker volume rm -f gravitino-quickstart_postgres_data || true
	$(COMPOSE) up -d --build
	@echo "Waiting for postgres to be healthy..."
	@for i in $$(seq 1 30); do \
		if docker inspect gqs-postgres --format '{{.State.Health.Status}}' 2>/dev/null | grep -q healthy; then \
			echo "postgres healthy."; \
			break; \
		fi; \
		if [ $$i -eq 30 ]; then \
			echo "postgres failed to start — restarting..."; \
			$(COMPOSE) restart postgres; \
			sleep 5; \
		fi; \
		sleep 2; \
	done
	@echo ""
	@echo "  Services starting (allow ~5 minutes for full init including Iceberg data load):"
	@echo "    Gravitino  → http://localhost:8090"
	@echo "    Trino      → http://localhost:8082"
	@echo "    CloudBeaver→ http://localhost:8978"
	@echo "    Airflow    → http://localhost:8083"
	@echo "    MinIO      → http://localhost:9002"
	@echo ""
	@echo "  SQL shells (once services are healthy):"
	@echo "    make spark-sql   |   make trino-sql   |   make psql"

## Start services without rebuilding or resetting volumes
up-quick:
	$(COMPOSE) up -d
	@echo "Started without volume reset — use 'make up' for a clean start."

## Wait for init container to complete before connecting
wait-for-init:
	@echo "Waiting for init to complete..."
	@until docker logs gqs-init 2>&1 | grep -q "=== Init complete ==="; do \
		sleep 2; \
	done
	@echo "Init complete."

## Rebuild all custom images (run after any Dockerfile or init script changes)
build:
	./build.sh
	$(COMPOSE) build --no-cache irc gravitino init trino spark-sql

## Stop all containers
down:
	$(COMPOSE) down

## Restart all services
restart:
	$(COMPOSE) restart

## Tail logs for all services
logs:
	$(COMPOSE) logs -f

## Tail logs for a specific service (usage: make logs-svc SVC=gravitino)
logs-svc:
	$(COMPOSE) logs -f $(SVC)

## Show running containers
ps:
	$(COMPOSE) ps

# -------------------------------------------------------
# SQL Shells
# -------------------------------------------------------

## Launch Spark SQL shell (connects through Gravitino postgres_demo catalog)
spark-sql: wait-for-init
	$(COMPOSE) run --rm spark-sql

## Launch Trino CLI (connects through Gravitino catalog)
trino-sql: wait-for-init
	docker exec -it gqs-trino \
	  trino --server http://localhost:8082 \
	        --user admin

## Open psql directly on Postgres
psql:
	docker exec -it gqs-postgres \
	  psql -U postgres -d demo_data

## Open psql as gravitino user
psql-gravitino:
	docker exec -it gqs-postgres \
	  psql -U gravitino -d demo_data

# -------------------------------------------------------
# View test helpers
# -------------------------------------------------------

## Show views in the sales schema
show-views:
	docker exec -it gqs-postgres \
	  psql -U gravitino -d demo_data -c "\dv sales.*"

## Reload the sales schema (drops and recreates tables + views)
reload-views:
	docker exec -i gqs-postgres \
	  psql -U postgres -d demo_data < postgres/init/03-view-test.sql

## Print Trino EXPLAIN for a view through Gravitino (usage: make explain VIEW=v_order_summary)
explain:
	@test -n "$(VIEW)" || (echo "Usage: make explain VIEW=<view_name>" && exit 1)
	docker exec -it gqs-trino \
	  trino --server http://localhost:8082 \
	        --user admin \
	        --execute "EXPLAIN SELECT * FROM gravitino.\"postgres_demo\".\"sales\".$(VIEW)"

# -------------------------------------------------------
# Cleanup
# -------------------------------------------------------

## Remove all containers and named volumes
clean:
	$(COMPOSE) down -v --remove-orphans

## Full reset: clean + prune dangling images
reset: clean
	docker image prune -f

# -------------------------------------------------------
# Help
# -------------------------------------------------------
help:
	@echo ""
	@echo "Gravitino Quickstart"
	@echo "===================="
	@echo ""
	@grep -E '^##' Makefile | sed 's/## /  /'
	@echo ""
	@echo "Examples:"
	@echo "  make up                    # clean start (resets postgres volume)"
	@echo "  make up-quick              # start without resetting volumes"
	@echo "  make spark-sql             # Spark SQL shell via Gravitino"
	@echo "  make trino-sql             # Trino CLI via Gravitino"
	@echo "  make psql                  # psql on demo_data"
	@echo "  make explain VIEW=v_open_orders"
	@echo "  make logs-svc SVC=gravitino"
	@echo ""
