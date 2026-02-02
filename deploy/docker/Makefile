# =============================================================================
# UDPS Docker Deployment Makefile
# =============================================================================
# Simple make targets for common deployment operations.
#
# Usage:
#   make deploy     # Full deployment
#   make quick      # Quick start (skip build if images exist)
#   make status     # Check deployment status
#   make logs       # View live logs
#   make stop       # Stop services
#   make clean      # Remove all data
#   make help       # Show available targets
#
# =============================================================================

.PHONY: help deploy quick setup build start stop restart logs status test clean health shell-api shell-db validate ps pull

# Default target
.DEFAULT_GOAL := help

# Variables
VERSION ?= 0.1.0
IMAGE ?= seri-sa/udps
COMPOSE_FILE := docker-compose.yml

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m

## help: Show this help message
help:
	@echo ""
	@echo "$(BLUE)UDPS Docker Deployment$(NC)"
	@echo ""
	@echo "$(GREEN)Available targets:$(NC)"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## /  /' | column -t -s ':'
	@echo ""
	@echo "$(GREEN)Examples:$(NC)"
	@echo "  make deploy           # Full deployment (first time)"
	@echo "  make quick            # Quick restart"
	@echo "  make status           # Check health"
	@echo ""

## deploy: Full deployment (preflight, build, setup, start)
deploy:
	@./deploy.sh

## quick: Quick start (skip build if images exist)
quick:
	@./deploy.sh --quick

## setup: Run interactive environment setup
setup:
	@./setup.sh

## build: Build UDPS Docker image (sbt assembly + docker build)
build:
	@cd ../.. && sbt "api/assembly" && docker build -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

## start: Start all services
start:
	@docker compose up -d
	@echo ""
	@echo "$(GREEN)Services starting...$(NC)"
	@echo "Run 'make status' to check health"

## stop: Stop all services (preserves data)
stop:
	@docker compose down
	@echo "$(GREEN)Services stopped$(NC)"

## restart: Restart all services
restart: stop start

## logs: View live logs from all services
logs:
	@docker compose logs -f --tail=100

## status: Check deployment status and health
status:
	@./deploy.sh --status

## test: Run deployment validation tests
test:
	@./deploy.sh --test

## clean: Stop services and remove all data
clean:
	@echo "$(YELLOW)This will remove all containers, volumes, and data!$(NC)"
	@read -p "Are you sure? [y/N] " confirm; \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		docker compose down -v --rmi local; \
		echo "$(GREEN)All data removed$(NC)"; \
	else \
		echo "Cancelled"; \
	fi

## health: Quick health check
health:
	@curl -sf http://localhost:8081/health/live && echo " OK" || echo "FAIL"

## ps: Show container status
ps:
	@docker compose ps

## pull: Pull latest base images
pull:
	@docker compose pull

## validate: Validate configuration
validate:
	@./validate-config.sh --env-file .env

## shell-api: Open shell in UDPS API container
shell-api:
	@docker compose exec udps-api /bin/bash 2>/dev/null || docker compose exec udps-api /bin/sh

## shell-db: Open psql in PostgreSQL container
shell-db:
	@docker compose exec postgres psql -U $${POSTGRES_USER:-udps} -d $${POSTGRES_DB:-udps_db}

## logs-api: View UDPS API logs only
logs-api:
	@docker compose logs -f --tail=100 udps-api

## logs-db: View PostgreSQL logs only
logs-db:
	@docker compose logs -f --tail=100 postgres

## teardown: Stop and remove services (default level)
teardown:
	@./teardown.sh

## teardown-full: Stop and remove all data volumes
teardown-full:
	@./teardown.sh --full

## teardown-purge: Complete cleanup (volumes, images, config)
teardown-purge:
	@./teardown.sh --purge
