.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

.PHONY: sync
sync: ## Install/update the whole workspace (all packages, incl. dev deps)
	uv sync --all-packages --group dev

.PHONY: lint
lint: ## Lint with ruff
	uv run ruff check .

.PHONY: test
test: ## Run the test suite
	uv run pytest

.PHONY: up
up: ## Start Kafka + kafka-ui + the dev container (docker compose)
	docker compose up -d

.PHONY: down
down: ## Stop and remove the docker compose stack
	docker compose down

.PHONY: shell
shell: ## Start the dev container and open a shell in it
	docker compose up -d app
	docker compose exec app bash

.PHONY: worker
worker: ## Run the prod-image worker against sqlite (edit compose.yaml's worker.volumes to point at your own workflows/handlers dirs first)
	docker compose up worker

.PHONY: worker-postgres
worker-postgres: ## Same as `make worker`, against the postgres service instead of sqlite
	docker compose --profile worker-postgres up worker-postgres

.PHONY: init-db
init-db: ## Create the workflow_run / task_run tables (quick dev bootstrap, no migration history — see `make migrate`)
	uv run mira-flow init-db

.PHONY: migrate
migrate: ## Apply Alembic migrations to DATABASE_URL (the real, production-safe path)
	uv run alembic -c packages/ledger/alembic.ini upgrade head

.PHONY: migration
migration: ## Autogenerate a new migration from model changes (usage: make migration name="add foo column")
	uv run alembic -c packages/ledger/alembic.ini revision --autogenerate -m "$(name)"

.PHONY: run
run: ## Start the orchestrator against workflows/ (needs `make up` + `make init-db` first)
	uv run mira-flow run --workflows-dir workflows --import examples.echo

.PHONY: api-server
api-server: ## Start the API server (http://localhost:8001, docs at /docs)
	uv run uvicorn mira_flow.api_server.app:app --reload --port 8001

.PHONY: clean
clean: ## Remove __pycache__/.pyc and tool caches
	find . -name "__pycache__" -type d -not -path "./.venv/*" -exec rm -rf {} +
	find . -name "*.py[co]" -not -path "./.venv/*" -delete
	rm -rf .pytest_cache .ruff_cache
