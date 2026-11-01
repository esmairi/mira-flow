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

.PHONY: hooks-install
hooks-install: ## Install the prek git pre-commit hook (run once after clone)
	uv run prek install

.PHONY: hooks
hooks: ## Run all prek hooks against the whole tree (what pre-commit would run)
	uv run prek run --all-files

.PHONY: test
test: ## Run the test suite
	uv run pytest

.PHONY: test-e2e
test-e2e: ## Run the end-to-end API tests against a throwaway Kafka + Postgres (docker compose)
	docker compose -f tests/e2e/compose.yaml run --rm --build tests; \
	status=$$?; docker compose -f tests/e2e/compose.yaml down -v; exit $$status

.PHONY: e2e-up
e2e-up: ## Start only the e2e Kafka + Postgres (host ports 29092/55432), to run tests/e2e from the host
	docker compose -f tests/e2e/compose.yaml up -d --wait kafka postgres

.PHONY: e2e-down
e2e-down: ## Stop and remove the e2e stack
	docker compose -f tests/e2e/compose.yaml down -v

.PHONY: up
up: ## Start Kafka + kafka-ui + the dev container (docker compose)
	docker compose up -d

.PHONY: down
down: ## Stop and remove the docker compose stack (every profile: worker/api/frontend/postgres too)
	docker compose --profile worker --profile worker-postgres --profile postgres down

.PHONY: shell
shell: ## Start the dev container and open a shell in it
	docker compose up -d app
	docker compose exec app bash

.PHONY: worker
worker: ## Run the prod-image worker against sqlite (edit compose.yaml's worker.volumes to point at your own workflows/handlers dirs first)
	docker compose up worker

.PHONY: stack
stack: ## Run worker + api_server + frontend in docker (UI on http://localhost:3000, API on :8001)
	docker compose --profile worker up --build worker api frontend

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
run: ## Start the ledgered orchestrator against workflows/ (needs `make up` + `make init-db` first)
	uv run mira-flow run --workflows-dir workflows --import examples.echo

.PHONY: run-workflow
run-workflow: ## Run one workflow config with no DB ledger (usage: make run-workflow file=path/to/config.json)
	uv run mira-flow run --workflow $(file) --import examples.echo

.PHONY: api-server
api-server: ## Start the API server (http://localhost:8001, docs at /docs)
	uv run uvicorn mira_flow.api_server.app:app --reload --port 8001

.PHONY: clean
clean: ## Remove __pycache__/.pyc and tool caches
	find . -name "__pycache__" -type d -not -path "./.venv/*" -exec rm -rf {} +
	find . -name "*.py[co]" -not -path "./.venv/*" -delete
	rm -rf .pytest_cache .ruff_cache

.PHONY: ui
ui: ## Start the frontend dev server on :5173 (proxies /api to :8001)
	cd frontend && npm install && npm run dev

# Overridable:
#   make local-worker EXAMPLE=retries                    (= examples/<name>/{workflows,handlers})
#   make local-worker WORKFLOWS_DIR=my/workflows HANDLERS_DIR=my/handlers
EXAMPLE ?= order-fulfillment
WORKFLOWS_DIR ?= examples/$(EXAMPLE)/workflows
HANDLERS_DIR ?= examples/$(EXAMPLE)/handlers

.PHONY: local-deps
local-deps: ## Start local deps: Kafka (docker, waits until healthy) + create the ledger tables
	docker compose up -d --wait kafka
	uv run mira-flow init-db

.PHONY: local-worker
local-worker: ## Run the worker locally against WORKFLOWS_DIR (needs `make local-deps`)
	uv run mira-flow run --workflows-dir $(WORKFLOWS_DIR) --handlers-dir $(HANDLERS_DIR)

.PHONY: local-api
local-api: ## Run the api_server locally on :8001 against WORKFLOWS_DIR (needs `make local-deps`; UI: `make ui`)
	WORKFLOWS_DIR=$(WORKFLOWS_DIR) uv run uvicorn mira_flow.api_server.app:app --reload --port 8001
