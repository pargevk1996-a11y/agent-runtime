# agent-runtime — developer entrypoints.
# Every target shells through `uv run` so contributors need only `uv` installed;
# the toolchain versions are pinned by uv.lock, identical to CI.

.PHONY: help install lint fmt fmt-check typecheck test test-integration up down

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Sync all workspace packages + dev tools
	uv sync --all-packages

lint: ## Static lint (ruff check)
	uv run ruff check .

fmt: ## Auto-format (ruff format)
	uv run ruff format .

fmt-check: ## Verify formatting without writing (CI)
	uv run ruff format --check .

typecheck: ## Strict type check (mypy)
	uv run mypy .

test: ## Fast unit tests only
	uv run pytest -m "not integration"

test-integration: ## Integration tests (real Postgres/Redis via testcontainers)
	uv run pytest -m integration

up: ## Start local infra (Postgres, Redis, OTEL, Prometheus)
	docker compose up -d

down: ## Stop local infra and drop volumes
	docker compose down -v
