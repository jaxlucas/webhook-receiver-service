.PHONY: venv test test-all lint format promtool smoke up down obs load

PROM_IMAGE ?= prom/prometheus:v3.5.0
IN_DOCKER := scripts/in-docker.sh

# Local development with uv (Python 3.14).
venv:
	uv sync --group dev

test:
	uv run pytest -m "not slow"

test-all:
	uv run pytest

lint:
	uv run ruff check src tests
	uv run ruff format --check src tests

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

# Same targets without a local Python 3.14: run inside python:3.14-slim.
docker-venv:
	$(IN_DOCKER) sh -c "python -m venv .venv && pip install -q -e . --group dev"

docker-test:
	$(IN_DOCKER) pytest

docker-lint:
	$(IN_DOCKER) sh -c "ruff check src tests && ruff format --check src tests"

promtool:
	docker run --rm -v $(PWD)/deploy/prometheus:/p -w /p --entrypoint promtool $(PROM_IMAGE) check rules alerts.yml
	docker run --rm -v $(PWD)/deploy/prometheus:/p -w /p --entrypoint promtool $(PROM_IMAGE) test rules alerts_test.yml
	sed 's|/etc/prometheus/alerts.yml|alerts.yml|' deploy/prometheus/prometheus.yml > deploy/prometheus/.check.yml
	docker run --rm -v $(PWD)/deploy/prometheus:/p -w /p --entrypoint promtool $(PROM_IMAGE) check config .check.yml; \
		status=$$?; rm -f deploy/prometheus/.check.yml; exit $$status

up:
	docker compose up -d --build --wait

obs:
	docker compose --profile obs up -d --build --wait

down:
	docker compose --profile obs down

smoke:
	scripts/smoke.sh

load:
	scripts/load.sh
