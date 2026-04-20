ifneq (,$(wildcard .env))
    include .env
    export
endif

# Prefer Docker Compose V2 (`docker compose`); fall back to the legacy
# standalone `docker-compose` (V1) still shipped on some older installs.
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: init kafka-up kafka-down run-producer run-consumer test-unit test-integration dbt-compile terraform-plan

init:
	pip install -r requirements.txt

kafka-up:
	$(DOCKER_COMPOSE) up -d

kafka-down:
	$(DOCKER_COMPOSE) down

run-producer:
	python -m ingestion.producer

run-consumer:
	JAVA_TOOL_OPTIONS="--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED" python -m streaming.spark_consumer

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

dbt-compile:
	cd dbt_project && dbt compile

terraform-plan:
	cd terraform && terraform plan
