# zv-monitor / CDC pipeline lifecycle.
#
# This Makefile is a thin wrapper: every target just calls the matching
# script in scripts/, where all the logic lives. The scripts are also
# usable directly from scripts/.
#
#   make up            full stack in docker (incl. the zv-monitor container)
#   make up-dev        dev stack: no zv-monitor container, run it on your host
#   make connectors    (re-)register the connectors from development/kafka-connect/
#   make urls          print all exposed service URLs
#   make logs          follow service logs (make logs SERVICES="kafka kafka-connect")
#   make simulate      continuous INSERT/UPDATE/DELETE traffic on the source (Ctrl+C)
#   make track         zv-debezium branch comparison (FEATURE=... RELEASE=...)
#   make down          stop (make down ARGS=-v also wipes the volumes)
#   make clean-postgres wipe the Postgres data volumes and re-init both DBs
#   make clean-kafka    wipe Kafka data (topics, connector state) + restart broker/worker

SHELL := /bin/bash
.DEFAULT_GOAL := help
MAKEFLAGS += --no-print-directory

.PHONY: help up up-dev connectors urls logs simulate track down clean-postgres clean-kafka

help: ## show this help
	@echo 'zv-monitor pipeline - targets (make <target>):'
	@echo
	@grep -E '^[a-zA-Z0-9_-]+:.*## ' $(firstword $(MAKEFILE_LIST)) \
	  | awk -F':.*## ' '{ printf "  %-14s %s\n", $$1, $$2 }'

up: ## start the full stack (containers incl. zv-monitor)
	@./scripts/pipeline-up.sh

up-dev: ## start the dev stack (no zv-monitor container)
	@./scripts/pipeline-up-dev.sh

connectors: ## (re-)register the connectors, wait for RUNNING
	@./scripts/register-connectors.sh

urls: ## print the exposed service URLs
	@./scripts/print-urls.sh

logs: ## follow service logs (make logs SERVICES="kafka kafka-connect" to pick)
	@test -n "$$(docker ps -q --filter label=com.docker.compose.project=development)" \
	  || { echo 'ERROR: no pipeline services running - start with: make up (or make up-dev)' >&2; exit 1; }
	@services='$(SERVICES)'; \
	test -n "$$services" || services='kafka kafka-connect postgres-source postgres-sink'; \
	echo "== Following logs (Ctrl+C to stop): $$services"; \
	docker compose -f development/docker-compose.yml logs -f --tail=50 $$services

simulate: ## continuous CDC traffic generator (Ctrl+C to stop)
	@./scripts/simulate-changes.sh

track: ## zv-debezium branch comparison
	@./scripts/zv-debezium-track.sh

down: ## stop the stack (make down ARGS=-v also wipes the volumes)
	@./scripts/pipeline-down.sh $(ARGS)

clean-postgres: ## wipe the Postgres data volumes, re-init both DBs (stack keeps running)
	@echo '== Wiping Postgres data volumes (fresh init from postgres/*-init) =='
	@docker compose -f development/docker-compose.yml rm -sfv postgres-source postgres-sink
	@docker compose -f development/docker-compose.yml up -d --wait postgres-source postgres-sink
	@echo 'Done. Note: CDC offsets live in Kafka - run `make clean-kafka connectors`'
	@echo 'afterwards if the source connector should re-snapshot the fresh source.'

clean-kafka: ## wipe Kafka data (topics, connector state) and restart broker + worker
	@echo '== Wiping Kafka data volumes (topics + connector configs/offsets) =='
	@docker compose -f development/docker-compose.yml rm -sfv kafka kafka-connect
	@docker compose -f development/docker-compose.yml up -d kafka kafka-connect
	@echo 'Done. Connectors are gone (their configs lived in Kafka) - re-register with:'
	@echo '  make connectors'
