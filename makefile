#!make

include .env

# Python setup with pipenv
ifeq ($(PY_VERSION),)
PY_VERSION := 3.11
endif

setup:
	mkdir ./.venv &&  \
	pipenv --python ${PY_VERSION} && \
	source .venv/bin/activate && \
	pip install --upgrade pip
install-dev:
	pipenv install --dev
packages-lock:
	pipenv lock
install-prod:
	pipenv install --ignore-pipfile --deploy

down:
	docker-compose down --volumes --remove-orphans

run:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans

run-scaled:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans --scale spark-worker=3

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)