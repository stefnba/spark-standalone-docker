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


run-iceberg-sql:
	docker compose -f docker/docker-compose.yml -f docker/iceberg-sql/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans
run-iceberg-rest:
	docker compose -f docker/docker-compose.yml -f docker/iceberg-rest/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans

notebook:
# stark a jupyter notebook server
	docker exec -it spark-master bash -c \
		"cd / \
		&& jupyter notebook \
		--allow-root \
		--no-browser \
		--port=8888 \
		--ServerApp.ip='0.0.0.0' \
		--IdentityProvider.token='${JUPYTER_TOKEN}' \
		--notebook-dir=/opt/workspace \
		"