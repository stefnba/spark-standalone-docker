#!make

include .env

down:
	docker-compose down --volumes --remove-orphans

run:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans

run-scaled:
	docker compose -f docker/docker-compose.yml --project-directory . up -d --build --force-recreate --remove-orphans --scale spark-worker=3

submit:
	docker exec spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/$(app)