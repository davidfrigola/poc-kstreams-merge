
default:
	@echo "Use options infra-start|infra-stop"

infra-start:
	docker-compose -f docker/docker-compose.yaml up -d

infra-stop:
	docker-compose -f docker/docker-compose.yaml down
