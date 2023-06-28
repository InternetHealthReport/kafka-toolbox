.PHONY: up, down, logs, build

up:
	docker-compose up -d
	@make logs

down:
	docker-compose down

logs:
	docker-compose logs -f rrc00

build:
	docker build --tag mstfh/bgp .

dive:
	@dive mstfh/bgp
