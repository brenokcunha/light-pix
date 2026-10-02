.PHONY: up down reset ps logs build test vet fmt

up: ## sobe a stack completa (rebuild das imagens Go)
	docker compose up -d --build

down: ## para a stack, mantendo os volumes
	docker compose down

reset: ## para a stack e apaga os volumes (dados de Mongo, RabbitMQ e telemetria)
	docker compose down -v

ps:
	docker compose ps

logs: ## make logs s=psp-a-api
	docker compose logs -f $(s)

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
