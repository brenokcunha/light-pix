.PHONY: up down reset ps logs build test test-integration vet fmt

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

# Usa o MongoDB da stack (make up); cada teste cria e apaga o próprio banco.
LIGHTPIX_TEST_MONGO_URI ?= mongodb://localhost:27017/?directConnection=true

test-integration: ## testes com MongoDB real
	LIGHTPIX_TEST_MONGO_URI="$(LIGHTPIX_TEST_MONGO_URI)" go test -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .
