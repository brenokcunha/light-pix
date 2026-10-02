.PHONY: up down reset ps logs build test test-integration vet fmt load faults

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

# Usa o MongoDB e o RabbitMQ da stack (make up); cada teste cria e apaga o
# próprio banco e o próprio vhost.
LIGHTPIX_TEST_MONGO_URI ?= mongodb://localhost:27017/?directConnection=true
LIGHTPIX_TEST_RABBITMQ_URL ?= amqp://lightpix:lightpix@localhost:5672

test-integration: ## testes com MongoDB e RabbitMQ reais
	LIGHTPIX_TEST_MONGO_URI="$(LIGHTPIX_TEST_MONGO_URI)" LIGHTPIX_TEST_RABBITMQ_URL="$(LIGHTPIX_TEST_RABBITMQ_URL)" go test -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

# Carga de Pix nos dois sentidos com k6 (container). Exemplos:
#   make load RATE=20 DURATION=2m
#   make load FAULTS_SPI='{"errorRate":0.3}' FAULTS_DICT='{"latency":{"mode":"fixed","ms":300}}'
load: ## carga sustentada de Pix, com falhas opcionais nos simuladores
	docker compose --profile load run --rm -e RATE -e DURATION -e FAULTS_SPI -e FAULTS_DICT -e FAULTS_BACEN k6

# Falhas dos simuladores: make faults                 (mostra as atuais)
#                         make faults s=spi j='{"dropPacs002":0.5}'
#                         make faults s=spi j=clear
s ?= spi
faults: ## mostra, define ou limpa as falhas de um simulador (s=spi|dict|bacen)
	@./scripts/faults.sh $(s) '$(j)'
