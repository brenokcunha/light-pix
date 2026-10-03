.PHONY: up down reset ps logs build test test-integration vet fmt load faults slo dashboards runbooks scenarios k8s-tools k8s-sync k8s-up k8s-down k8s-status

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
	docker compose --profile load run --rm -e RATE -e DURATION -e DIRECTION -e FAULTS_SPI -e FAULTS_DICT -e FAULTS_BACEN k6

# Falhas dos simuladores: make faults                 (mostra as atuais)
#                         make faults s=spi j='{"dropPacs002":0.5}'
#                         make faults s=spi j=clear
s ?= spi
faults: ## mostra, define ou limpa as falhas de um simulador (s=spi|dict|bacen)
	@./scripts/faults.sh $(s) '$(j)'

# Artefatos gerados (os JSON/YAML/Markdown gerados ficam versionados).
SLOTH_IMAGE ?= ghcr.io/slok/sloth@sha256:ad651f2de49307f5e5f971be82927ef99ac27bd67df6aa6aff713957ede5593f
WORKDIR := $(shell pwd -W 2>/dev/null || pwd)

slo: ## gera as especificações de SLO e as regras do Prometheus (Sloth, via container)
	python scripts/gen_slo.py
	MSYS_NO_PATHCONV=1 docker run --rm -v "$(WORKDIR):/work" $(SLOTH_IMAGE) generate -i /work/deploy/slo/specs -o /work/deploy/observability/prometheus/rules --slo-period-windows-path /work/deploy/slo/windows

dashboards: ## gera os dashboards do Grafana
	python scripts/gen_dashboards.py

runbooks: ## gera docs/runbooks
	python scripts/gen_runbooks.py

# Cenários de incidente (cada um leva alguns minutos): make scenarios n="2 4"
scenarios: ## reproduz os incidentes e confere os alertas (n="1 2" escolhe)
	./scenarios/run.sh $(n)

# Kubernetes (kind). Pare a stack do Compose antes: o cluster usa as mesmas portas.
k8s-tools: ## baixa kind e helm para .tools/
	./scripts/k8s_tools.sh

k8s-sync: ## copia dashboards, regras e configs para o chart Helm
	python scripts/k8s_sync.py

k8s-up: ## cria o cluster kind e instala metrics-server, kube-prometheus-stack e o chart light-pix
	./scripts/k8s_up.sh

k8s-down: export PATH := $(CURDIR)/.tools:$(PATH)
k8s-down: ## apaga o cluster kind
	kind delete cluster --name lightpix

k8s-status: ## pods, HPA e alertas do cluster
	kubectl -n lightpix get pods,hpa
	kubectl -n monitoring get pods
