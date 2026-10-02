# light-pix

Laboratório local de um PSP Pix (envio e recebimento) com simuladores de DICT,
SPI e Bacen e observabilidade open source. Arquitetura e fases em
[docs/arquitetura.md](docs/arquitetura.md).

## Pré-requisitos

- Docker com Compose v2
- Go 1.26 (o toolchain é baixado automaticamente pelo `go` 1.21+)
- `make` (opcional)

## Subir

```bash
make up
```

Primeira subida baixa as imagens e inicializa o replica set do MongoDB.

| Serviço | URL |
|---|---|
| Grafana (anônimo vê dashboards; Explore com admin / lightpix) | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Alertmanager | http://localhost:9093 |
| RabbitMQ (lightpix / lightpix) | http://localhost:15672 |
| psp-a payment-api (ISPB 11111111) | http://localhost:8081 |
| psp-b payment-api (ISPB 22222222) | http://localhost:8082 |
| spi-sim | http://localhost:8091 |
| dict-sim | http://localhost:8092 |
| bacen-sim (contas PI começam com R$ 1.000.000,00) | http://localhost:8093 |
| Alloy (pipeline de logs) | http://localhost:12345 |
| MongoDB | `mongodb://localhost:27017/?directConnection=true` |
| OTLP (gRPC / HTTP) | `localhost:4317` / `localhost:4318` |

Do host, o MongoDB exige `directConnection=true`: o replica set anuncia o
nome interno `mongo:27017`, que só resolve dentro da rede do compose.

## Conferir os sinais

```bash
curl http://localhost:8081/readyz
```

- **Métricas:** no Grafana, Explore → Prometheus →
  `http_server_request_duration_seconds_bucket{psp_ispb="11111111"}`.
- **Traces:** Explore → Tempo → `{resource.service.name="payment-api"}`;
  para os simuladores, `{span.pix.end_to_end_id="E..."}` mostra spi-sim e
  bacen-sim no mesmo trace.
- **Logs:** Explore → Loki → `{service_name="psp-a-api"}`; o `trace_id`
  (structured metadata) abre o trace correspondente.

## Configuração útil

| Variável | Onde | Efeito |
|---|---|---|
| `TRACE_SAMPLE_PERCENT` | `.env` ou shell | % de traces normais guardados pelo tail sampling (padrão 100; erros e lentos são sempre guardados) |
| `LOG_LEVEL` | serviços Go | `debug`, `info`, `warn`, `error` |
| `SPI_*`, `DICT_*`, `BACEN_PI_ACCOUNTS` | `compose.yaml` (simuladores) | prazo do recebedor, long-poll e lease do SPI; rate limit do DICT; saldos iniciais das contas PI |

## Comandos

| Comando | O que faz |
|---|---|
| `make up` / `make down` | sobe / para a stack |
| `make reset` | para e apaga os volumes |
| `make logs s=psp-a-api` | segue os logs de um serviço |
| `make test` / `make vet` | testes / análise estática |
| `make test-integration` | testes com o MongoDB da stack (`make up` antes) |
