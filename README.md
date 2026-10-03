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
| psp-a payment-api (ISPB 11111111; também sobem relay, connector e worker) | http://localhost:8081 |
| psp-b payment-api (ISPB 22222222; idem) | http://localhost:8082 |
| resolver do psp-a / psp-b (API administrativa) | http://localhost:8083 / http://localhost:8084 |
| spi-sim | http://localhost:8091 |
| dict-sim | http://localhost:8092 |
| bacen-sim (contas PI começam com R$ 1.000.000,00) | http://localhost:8093 |
| Alloy (pipeline de logs) | http://localhost:12345 |
| MongoDB | `mongodb://localhost:27017/?directConnection=true` |
| OTLP (gRPC / HTTP) | `localhost:4317` / `localhost:4318` |

Do host, o MongoDB exige `directConnection=true`: o replica set anuncia o
nome interno `mongo:27017`, que só resolve dentro da rede do compose.

## Fazer um Pix

Contas de exemplo (saldo inicial R$ 10.000,00): Alice `0001/1000001` e Carol
`0001/1000002` no PSP A; Bob `0001/2000001` e Dave `0001/2000002` no PSP B. Cada
uma tem chaves de e-mail (`alice@example.com`, ...), CPF e celular em
`deploy/sims/dict-seed.json`.

```bash
curl -s -X POST http://localhost:8081/v1/pix/payments \
  -H 'Idempotency-Key: meu-primeiro-pix' -H 'Content-Type: application/json' \
  -d '{"debtorAccount":{"branch":"0001","number":"1000001"},"key":"bob@example.com","amount":25.50}'
```

A resposta traz o `endToEndId`; `GET /v1/pix/payments/{endToEndId}` mostra o
estado (`PENDING` → `SENT` → `CONFIRMED`) e `GET /v1/accounts/0001/1000001` o
saldo. No Grafana, Explore → Tempo → `{span.pix.end_to_end_id="E..."}` abre o
trace do Pix, da API ao worker passando pelos dois PSPs e pelos simuladores.
O mesmo `endToEndId` consultado no PSP B (`localhost:8082`) mostra o recebimento
(`RECEIVED` → `ACCEPTED` → `CREDITED`), e `GET /v1/accounts/0001/2000001` mostra
o saldo de Bob já creditado.

## Falhas e carga

Os simuladores aceitam falhas em tempo de execução (campos e efeitos em
`docs/arquitetura.md`, seção "Injeção de falhas"):

```bash
make faults s=spi j='{"errorRate":0.3,"latency":{"mode":"lognormal","p50Ms":80,"p99Ms":1500}}'
make faults s=spi j='{"dropPacs002":0.5}'     # pacs.002 perdidas: o resolver recupera
make faults s=spi                             # mostra as atuais
make faults s=spi j=clear                     # desliga
```

`make load` roda o k6 (container) com Pix nos dois sentidos e mede, do pedido ao
estado final, a experiência do pagador; as métricas vão ao Prometheus (`k6_*`).
As falhas podem entrar só durante a carga:

```bash
make load RATE=20 DURATION=2m
make load RATE=10 DURATION=1m FAULTS_SPI='{"errorRate":0.2,"dropPacs002":0.1}' FAULTS_DICT='{"latency":{"mode":"fixed","ms":300}}'
```

Os limites do `loadtest/pix.js` são os do Pix (iniciação p95 < 1,5s, estado final
p99 < 10s, nada preso); com falhas ativas eles podem falhar de propósito. Nesta
máquina (8 CPUs, o laboratório inteiro no mesmo Docker) a carga se sustenta em
cerca de 20 Pix/s; acima de ~25 Pix/s a fila cresce, o p99 passa de 10s e o
MongoDB (um nó, transações em todo passo) é o recurso saturado. Depois de uma
carga alta, espere os pagamentos pendentes terminarem antes da próxima rodada.

## Dashboards, SLOs, alertas e runbooks

- **Grafana** (`localhost:3000`): *Pix — visão IGA* (experiência do pagador, iniciação,
  liquidação, PSP × regulador, orçamento de erro, Pix lentos) e *Pix — serviços*.
  Os dashboards são gerados por `make dashboards`; se os arquivos mudarem com o
  Grafana no ar, `docker compose restart grafana` os recarrega.
- **SLOs e alertas**: cinco SLOs por PSP (Sloth, `make slo`) com alerta de página e
  de ticket por burn rate, mais os alertas de causa em
  `deploy/observability/prometheus/rules/pix.yml`. Veja o que está disparando em
  `localhost:9090/alerts` (ou no Alertmanager, `localhost:9093`).
- **Runbooks**: um por alerta, em [docs/runbooks](docs/runbooks/README.md); a anotação
  `runbook` do alerta aponta para ele.
- **IGA**: `curl localhost:8093/api/v1/iga/11111111?window=1h` mostra a visão do
  regulador (P50/P95/P99 e metas) sobre o PSP.

## Cenários de incidente

Cinco incidentes reproduzíveis (vazamento de memória, SPI intermitente, `pacs.002`
perdidas, DICT lento, conta PI sem saldo): cada um aplica carga e falha, confere que
os alertas certos disparam e que o sistema se recupera sozinho.

```bash
make scenarios            # os cinco (alguns minutos cada)
make scenarios n="2 4"    # só alguns
```

Detalhes e alertas esperados em [scenarios/README.md](scenarios/README.md).

## Resolver e reconciliação

O resolver de cada PSP resolve pagamentos parados consultando o SPI, devolve
mensagens das DLQs e concilia a conta PI com o PSP. Para conciliar na hora o dia
de hoje (UTC) no PSP A:

```bash
curl -s -X POST "http://localhost:8083/admin/reconciliation?from=$(date -u +%Y-%m-%dT00:00:00Z)&to=$(date -u -d tomorrow +%Y-%m-%dT00:00:00Z)"
```

O relatório lista as divergências por `EndToEndId`. `GET /admin/reconciliation`
devolve o último relatório e `GET /admin/dead-letters` as mensagens estacionadas.
Para ver o retry em ação, `docker compose stop spi-sim`, faça um Pix, espere uns
segundos e `docker compose start spi-sim`: o Pix fica `PENDING` e se completa
sozinho nas tentativas seguintes.

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
| `DICT_TIMEOUT`, `DICT_ATTEMPTS`, `DICT_CACHE_TTL` | serviços do PSP | timeout por tentativa (800ms), tentativas por consulta (3) e cache de chaves (10s; `0` desliga) |
| `RETRY_DELAYS`, `WORKER_CONCURRENCY` | serviços do PSP | esperas entre as tentativas de uma mensagem que falhou (`5s,30s,2m`; depois vai à DLQ) e mensagens em paralelo (4) |
| `RESOLVER_INTERVAL`, `RESOLVER_MIN_AGE`, `RESOLVER_DLQ_INTERVAL`, `RESOLVER_MAX_REDRIVES`, `RECONCILE_AT` | resolver | período das pendências (10s), tempo parado para consultar o SPI (40s), período das DLQs (1m), voltas pela DLQ antes de estacionar (3), horário UTC da reconciliação diária (`03:00`; vazio desliga) |

## Comandos

| Comando | O que faz |
|---|---|
| `make up` / `make down` | sobe / para a stack |
| `make reset` | para e apaga os volumes |
| `make logs s=psp-a-api` | segue os logs de um serviço |
| `make test` / `make vet` | testes / análise estática |
| `make load` / `make faults` | carga com k6 / falhas dos simuladores |
| `make scenarios` | os cenários de incidente |
| `make slo` / `make dashboards` / `make runbooks` | regenera as regras de SLO, os dashboards e os runbooks |
| `make test-integration` | testes com o MongoDB e o RabbitMQ da stack (`make up` antes) |
