# light-pix — Arquitetura de referência

Laboratório local e didático de um PSP Pix (envio e recebimento), com simuladores
de DICT, SPI e Bacen, resolvedor de pendências e stack de observabilidade 100%
open source. Não é a arquitetura de nenhuma instituição real; números e regras
são aproximações para estudo.

## 1. Objetivos

- Exercitar os padrões que sustentam um Pix confiável: idempotência por
  `EndToEndId`, outbox transacional, entrega at-least-once com consumidores
  idempotentes, retry com backoff, DLQ, circuit breaker e reconciliação.
- Medir o fluxo com SLIs alinhados ao IGA (P50/P99 da experiência do pagador,
  tempo de iniciação, disponibilidade) e alertar por burn rate.
- Reproduzir incidentes (ex.: mini-case de `OOMKilled` no SPI connector) com
  injeção de falhas e carga sintética.
- Tudo sobe com `docker compose` numa máquina de desenvolvimento.

## 2. Stack

| Camada | Escolha |
|---|---|
| Linguagem | Go 1.26 (`net/http`, `log/slog`) |
| Armazenamento | MongoDB 8 em replica set de 1 nó (transações e change streams) |
| Mensageria | RabbitMQ 4 (quorum queues, publisher confirms, acks manuais) |
| Resiliência | `sony/gobreaker` (circuit breaker), retry com backoff e jitter próprio |
| Telemetria | OpenTelemetry SDK (traces + métricas), logs JSON com `trace_id` |
| Coleta | OpenTelemetry Collector (tail sampling), Grafana Alloy (logs) |
| Backends | Prometheus + Alertmanager, Tempo, Loki |
| Visualização | Grafana (datasources e dashboards provisionados) |
| SLO | Sloth (gera recording rules e alertas multi-window burn rate) |
| Carga | k6 (via container) |

## 3. Visão geral

Dois PSPs simulados (`psp-a`, ISPB `11111111`, e `psp-b`, ISPB `22222222`) rodam
o mesmo código. Um Pix enviado por A é recebido por B, então os dois fluxos são
exercitados com código real, sem PSP falso do outro lado.

```
                 ┌──────────────────────── PSP (A ou B) ─────────────────────────┐
 cliente ──HTTP──► payment-api ──► MongoDB (accounts, payments, outbox)          │
                 │     │                     │ change stream                     │
                 │     │ DICT (cache+CB)     ▼                                   │
                 │     │              outbox-relay ──► RabbitMQ                  │
                 │     │                                │                        │
                 │     │        ┌───────────────────────┼──────────────┐         │
                 │     │        ▼                       ▼              ▼         │
                 │     │  spi-connector          result-worker   incoming-worker │
                 │     │   (out + long-poll in)       │              │           │
                 │     │        │                     └──► MongoDB ◄─┘           │
                 │     │        │                pending-resolver (cron) ──► SPI │
                 └─────┼────────┼────────────────────────────────────────────────┘
                       ▼        ▼
                   dict-sim   spi-sim ◄──► bacen-sim (conta PI / STR)
```

Binário único `psp` com subcomandos (`api`, `relay`, `connector`, `worker`,
`resolver`): uma imagem, réplicas escaláveis por papel.

## 4. Componentes do PSP

**payment-api** — recebe a ordem (`POST /v1/pix/payments`) com header
`Idempotency-Key`, gera o `EndToEndId`, resolve a chave no DICT (via gateway com
cache curto, timeout por `context`, retry limitado e circuit breaker) e, numa
única transação MongoDB: reserva o saldo (`available -= v`, `held += v`, com
condição `available >= v`), grava o pagamento `PENDING` e o evento de outbox.
Também expõe consulta de pagamento, contas e gestão de chaves (proxy ao DICT).

**outbox-relay** — escuta o change stream da coleção `outbox`, publica no
RabbitMQ com publisher confirms, marca `publishedAt` e persiste o resume token.
Copia o `traceparent` gravado no evento para os headers AMQP. Entrega
at-least-once: duplicatas são absorvidas pela idempotência dos consumidores.

**spi-connector** — consome `spi.out`, monta a mensagem (JSON com a semântica
e os nomes de campo do ISO 20022), "assina" (stub) e envia ao SPI. Em paralelo, faz long-polling do stream de
entrada do SPI e roteia: `pacs.008` recebida → `pix.incoming`; `pacs.002` →
`pix.results`. Controla timeout e reenvio sem duplicar (o SPI também deduplica
por `EndToEndId`).

**result-worker** — consome `pix.results` e fecha o pagamento enviado:
`ACSC` → `CONFIRMED` (baixa `held`); `RJCT` → `REJECTED` (devolve `held` para
`available`). Notifica o cliente (webhook/log).

**incoming-worker** — consome `pix.incoming`: valida conta de destino e grava o
recebimento `RECEIVED`; responde `pacs.002` `ACSP` (aceite) ou `RJCT` (ex.:
conta inexistente/bloqueada) via outbox. Na confirmação de liquidação (`ACSC`)
credita a conta (`CREDITED`). Tudo idempotente por `EndToEndId`.

**pending-resolver** — job periódico que busca pagamentos/recebimentos em estado
não terminal além de um limiar (ex.: `PENDING`/`SENT` > 40s, `ACCEPTED` sem
liquidação), consulta o status no SPI e aplica o resultado pelo mesmo caminho do
result-worker. Também reprocessa a DLQ com segurança (idempotência) e roda a
reconciliação diária: compara o extrato da conta PI (bacen-sim) com o ledger
interno e registra divergências (coleção `reconciliation_reports` + métrica e
alerta).

## 5. Simuladores

**spi-sim** — roteador entre participantes, inspirado na interface ICOM:
- `POST /api/v1/in/{ispb}/msgs`: PSP envia `pacs.008`/`pacs.002`.
- `GET /api/v1/out/{ispb}/stream`: long-polling das mensagens destinadas ao PSP.
- `GET /api/v1/status/{endToEndId}`: consulta de situação (simplificação do `pacs.028`).
- Liquidação contra a conta PI de cada participante (saldo mantido pelo bacen-sim):
  saldo insuficiente → `RJCT AM04`; recebedor sem resposta no prazo → `RJCT`
  para o pagador.
- Deduplicação por `EndToEndId`.

**dict-sim** — diretório de chaves:
- CRUD de vínculos (CPF, CNPJ, e-mail, celular, EVP) por participante.
- `GET /api/v1/entries/{key}` com header do pagador e rate limit por token bucket
  (anti-varredura, como no DICT real).
- (fase MED) relatos de infração e pedidos de devolução.

**bacen-sim** — reservas do participante e visão do regulador:
- Conta PI/STR: saldo da conta PI por ISPB, aporte/retirada (transferência
  STR ↔ PI) e extrato de lançamentos de liquidação, usado na reconciliação diária.
- IGA: recebe os timestamps por transação (o SPI envia os seus; cada PSP envia
  periodicamente `t0'`, `t6a` e equivalentes do recebedor em
  `POST /api/v1/iga/reports`), calcula os indicadores por janela
  (`GET /api/v1/iga/{ispb}`) e os expõe como métricas. Permite comparar o SLO
  interno do PSP com o que o regulador enxerga.

**Injeção de falhas** — todos os simuladores expõem `/admin/faults` para
configurar latência (fixa ou distribuição), taxa de erro HTTP, perda de
`pacs.002`, timeout do recebedor e indisponibilidade total.

## 6. Fluxos

### 6.1 Pix enviado (A → B)

1. Cliente chama `POST /v1/pix/payments` em A (`t0`).
2. A resolve a chave no DICT → conta em B.
3. Transação Mongo: reserva + pagamento `PENDING` + outbox `pacs.008`.
4. Relay publica em `spi.out`; connector envia ao SPI → pagamento `SENT`.
5. SPI encaminha a `pacs.008` para B (stream de B).
6. B valida, grava `RECEIVED`, responde `pacs.002 ACSP`.
7. SPI liquida (conta PI A → B) e envia `pacs.002 ACSC` para A e B.
8. A: `CONFIRMED`, baixa `held`, notifica o pagador (`t6a`).
   B: `CREDITED`, credita o recebedor.

### 6.2 Falhas tratadas

| Falha | Tratamento |
|---|---|
| Mensagem duplicada (relay/rabbit) | escrita condicional por `EndToEndId`; consumidor descarta |
| SPI fora / 5xx | retry com backoff via filas TTL; esgotou → DLQ + alerta |
| `pacs.002` perdida | pending-resolver consulta status no SPI |
| DICT lento/fora | timeout + circuit breaker; falha rápida para o cliente |
| Saldo insuficiente | rejeição síncrona na API (transação não comita) |
| Crash entre gravar e publicar | outbox + resume token garantem a publicação |

## 7. Estados

- **Enviado:** `PENDING` → `SENT` → `CONFIRMED` | `REJECTED`
- **Recebido:** `RECEIVED` → `ACCEPTED` → `CREDITED` | `REJECTED`

Transições só avançam (escrita condicional sobre o estado atual), então
reprocessar um evento antigo nunca regride um pagamento.

## 8. Modelo de dados (MongoDB, um database por PSP)

| Coleção | Conteúdo | Índices |
|---|---|---|
| `accounts` | agência/conta, titular, `balance.available`, `balance.held`, status | único (agência, conta) |
| `ledger_entries` | lançamentos append-only (débito, crédito, reserva, estorno) | (conta, data) |
| `payments` | `_id = EndToEndId`, direção, estado, valor, partes, histórico, timestamps do IGA, `traceparent` | estado + `updatedAt` (resolver) |
| `idempotency_keys` | (cliente, `Idempotency-Key`) → `EndToEndId` | único, TTL 24h |
| `outbox` | evento, payload, `traceparent`, `createdAt`, `publishedAt` | `publishedAt` |
| `relay_checkpoints` | resume token do change stream | — |

## 9. Mensageria (RabbitMQ, um vhost por PSP)

- Exchange `pix` (topic) e `pix.dlx` (dead-letter).
- Quorum queues: `spi.out`, `pix.results`, `pix.incoming`, cada uma com
  `x-delivery-limit` e DLQ (`*.dlq`).
- Retry: filas `*.retry.{5s,30s,2m}` com TTL que devolvem para a fila de origem.
- Headers: `traceparent`, `x-enqueued-at` (mede a idade da mensagem), `x-attempt`.

## 10. Observabilidade

- **Traces:** OTel SDK em todos os serviços (incluindo os simuladores), contexto
  propagado por HTTP e pelo par outbox → AMQP (link entre produtor e consumidor).
  Collector com tail sampling: 100% dos erros e lentos, amostra dos demais.
  Destino: Tempo.
- **Métricas:** histogramas com buckets nos limites do IGA (0,9s, 1,5s, 6s, 10s),
  exportados via OTLP → Collector → Prometheus (remote write). `EndToEndId`
  nunca vira label. Também: `rabbitmq_prometheus`, `mongodb_exporter`, cAdvisor.
- **Logs:** `slog` JSON com `trace_id`/`span_id` e mascaramento de CPF e chave
  Pix; Alloy coleta do Docker e envia ao Loki.
- **SLOs (Sloth):** experiência do pagador (P99 < 10s), iniciação (P95 < 1,5s),
  disponibilidade da API, taxa de sucesso. Alertas multi-window burn rate, cada
  um com link para runbook em `docs/runbooks/`.
- **Alertas de causa:** idade/profundidade de fila, crescimento de DLQ, restarts
  e `OOMKilled`, circuit breaker aberto, conexões ao Mongo.
- **Grafana:** dashboard "Pix — visão IGA", dashboard por serviço, anotações de
  deploy, correlação trace ↔ logs ↔ métricas (exemplars).

## 11. Cenários de falha (`scenarios/`)

Scripts que combinam carga (k6) com injeção de falhas, cada um com o que
observar e o runbook esperado:

1. Mini-case: leak de memória no connector (flag) + `mem_limit` → `OOMKilled`,
   fila `spi.out` crescendo.
2. SPI intermitente (30% de 5xx) → retries, DLQ, reprocessamento.
3. `pacs.002` perdidas → pendências resolvidas pelo resolver.
4. DICT lento → circuit breaker abre, falha rápida.
5. Conta PI sem saldo → rejeições `AM04`, alerta de liquidez.

## 12. Estrutura do repositório

```
cmd/psp            subcomandos api | relay | connector | worker | resolver
cmd/spi-sim
cmd/dict-sim
cmd/bacen-sim
internal/pix       domínio: EndToEndId, estados, regras
internal/store     MongoDB (repos, transações, change streams)
internal/broker    RabbitMQ (topologia, publisher confirms, consumidor)
internal/iso20022  pacs.008 / pacs.002 simplificados (JSON)
internal/clients   DICT, SPI, bacen (com retry e circuit breaker)
internal/telemetry OTel, slog, mascaramento
internal/httpx     servidor HTTP instrumentado, health checks, shutdown gracioso
internal/config    leitura de variáveis de ambiente
compose.yaml       stack local (raiz do repositório)
deploy/rabbitmq    vhosts, usuário e plugins
deploy/observability  otelcol, prometheus, alertmanager, tempo, loki, alloy, grafana
deploy/slo         especificações Sloth e regras geradas
loadtest           scripts k6
scenarios          scripts de incidentes
docs               arquitetura, ADRs, runbooks
```

## 13. Fases

| Fase | Entrega | Pronto quando |
|---|---|---|
| F0 | Esqueleto: módulo Go, compose com Mongo RS, RabbitMQ e stack de observabilidade, pacote de telemetria | `docker compose up` sobe tudo, Grafana com datasources |
| F1 | Simuladores DICT, SPI e Bacen (conta PI/STR; sem falhas, sem IGA) | testes de integração dos simuladores passam |
| F2 | Pix enviado ponta a ponta (api, relay, connector, result-worker) | Pix A → B confirmado, trace único ponta a ponta |
| F3 | Pix recebido (incoming-worker, crédito na liquidação) | saldos de A, B e contas PI batem |
| F4 | Retry/DLQ, pending-resolver (estados, DLQ, reconciliação diária) | `pacs.002` perdida e SPI com 5xx se resolvem sozinhos; reconciliação sem divergência |
| F5 | Injeção de falhas nos simuladores + scripts k6 | carga sustentada com falhas configuráveis |
| F6 | Dashboards, SLOs Sloth, alertas, runbooks, IGA no bacen-sim, cenários | os 5 cenários reproduzíveis e os alertas certos disparam |
| F7 (opcional) | Devolução (`pacs.004`), MED 2.0, deploy em kind + kube-prometheus-stack | — |

## 14. Decisões e premissas

- **Formato das mensagens:** JSON com a semântica e os nomes de campo do
  ISO 20022 (`pacs.008`, `pacs.002`), só os campos usados; sem mTLS nem
  assinatura real.
- **bacen-sim:** conta PI/STR (saldo, aporte/retirada, extrato) + cálculo do IGA.
- **resolvedor de pendências:** estados não terminais via consulta ao SPI,
  reprocessamento de DLQ e reconciliação diária conta PI × ledger.
- **Dois PSPs no compose:** compartilham o mesmo Mongo e RabbitMQ (database e
  vhost separados) para manter a máquina leve.
