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
cache curto, timeout por tentativa, retry com backoff e jitter só para erros
transitórios e circuit breaker) e, numa única transação MongoDB: grava a chave
de idempotência, reserva o saldo (`available -= v`, `held += v`, com condição
`available >= v`), lança a reserva no razão, grava o pagamento `PENDING` e o
evento de outbox com a `pacs.008`.
- Idempotência por (conta pagadora, `Idempotency-Key`): repetir o mesmo pedido
  devolve o pagamento original (`200`, `Idempotent-Replayed: true`); outro pedido
  com a mesma chave → `422 IDEMPOTENCY_KEY_REUSED`.
- Erros de negócio: `INSUFFICIENT_FUNDS`, `ACCOUNT_NOT_FOUND`, `ACCOUNT_NOT_ACTIVE`,
  `KEY_NOT_FOUND`, `SAME_PSP_NOT_SUPPORTED` (422), `DICT_RATE_LIMITED` (429),
  `DICT_UNAVAILABLE` (503, falha rápida com o breaker aberto).
- Também expõe `GET /v1/pix/payments/{endToEndId}`, `GET /v1/accounts/{agência}/{conta}`
  e gestão de chaves (`POST /v1/keys`, `DELETE /v1/keys/{chave}`, proxy ao DICT).

**outbox-relay** — escuta o change stream da coleção `outbox`, publica no
RabbitMQ com publisher confirms, marca `publishedAt` e persiste o resume token.
Continua o trace gravado no evento (o `traceparent` segue nos headers AMQP).
Uma varredura periódica publica o que o stream não entregou (primeira subida,
falha de publicação, token descartado por `ChangeStreamHistoryLost`). Entrega
at-least-once: duplicatas são absorvidas pela idempotência dos consumidores.

**spi-connector** — consome `spi.out` e envia ao SPI (a `pacs.008` do Pix
enviado e as `pacs.002` de resposta do lado recebedor). Aceite do SPI (`202`)
leva o pagamento a `SENT`; recusa definitiva (4xx) rejeita o pagamento e libera a
reserva; falha transitória devolve a mensagem à fila, e o reenvio é seguro
porque o SPI deduplica por `EndToEndId`. Em paralelo, faz long-polling do SPI e
roteia: `pacs.008` recebida → `pix.incoming`; `pacs.002` → `pix.results`. O lote
só é confirmado ao SPI (`?ack=` do `PI-PULL-NEXT`) depois de publicado com
confirm. Cada mensagem continua o trace de quem a originou no SPI.

**result-worker** — consome `pix.results` e fecha o pagamento enviado:
`ACSC` → `CONFIRMED` (baixa `held`); `RJCT` → `REJECTED` (devolve `held` para
`available`). Estado, saldo e razão mudam numa transação, condicionada ao
pagamento ainda estar em `PENDING` ou `SENT`, então reentregas não fazem nada.
Notifica o cliente (log; webhook fica para depois).

**incoming-worker** — consome `pix.incoming`: valida conta de destino e grava o
recebimento `RECEIVED`; responde `pacs.002` `ACSP` (aceite) ou `RJCT` (ex.:
conta inexistente/bloqueada) via outbox. Na confirmação de liquidação (`ACSC`)
credita a conta (`CREDITED`). Tudo idempotente por `EndToEndId`.
*Hoje (F2) só responde: `ACSP` se a conta existe e está ativa, `RJCT AC03` se não
existe, `RJCT AC06` se bloqueada ou encerrada. Gravar o recebimento e creditar é
a F3.*

**pending-resolver** — job periódico que busca pagamentos/recebimentos em estado
não terminal além de um limiar (ex.: `PENDING`/`SENT` > 40s, `ACCEPTED` sem
liquidação), consulta o status no SPI e aplica o resultado pelo mesmo caminho do
result-worker. Também reprocessa a DLQ com segurança (idempotência) e roda a
reconciliação diária: compara o extrato da conta PI (bacen-sim) com o ledger
interno e registra divergências (coleção `reconciliation_reports` + métrica e
alerta).

## 5. Simuladores

**spi-sim** — roteador entre participantes, inspirado na interface ICOM:
- `POST /api/v1/in/{ispb}/msgs`: PSP envia `pacs.008`/`pacs.002` (`AppHdr.Fr`
  = ISPB do caminho, `AppHdr.To` = `00038166`). Responde `202`; o SPI reemite a
  `pacs.008` para o recebedor com o próprio cabeçalho.
- `GET /api/v1/out/{ispb}/stream?wait=<dur>`: long-polling. `200` com até 100
  mensagens ou `204` no prazo; o header `PI-PULL-NEXT` traz a URL do próximo
  pull, com `?ack=<lote>` para confirmar o lote recebido. Entrega
  at-least-once: lote não confirmado volta a ser entregue quando o lease vence.
- `GET /api/v1/status/{endToEndId}` com `PI-RequestingParticipant`: situação
  (`PDNG`, `ACSP`, `ACSC`, `RJCT`), simplificação do `pacs.028`. Quem não
  participa da transação recebe `404`.
- Estados: `AWAITING_CREDITOR` → `SETTLING` → `SETTLED` | `REJECTED`, com
  transições condicionais sobre o estado atual.
  - Recebedor responde `RJCT` → pagador recebe `RJCT` com o motivo dele.
  - Recebedor não responde no prazo (`SPI_CREDITOR_TIMEOUT`) → `RJCT AB03` para
    os dois.
  - Liquidação recusada pelo bacen-sim → `RJCT AM04` (saldo) ou `ED05` para os
    dois; falha temporária deixa em `SETTLING` e a varredura tenta de novo.
  - Liquidou → `ACSC` para os dois.
- Deduplicação por `EndToEndId`: reenvio da mesma mensagem é aceito e
  ignorado; o mesmo id em outra mensagem → `409 DUPLICATE_END_TO_END_ID`.
- Extensão do laboratório: cada mensagem entregue carrega `traceContext`
  (W3C `traceparent`) de quem a originou, para o trace seguir do pagador ao
  recebedor através do SPI.

**dict-sim** — diretório de chaves:
- `POST /api/v1/entries`: registra vínculo (CPF, CNPJ, e-mail, celular E.164,
  EVP gerado se vier vazio) de conta do próprio participante. Chave CPF/CNPJ
  precisa ser o documento do titular; até 5 chaves por conta (PF) ou 20 (PJ);
  chave já vinculada → `409` (portabilidade e reivindicação ficam fora).
- `GET /api/v1/entries/{key}` com `PI-RequestingParticipant` e `PI-PayerId`
  (CPF/CNPJ do pagador): token bucket por usuário pagador; chave inexistente
  custa mais que encontrada (anti-varredura). Esgotado → `429` + `Retry-After`.
- `DELETE /api/v1/entries/{key}`: só o participante dono.
- Carga inicial em `deploy/sims/dict-seed.json` (Alice e Carol em A; Bob e
  Dave em B, uma chave de cada tipo).
- (fase MED) relatos de infração e pedidos de devolução.

**bacen-sim** — reservas do participante e visão do regulador:
- `POST /api/v1/settlements` (chamado pelo spi-sim): débito e crédito nas
  contas PI numa transação, débito condicionado ao saldo. Idempotente por
  `EndToEndId` (`201` nova, `200` repetição); saldo insuficiente → `422 AM04`.
- `GET /api/v1/pi-accounts/{ispb}`: saldo.
- `POST /api/v1/pi-accounts/{ispb}/str-transfers`: aporte (`IN`) ou retirada
  (`OUT`), idempotente por `transferId`.
- `GET /api/v1/pi-accounts/{ispb}/statement?from&to&limit&after`: extrato
  append-only paginado por cursor, usado na reconciliação diária. O saldo é
  sempre a soma do extrato (o saldo inicial entra como aporte `seed`).
- (F6) IGA: recebe os timestamps por transação (o SPI envia os seus; cada PSP
  envia periodicamente `t0'`, `t6a` e equivalentes do recebedor em
  `POST /api/v1/iga/reports`), calcula os indicadores por janela
  (`GET /api/v1/iga/{ispb}`) e os expõe como métricas. Permite comparar o SLO
  interno do PSP com o que o regulador enxerga.

**Injeção de falhas** (F5) — todos os simuladores expõem `/admin/faults` para
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
| `accounts` | agência/conta, titular, `balance.available`, `balance.held` (centavos), status | único (agência, conta) |
| `ledger_entries` | lançamentos append-only (`SEED`, `HOLD`, `CONFIRM`, `RELEASE`; crédito na F3), cada um com `availableDelta` e `heldDelta`: o saldo é a soma do razão | único (conta, `EndToEndId`, tipo); (conta, data) |
| `payments` | `_id = EndToEndId`, direção, estado, valor, partes, histórico, `createdAt` (t0), `sentAt`, `settledAt`, `finishedAt` | estado + `updatedAt` (resolver) |
| `idempotency_keys` | (conta, `Idempotency-Key`) → `EndToEndId` + hash do pedido | único (conta, chave), TTL 24h |
| `outbox` | rota, `messageId`, corpo JSON, `traceContext`, `createdAt`, `publishedAt` | `publishedAt` (TTL 24h só nos publicados) |
| `relay_checkpoints` | resume token do change stream | — |

## 9. Mensageria (RabbitMQ, um vhost por PSP)

- Exchange `pix` (topic) e `pix.dlx` (dead-letter).
- Quorum queues: `spi.out`, `pix.results`, `pix.incoming`, cada uma com
  `x-delivery-limit` (10) e DLQ (`*.dlq`, via `pix.dlx`). Cada processo declara
  a topologia ao subir (idempotente).
- Consumidores com ack manual: erro transitório espera `RETRY_DELAY` e devolve a
  mensagem à fila (nack com requeue); mensagem malformada vai direto à DLQ; ao
  estourar o limite de entregas a fila a manda à DLQ.
- Retry com backoff (F4): filas `*.retry.{5s,30s,2m}` com TTL que devolvem para a fila de origem.
- Headers: `traceparent`, `x-enqueued-at` (idade da mensagem, vira a métrica
  `broker.message.age`); `x-attempt` chega com o retry da F4.

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
internal/sim       spi, dict e bacen (lógica dos simuladores)
internal/app       esqueleto comum dos binários de simulador
compose.yaml       stack local (raiz do repositório)
deploy/rabbitmq    vhosts, usuário e plugins
deploy/sims        carga inicial do dict-sim
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
- **Pix recebido na F2:** o incoming-worker só aceita ou recusa; sem isso o Pix
  enviado não fecharia o ciclo. A gravação do recebimento e o crédito (e o
  `ACSC` do lado recebedor, hoje ignorado) entram na F3.
- **Dois PSPs no compose:** compartilham o mesmo Mongo e RabbitMQ (database e
  vhost separados) para manter a máquina leve.
