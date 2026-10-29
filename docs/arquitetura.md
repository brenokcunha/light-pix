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
- Devolução: `POST /v1/pix/payments/{endToEndId}/returns` (com `Idempotency-Key`) devolve,
  total ou parcialmente, um Pix recebido e creditado, dentro de 90 dias; `GET
  /v1/pix/returns/{id}` e `GET /v1/pix/payments/{endToEndId}/returns` consultam.
  Ver 6.3.
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

**result-worker** — consome `pix.results` e fecha o pagamento: no Pix enviado,
`ACSC` → `CONFIRMED` (baixa `held`) e `RJCT` → `REJECTED` (devolve `held` para
`available`); no recebido, ver o incoming-worker. Estado, saldo e razão mudam numa transação, condicionada ao
pagamento ainda estar em `PENDING` ou `SENT`, então reentregas não fazem nada.
Notifica o cliente (log; webhook fica para depois). Uma `pacs.002` sobre Pix
recebido ainda não registrado (correu na frente da `pacs.008` entre as filas)
volta à fila e é tentada de novo; sobre Pix enviado que este PSP não conhece, é
descartada.

**incoming-worker** — consome `pix.incoming` (`pacs.008` vinda do SPI). Numa
transação, grava o recebimento em `payments` (`direction = IN`, `_id =
EndToEndId`) e a resposta na outbox: conta existe e está ativa → `ACCEPTED` e
`pacs.002 ACSP`; conta inexistente → `REJECTED` e `RJCT AC03`; conta bloqueada
ou encerrada → `REJECTED` e `RJCT AC06`. O histórico começa em `RECEIVED`.
Reentrega da mesma `pacs.008` não faz nada (o `_id` já existe). O crédito só
acontece na liquidação: o `ACSC` que o SPI manda ao recebedor chega por
`pix.results` e leva `ACCEPTED` → `CREDITED`, creditando `available` e lançando
`CREDIT` no razão, tudo na mesma transação e condicionado ao estado. Um `RJCT`
tardio (prazo estourado) leva `ACCEPTED` → `REJECTED` sem mover dinheiro; um
`RJCT` depois de `CREDITED` é ignorado.

**pending-resolver** — três ciclos independentes, todos idempotentes (usam as
mesmas escritas condicionais do caminho normal, então várias réplicas convivem):
- *Pendências* (a cada `RESOLVER_INTERVAL`, 10s): pagamentos em estado não
  terminal além de `RESOLVER_MIN_AGE` (40s) — enviados em `PENDING`/`SENT`,
  recebidos em `ACCEPTED` — têm a situação consultada no SPI
  (`GET /api/v1/status`) e o resultado aplicado pelo mesmo caminho do worker:
  `ACSC` confirma/credita, `RJCT` rejeita, `ACSP`/`PDNG` espera. Se o SPI não
  conhece a transação (404), nada é feito: reembolsar daqui arriscaria pagar duas
  vezes se a mensagem ainda estiver na fila de retry. O gauge
  `pix.payments.stuck` conta o que está parado.
- *DLQs* (a cada `RESOLVER_DLQ_INTERVAL`, 1min): devolve à fila de origem as
  mensagens das três DLQs, até `RESOLVER_MAX_REDRIVES` vezes cada (header
  `x-redrive`); é seguro porque os consumidores são idempotentes. Depois disso a
  mensagem é estacionada na coleção `dead_letters` (`GET /admin/dead-letters`).
  Se for a `pacs.008` de um Pix enviado, o resolver pergunta ao SPI antes de
  estacionar: se ele nunca a recebeu, o Pix é rejeitado (`ED05`) e a reserva
  volta, porque a mensagem não vai mais sair; se recebeu, vale o que o SPI diz.
- *Reconciliação* (diária, depois de `RECONCILE_AT`, 03:00 UTC, para o dia UTC
  anterior; `POST /admin/reconciliation?from=&to=` roda na hora): compara as
  liquidações do extrato da conta PI (bacen-sim) com os pagamentos `CONFIRMED` e
  `CREDITED` do PSP, pelo `EndToEndId`, e grava o relatório em
  `reconciliation_reports` (totais dos dois lados e divergências). Tipos:
  `PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT`, `LOCAL_SETTLEMENT_WITHOUT_PI_ENTRY`,
  `AMOUNT_MISMATCH` e `DIRECTION_MISMATCH`. Métricas: `reconciliation.runs`,
  `reconciliation.divergences{type}`, `reconciliation.last.divergences` e
  `reconciliation.last.timestamp` (para alertar divergência e relatório atrasado).

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
- Devolução (`pacs.004`): o SPI valida que o Pix original está liquidado e dentro do
  prazo (`ReturnWindow`, 90 dias), que quem devolve é o recebedor dele e que a
  devolução vai ao pagador dele. O total devolvido (ou em devolução) não passa do
  valor do Pix: a reserva é um `$inc` condicional no Pix original, na mesma
  transação que grava a devolução, então devoluções simultâneas nunca somam mais
  que o Pix. Recusas síncronas: `UNKNOWN_END_TO_END_ID`, `ORIGINAL_NOT_SETTLED`,
  `FORBIDDEN`, `RETURN_WINDOW_EXPIRED`, `RETURN_AMOUNT_EXCEEDED`. A devolução é uma
  transação do SPI com identificador `D...` e `kind=RETURN`; segue os estados do
  Pix (o pagador original responde `pacs.002`, o SPI liquida, o prazo vence com
  `AB03`) e, se rejeitada, libera o valor reservado.
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
- **Relatos de infração (MED)**: `POST /api/v1/infraction-reports` (o PSP do pagador relata
  um Pix seu), `GET` (um relato, ou a lista: `role=counterparty` é a fila de quem precisa
  analisar), `POST .../analysis` (o recebedor decide, uma vez: `AGREED` ou `DISAPPROVED`, com
  a devolução especial gerada) e `POST .../cancel` (o pagador, só antes da análise). Um relato
  ativo por Pix (índice único parcial); prazo de análise `DICT_ANALYSIS_DEADLINE` (72h), com
  `overdue` calculado na leitura. Só participam do relato os dois PSPs.

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
- **IGA**: mede a experiência do Pix do ponto de vista do regulador, juntando
  instantes de três fontes por `EndToEndId`:
  - o SPI envia os seus ao encerrar cada transação (`POST /api/v1/iga/spi-records`,
    em lotes de 1s, sem bloquear o fluxo): chegada da `pacs.008`, aceite do
    recebedor, liquidação e resultado;
  - cada PSP envia os seus (`POST /api/v1/iga/reports`, a cada 10s, pelo resolver,
    com cursor persistido): o pagador manda `t0` (pedido aceito) e `t6a` (estado
    final); o recebedor manda a chegada e o aceite.
  - Os indicadores saem como histogramas, cada um medido uma única vez quando as duas
    pontas existem, mesmo com relatórios repetidos ou fora de ordem:
    `iga.payer_experience.duration` (t0 → t6a), `iga.initiation.duration` (t0 → chegada
    ao SPI), `iga.settlement.duration` (chegada → liquidação, meta 40s),
    `iga.creditor_response.duration` (chegada → ACSP) e o contador `iga.transactions`
    por desfecho. `GET /api/v1/iga/{ispb}?window=1h` devolve P50/P95/P99, rejeições
    por motivo e se cada meta (P50 ≤ 6s e P99 ≤ 10s do pagador; P50 ≤ 0,9s e P95 ≤ 1,5s
    da iniciação; liquidação ≤ 40s) está sendo cumprida. Permite comparar o SLO
    interno do PSP com o que o regulador enxerga.

**Injeção de falhas** — os três simuladores expõem `GET`, `PUT` e `DELETE
/admin/faults` (PUT substitui tudo; DELETE desliga; `FAULTS` no ambiente dá a
configuração inicial). As falhas mudam em tempo de execução, em memória, e
`/admin`, `/healthz` e `/readyz` nunca são afetados (dá para desligar as falhas
e o orquestrador não derruba o simulador). Cada falha injetada soma em
`faults.injected{kind}` e marca o span (`fault.injected`, `fault.latency_ms`).

| Campo | Efeito |
|---|---|
| `latency` | espera antes de responder: `{"mode":"fixed","ms":200}`, `{"mode":"uniform","minMs":50,"maxMs":400}` ou `{"mode":"lognormal","p50Ms":80,"p99Ms":1500}` (cauda longa) |
| `errorRate`, `errorStatus` | fração (0 a 1) das requisições respondidas com o status (padrão 503) |
| `unavailable` | 503 em tudo |
| `paths` | restringe latência e erros a rotas com esses prefixos (vazio: todas) |
| `dropPacs002` (SPI) | fração das `pacs.002` que o SPI nunca entrega; a transação avança e só a consulta de status revela o resultado |
| `creditorTimeoutRate` (SPI) | fração das `pacs.008` que o SPI não encaminha ao recebedor: o prazo vence e só o pagador recebe `RJCT AB03` |

Conta PI sem saldo (`AM04`) não é uma falha injetada: basta uma retirada STR
(`POST /api/v1/pi-accounts/{ispb}/str-transfers`, `OUT`) esvaziar a conta.

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

### 6.3 Devolução (R → P: o recebedor devolve ao pagador)

1. O cliente de B pede `POST /v1/pix/payments/{E...}/returns`. B reserva o valor
   devolvível do Pix original (teto atômico), reserva o saldo do cliente, grava a
   devolução `PENDING` (coleção `returns`, `_id = D...`) e a `pacs.004` na outbox.
2. O conector envia ao SPI (`SENT`), que valida e encaminha a A (pagador original).
3. A, no incoming-worker, confere que o Pix é dele e foi confirmado, que a conta
   existe e está ativa e que a devolução cabe no que resta; grava a devolução
   `ACCEPTED` e responde `pacs.002 ACSP` (ou `RJCT`: `BE01` incoerente, `AC03`/`AC06`
   conta, `AM09` valor).
4. O SPI liquida da conta PI de B para a de A e manda `ACSC` aos dois. B baixa a
   reserva (`CONFIRMED`); A credita a conta (`CREDITED`).

Rejeição em qualquer ponto devolve a reserva de saldo ao cliente de B e o valor ao
Pix original, que continua devolvível. O resolver, a DLQ e a reconciliação tratam
devoluções como tratam Pix; as métricas ficam em `pix.returns.finished`, fora dos SLOs
de Pix.

### 6.4 MED: relato de infração e devolução especial

1. A vítima avisa o PSP do pagador, que relata o Pix (`POST /v1/pix/payments/{E...}/infraction-reports`,
   `FRAUD` ou `SCAM`, em até 80 dias) no DICT. O relato nasce `OPEN`.
2. O ciclo do MED do PSP recebedor (no resolver, a cada 10s) traz o relato e faz o
   **bloqueio cautelar**: move da conta que recebeu o Pix, de `available` para `blocked`, o
   valor relatado ou o que sobrar dele (`BLOCK` no razão). Relato sobre um Pix que ele não
   conhece, não creditado ou incoerente é recusado sozinho.
3. O analista decide (`POST /v1/infraction-reports/{id}/decision`):
   - `AGREE`: o bloqueio vira reserva de uma **devolução especial** (`pacs.004`, motivo `FR01`,
     `MED_RESERVE`) do valor bloqueado, que segue o fluxo de qualquer devolução; o relato fica
     `AGREED` e depois `RETURNED` (ou `RETURN_FAILED`, que exige ação manual). Sem nada
     bloqueado, fica `NO_FUNDS`.
   - `DISAGREE`: o valor volta ao disponível (`UNBLOCK`), `DISAPPROVED`.
   A decisão vai ao DICT pelo mesmo ciclo (reenviada até conseguir).
4. O PSP do pagador acompanha o relato no DICT e fecha o dele quando a devolução especial é
   creditada. Se o pagador cancelar antes da análise, o recebedor desbloqueia no ciclo seguinte.

Limitação do laboratório: o MED 2.0 real rastreia o dinheiro que já saiu da conta do primeiro
recebedor para outras; aqui o bloqueio alcança só o saldo da conta que recebeu o Pix. Estados
em `infractions` (um documento por PSP e relato); `med.infractions` e `med.infractions.overdue`
(alerta `InfractionAnalysisOverdue`) são as métricas.

### 6.2 Falhas tratadas

| Falha | Tratamento |
|---|---|
| Mensagem duplicada (relay/rabbit) | escrita condicional por `EndToEndId`; consumidor descarta |
| SPI fora / 5xx | retry com backoff via filas TTL; esgotou → DLQ; o resolver devolve da DLQ e, no limite, estaciona e rejeita se o SPI nunca viu a transação |
| `pacs.002` perdida | pending-resolver consulta status no SPI (enviado e recebido) |
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
| `ledger_entries` | lançamentos append-only (`SEED`, `HOLD`, `CONFIRM`, `RELEASE`, `CREDIT` e, no MED, `BLOCK`, `UNBLOCK`, `MED_RESERVE`), cada um com `availableDelta`, `heldDelta` e `blockedDelta`: o saldo é a soma do razão | único (conta, `EndToEndId`, tipo); (conta, data) |
| `payments` | `_id = EndToEndId`, direção (`OUT`/`IN`), estado, valor, partes, histórico, `createdAt` (OUT: t0; IN: chegada da `pacs.008`), `sentAt`, `acceptedAt`, `settledAt`, `finishedAt`, `returnedAmount` (valor já devolvido ou em devolução) | estado + `updatedAt` (resolver) |
| `returns` | devoluções, no mesmo formato de `payments`: `_id = D...`, `returnOf` (Pix original), `returnReason` | estado + `updatedAt`; `returnOf` |
| `infractions` | relatos de infração do MED, nos papéis `REPORTER` e `RECEIVER`: estado, valor relatado e bloqueado, decisão, devolução especial, prazo | papel + estado + `createdAt`; `endToEndId` |
| `idempotency_keys` | (conta, `Idempotency-Key`) → `EndToEndId` + hash do pedido | único (conta, chave), TTL 24h |
| `outbox` | rota, `messageId`, corpo JSON, `traceContext`, `createdAt`, `publishedAt` | `publishedAt` (TTL 24h só nos publicados) |
| `relay_checkpoints` | resume token do change stream | — |
| `dead_letters` | mensagens que esgotaram os reenvios da DLQ (fila, corpo, último erro) | — |
| `reconciliation_reports` | relatório de cada reconciliação (janela, totais, divergências) | (gatilho, início da janela); `createdAt` |

## 9. Mensageria (RabbitMQ, um vhost por PSP)

- Exchange `pix` (topic) e `pix.dlx` (dead-letter).
- Quorum queues: `spi.out`, `pix.results`, `pix.incoming`, cada uma com
  `x-delivery-limit` (10) e DLQ (`*.dlq`, via `pix.dlx`). Cada processo declara
  a topologia ao subir (idempotente).
- Consumidores com ack manual. Falha transitória: a mensagem é republicada (com
  confirm) na fila de espera da tentativa seguinte, `<fila>.retry.5s`, `.30s` e
  `.2m` (`RETRY_DELAYS`), e confirmada na original; filas de espera são quorum
  queues com `x-message-ttl` e dead-letter para a fila de origem, então a
  mensagem volta sozinha com `x-attempt` incrementado. Esgotadas as tentativas,
  ou em erro permanente (mensagem malformada), vai para a DLQ com o último erro
  em `x-last-error`. O `x-delivery-limit` da fila só serve de rede de segurança
  caso nem a republicação funcione (nack com requeue).
- DLQ: quem a esvazia é o pending-resolver (ver acima).
- Headers: `traceparent`, `x-enqueued-at` (idade da mensagem, vira a métrica
  `broker.message.age`); `x-attempt`, `x-last-error`, `x-source-queue` e `x-redrive` controlam retry e DLQ.

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
- **SLOs (Sloth):** `scripts/gen_slo.py` gera uma especificação por PSP em
  `deploy/slo/specs/` e `make slo` roda o Sloth (container) para escrever as regras em
  `deploy/observability/prometheus/rules/slo-*.yml`. Cinco SLOs por PSP:
  experiência do pagador (99% dos Pix em até 10s), iniciação (95% em até 1,5s),
  disponibilidade da API (99,5% sem 5xx), taxa de sucesso (99% sem rejeição técnica:
  `AB03`, `ED05`, `AM04`) e prazo de liquidação (99,9% em até 40s, visão do regulador).
  Cada um tem alerta de página e de ticket por burn rate em múltiplas janelas; as
  janelas do laboratório são curtas (`deploy/slo/windows/30d.yaml`, 1m/5m, 3m/15m,
  10m/1h, 30m/3h), com as mesmas taxas de queima do padrão (14,4x, 6x, 3x e 1x).
- **Alertas de causa** (`rules/pix.yml`, `rules/infra.yml`): idade e profundidade de
  fila, outbox acumulando, retries, mensagens que foram à DLQ, DLQ que não esvazia,
  mensagens estacionadas, Pix parados, circuit breaker do DICT aberto, divergência e
  atraso da reconciliação, liquidez da conta PI, reinícios, memória perto do limite
  e provável OOM. Cada alerta traz a anotação `runbook` com o caminho em
  `docs/runbooks/` (um por alerta, gerados por `scripts/gen_runbooks.py`).
- **Grafana** (dashboards gerados por `scripts/gen_dashboards.py`, provisionados):
  - *Pix — visão IGA*: experiência do pagador, iniciação, liquidação e resposta do
    recebedor, PSP × regulador, com as metas; desfechos por motivo; orçamento de erro
    e burn rate dos SLOs; alertas disparando; e os Pix lentos (TraceQL no Tempo) para
    abrir o trace.
  - *Pix — serviços*: HTTP por serviço, contêineres e runtime Go, filas e idade das
    mensagens, outbox, resolver, reconciliação, circuit breaker, simuladores, conta
    PI, MongoDB e a carga do k6.
  - Anotações de falhas injetadas e de reinícios de contêiner; correlação trace ↔ logs
    ↔ métricas pelos datasources provisionados.

## 11. Cenários de falha (`scenarios/`)

Cada cenário combina carga (k6) e falhas injetadas, **confere que os alertas esperados
disparam** e que o sistema se recupera sozinho (filas e DLQs vazias, nenhum Pix parado,
conta PI conciliada). `make scenarios` roda os cinco; ver `scenarios/README.md`.

1. Mini-case: vazamento de memória no connector (`LEAK_KB_PER_MESSAGE`) + limite de
   memória → `OOMKilled` em ciclo, fila `spi.out` oscilando.
2. SPI intermitente (30% de 5xx) → retries, DLQ, reprocessamento.
3. `pacs.002` perdidas → pendências resolvidas pelo resolver.
4. DICT lento → circuit breaker abre, falha rápida.
5. Conta PI sem saldo → rejeições `AM04`, alerta de liquidez.

## 15. Kubernetes (kind)

O mesmo laboratório roda num cluster kind de um nó, empacotado num chart Helm
(`deploy/helm/light-pix`) mais o kube-prometheus-stack. Com a stack do Compose parada
(mesmas portas, e a máquina não comporta as duas), `make k8s-tools` baixa kind e helm
para `.tools/`, `make k8s-up` cria o cluster, carrega as imagens, instala metrics-server,
kube-prometheus-stack (namespace `monitoring`, release `kps`) e o chart (namespace
`lightpix`), e `make k8s-down` apaga o cluster. As portas do host são as do Compose
(8081 a 8084, 8091 a 8093, Grafana 3000, Prometheus 9090, Alertmanager 9093, RabbitMQ 15672).

| Compose | Kubernetes |
|---|---|
| `psp-{a,b}-{api,relay,connector,worker,resolver}` | um Deployment por papel e PSP (os mesmos 10), com Service NodePort só na api e no resolver |
| `spi-sim`, `dict-sim`, `bacen-sim` | um Deployment e um Service cada |
| `mongo` (replica set de 1 nó) | StatefulSet com volume; a sonda de prontidão inicia o replica set e só passa com primário eleito; Service headless `mongo-0.mongo` |
| `rabbitmq` | StatefulSet com volume; vhosts e plugins por ConfigMap; Service com as portas AMQP, gestão e métricas |
| `otel-collector`, `tempo` | Deployments (o Collector é uma réplica só: o tail sampling precisa ver o trace inteiro); métricas OTLP vão ao Prometheus do kps e traces ao Tempo |
| `prometheus`, `alertmanager`, `grafana` | kube-prometheus-stack; o Prometheus recebe OTLP e remote write, e os dashboards e regras do repositório entram por ConfigMaps (sidecar do Grafana) e PrometheusRules |
| `mongodb-exporter`, RabbitMQ `/metrics` | Deployment e ServiceMonitors |
| cAdvisor | kubelet/cAdvisor e kube-state-metrics do kps |
| `restart: unless-stopped`, healthchecks | probes (`startup`, `liveness` em `/healthz`, `readiness` em `/readyz`), kubelet reinicia |

Decisões:

- **Um chart para tudo que é nosso** (apps, Mongo, RabbitMQ, Collector, Tempo, exporters,
  regras, dashboards); o kube-prometheus-stack é instalado antes, porque o chart cria
  ServiceMonitors e PrometheusRules.
- **Escala e disponibilidade:** a api tem HPA por CPU (2 a 6 réplicas, exige metrics-server) e
  PodDisruptionBudget junto com o worker (`minAvailable: 1`); relay, resolver e simuladores
  ficam em uma réplica com `Recreate` (dois relays publicariam em dobro; dois resolvers e dois
  spi-sim disputariam o mesmo trabalho). Os consumidores são idempotentes, então réplicas do
  worker e do connector são seguras.
- **Segurança:** contêineres Go rodam com UID 65532 numérico (o Kubernetes não verifica o
  `USER nonroot` por nome), `readOnlyRootFilesystem`, sem privilégios e sem capabilities.
- **Imagens** vêm do Docker local (`kind load docker-image`), com `imagePullPolicy:
  IfNotPresent`; um init container espera o primário do MongoDB antes de cada serviço subir.
- **Cópias geradas:** o Helm só lê dentro do chart, então `make k8s-sync` copia dashboards,
  regras, configuração do RabbitMQ e cargas iniciais para `deploy/helm/light-pix/files/`.
- **Regras:** as que usam rótulos do Compose (cAdvisor) ficam de fora; reinício e OOM vêm das
  regras padrão do kube-state-metrics. Os painéis de contêiner dos dashboards só têm dados no Compose.
- **Fora do cluster:** Loki e Alloy (logs: `kubectl logs`), cenários e `make load`, que usam o
  Compose; o k6 pode apontar para o cluster (`PSP_A_URL`, `PSP_B_URL`, `host.docker.internal`).

Validado: Pix, devolução, consulta de métricas OTLP, alvos e regras no Prometheus, dashboards
no Grafana e HPA com carga; num teste de caos a 10 Pix/s, matar connectors, relays e workers
durante a carga não perdeu nenhum Pix (749 finalizados, conta PI conciliada sem divergência).

## 12. Estrutura do repositório

```
cmd/psp            subcomandos api | relay | connector | worker | resolver
cmd/spi-sim
cmd/dict-sim
cmd/bacen-sim
cmd/lab-ui         interface de testes (página + proxy para as APIs dos PSPs)
internal/labui     a página (embutida) e o proxy /api/a/ e /api/b/
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
deploy/psp         carga inicial das contas dos PSPs
deploy/observability  otelcol, prometheus, alertmanager, tempo, loki, alloy, grafana
deploy/helm        chart Helm do laboratório (light-pix) para o Kubernetes
deploy/k8s         configuração do cluster kind e valores do kube-prometheus-stack
deploy/slo         especificações Sloth (geradas) e janelas de alerta; as regras vão para deploy/observability/prometheus/rules
loadtest           scripts k6
scenarios          cenários de incidente (script + alertas esperados)
scripts            geradores (SLOs, dashboards, runbooks) e faults.sh
docs               arquitetura e runbooks
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
| F7 (opcional) | Devolução (`pacs.004`), MED 2.0, deploy em kind + kube-prometheus-stack | feito: devolução (6.3), MED (6.4) e Kubernetes (15) |

## 14. Decisões e premissas

- **Formato das mensagens:** JSON com a semântica e os nomes de campo do
  ISO 20022 (`pacs.008`, `pacs.002`), só os campos usados; sem mTLS nem
  assinatura real.
- **bacen-sim:** conta PI/STR (saldo, aporte/retirada, extrato) + cálculo do IGA.
- **resolvedor de pendências:** estados não terminais via consulta ao SPI,
  reprocessamento de DLQ e reconciliação diária conta PI × ledger.
- **Enviado e recebido na mesma coleção:** `payments` guarda os dois sentidos
  (o `EndToEndId` carrega o ISPB do pagador, então não colide entre PSPs). O
  recebimento nasce já `ACCEPTED` (ou `REJECTED`) na mesma transação que grava a
  resposta, em vez de passar por `RECEIVED` como estado persistido.
- **Dois PSPs no compose:** compartilham o mesmo Mongo e RabbitMQ (database e
  vhost separados) para manter a máquina leve.
