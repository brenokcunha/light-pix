# SLO — iniciação

**Alertas:** PixInitiationLatency (SLO: 95% dos pedidos em até 1,5s)  
**Cenário de reprodução:** 4 (`scenarios/`)

## Sintoma e impacto

A API demora para aceitar o pedido de Pix. Antes de qualquer SPI: a iniciação é a API, o DICT e a gravação no MongoDB.

## Diagnóstico

1. *Pix — visão IGA* → *Iniciação*: o P95 da API passou de 1,5s?
2. DICT lento? *Pix — serviços* → *DICT: consultas por resultado* e `curl localhost:8092/admin/faults`. O gateway tem timeout de 800ms e 3 tentativas: um DICT lento custa até ~2,4s antes de o circuit breaker abrir.
3. MongoDB saturado: *MongoDB: conexões e operações/s* e CPU do contêiner `mongo`. A reserva de saldo é uma transação com cinco escritas; contas muito disputadas geram conflitos de escrita.
4. Trace de um `POST /v1/pix/payments` lento: o span `HTTP GET` (DICT) ou `commitTransaction` mostra onde está a espera.

## Mitigação

- DICT: ver `dict-breaker.md`.
- MongoDB: reduzir a carga (taxa de entrada) ou aumentar recursos; em produção, escalar o banco.

## Depois

- Se a iniciação degrada com a carga, comparar a taxa atual com os ~20 Pix/s que esta máquina sustenta (README, "Falhas e carga").
