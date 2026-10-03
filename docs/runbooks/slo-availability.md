# SLO — disponibilidade da API

**Alertas:** PixApiAvailability (SLO: 99,5% das requisições sem 5xx)  
**Cenário de reprodução:** 4 (`scenarios/`)

## Sintoma e impacto

A payment-api devolve erros 5xx. O `DICT_UNAVAILABLE` (503) conta aqui, assim como qualquer erro interno.

## Diagnóstico

1. Qual código? No Loki: `{service_name=~"psp-.*-api"} |= "erro interno"` ou o campo `status` dos logs de acesso; no Prometheus: `sum by (http_response_status_code, http_route) (rate(http_server_request_duration_seconds_count{job="light-pix/payment-api", http_response_status_code=~"5.."}[5m]))`.
2. 503 em `/v1/pix/payments` com o breaker aberto: é o DICT (`dict-breaker.md`).
3. 500: erro interno, geralmente MongoDB. `docker compose logs psp-a-api | tail`; `mongo` saudável? Conflitos de transação esgotados sob carga alta aparecem como 500.

## Mitigação

- DICT: `make faults s=dict j=clear` se for falha injetada; senão ver `dict-breaker.md`.
- MongoDB: reiniciar só se não responder; sob carga, reduzir a taxa.

## Depois

- Pedidos que falharam com 5xx não deixam reserva de saldo (a transação desfaz), então não há o que acertar no razão.
