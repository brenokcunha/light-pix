# Runbooks

Um runbook por alerta. Cada alerta do Prometheus traz o caminho do seu runbook na
anotação `runbook`. Os alertas de **SLO** (Sloth) dizem que o usuário está sendo
afetado; os de **causa** dizem o que quebrou. Comece pelo SLO e desça até a causa.

Onde olhar primeiro:

- Grafana (`localhost:3000`): **Pix — visão IGA** (impacto, SLOs, Pix lentos) e
  **Pix — serviços** (HTTP, contêineres, filas, outbox, resolver, DICT, MongoDB).
- Alertmanager (`localhost:9093`) e Prometheus → Alerts (`localhost:9090/alerts`).
- Um Pix específico: Tempo → `{span.pix.end_to_end_id="E..."}`; logs no Loki com o
  `trace_id`.
- Resolver (`localhost:8083`/`8084`): `/admin/reconciliation`, `/admin/dead-letters`.
- Simuladores: `make faults s=spi|dict|bacen` mostra as falhas injetadas.

| Runbook | Alertas | Cenário |
|---|---|---|
| [SLO — experiência do pagador](slo-payer-experience.md) | PixPayerExperience (SLO: 99% dos Pix em até 10s) | 2, 3 |
| [SLO — iniciação](slo-initiation.md) | PixInitiationLatency (SLO: 95% dos pedidos em até 1,5s) | 4 |
| [SLO — disponibilidade da API](slo-availability.md) | PixApiAvailability (SLO: 99,5% das requisições sem 5xx) | 4 |
| [SLO — taxa de sucesso](slo-success-rate.md) | PixSuccessRate (SLO: 99% dos Pix sem rejeição técnica) | 2, 3, 5 |
| [SLO — prazo de liquidação (regulador)](slo-settlement.md) | PixSettlementDeadline (SLO: 99,9% das liquidações em até 40s) | 5 |
| [Outbox acumulando](outbox-backlog.md) | OutboxBacklog (mais de 100 eventos sem publicar por 1 min) | 1, 2 |
| [Fila do RabbitMQ acumulando ou mensagens velhas](queue-backlog.md) | QueueBacklog, QueueMessageAgeHigh | 1 |
| [DLQ, retries e mensagens estacionadas](dead-letter-queue.md) | MessageRetryRateHigh, MessagesDeadLettered, DeadLetterQueueNotEmpty, DeadLettersParked | 2 |
| [Pix parados em estado não terminal](stuck-payments.md) | PaymentsStuck, PaymentsStuckMany | 3 |
| [Circuit breaker do DICT aberto](dict-breaker.md) | DictCircuitBreakerOpen | 4 |
| [Divergência na reconciliação da conta PI](reconciliation-divergence.md) | ReconciliationDivergence, ReconciliationStale | 2, 3 |
| [Liquidez da conta PI](pi-liquidity.md) | PILiquidityLow, PixRejectedForLiquidity | 5 |
| [Relato de infração (MED) fora do prazo](med-infraction.md) | InfractionAnalysisOverdue | — |
| [Contêiner morto por memória ou reiniciando](container-oomkilled.md) | ContainerOOMKilled, ContainerRestartedNearMemoryLimit, ContainerRestarting, ContainerMemoryNearLimit | 1 |
| [Alvo de monitoramento fora do ar](target-down.md) | TargetDown | — |

Os cenários estão em [`scenarios/`](../../scenarios/README.md): cada um reproduz um incidente e confere que os alertas esperados disparam.

Este diretório é gerado por `scripts/gen_runbooks.py`.
