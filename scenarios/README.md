# Cenários de incidente

Cada cenário reproduz um incidente com carga (k6) e falhas injetadas, confere que
**os alertas certos disparam**, desfaz a falha e confere que o sistema **se recupera
sozinho** (filas e DLQs vazias, nenhum Pix parado, conta PI conciliada com os PSPs).
Saem com status ≠ 0 se algo não bater.

Pré-requisito: a stack no ar (`make up`). Cada cenário leva de 6 a 10 minutos;
antes de começar ele espera os alertas críticos anteriores resolverem.

```bash
make scenarios                 # os cinco, em sequência
make scenarios n="2 4"         # só o 2 e o 4
bash scenarios/03-pacs002-perdidas.sh
```

Para acompanhar, deixe aberto o Grafana (`localhost:3000`, dashboards **Pix — visão
IGA** e **Pix — serviços**) e o Prometheus em *Alerts* (`localhost:9090/alerts`).

| # | Cenário | Falha | Alertas esperados | Runbooks |
|---|---|---|---|---|
| 1 | [Vazamento de memória no connector](01-leak-oom.sh) | `LEAK_KB_PER_MESSAGE` + limite de 96 MiB no conector do PSP A → OOMKilled em ciclo | `ContainerRestarting`, `ContainerRestartedNearMemoryLimit` | [container-oomkilled](../docs/runbooks/container-oomkilled.md) |
| 2 | [SPI intermitente](02-spi-intermitente.sh) | 30% de 503 no spi-sim → retries, DLQ, reprocessamento | `MessageRetryRateHigh`, `MessagesDeadLettered`, `PixPayerExperience` (página) | [dead-letter-queue](../docs/runbooks/dead-letter-queue.md), [slo-payer-experience](../docs/runbooks/slo-payer-experience.md) |
| 3 | [pacs.002 perdidas](03-pacs002-perdidas.sh) | 30% das `pacs.002` somem no SPI → Pix parados até o resolver | `PaymentsStuck`, `PixPayerExperience` (página) | [stuck-payments](../docs/runbooks/stuck-payments.md) |
| 4 | [DICT lento](04-dict-lento.sh) | 2s de latência no dict-sim → circuit breaker abre, 503 rápido | `DictCircuitBreakerOpen`, `PixApiAvailability` (página) | [dict-breaker](../docs/runbooks/dict-breaker.md), [slo-availability](../docs/runbooks/slo-availability.md) |
| 5 | [Conta PI sem saldo](05-pi-sem-saldo.sh) | retirada STR deixa R$ 20 na conta PI do PSP A → `AM04` | `PILiquidityLow`, `PixRejectedForLiquidity`, `PixSuccessRate` (página) | [pi-liquidity](../docs/runbooks/pi-liquidity.md), [slo-success-rate](../docs/runbooks/slo-success-rate.md) |

## O que cada cenário mostra

1. **OOMKilled.** O mini-case: um vazamento lento vira reinícios em ciclo. O Pix
   segue (as mensagens não confirmadas voltam à fila e o SPI deduplica), mas a fila
   `spi.out` sobe a cada reinício. O alerta de memória inferido
   (`ContainerRestartedNearMemoryLimit`) existe porque o cAdvisor do Docker Desktop
   não registra o evento de OOM.
2. **SPI intermitente.** Os retries (5s, 30s, 2min) absorvem a maioria; quem esgota
   vai à DLQ e o resolver devolve. O custo aparece como Pix lentos e orçamento de
   erro queimado, não como Pix perdidos.
3. **pacs.002 perdidas.** A transação liquida, mas ninguém avisa. Só o resolver, que
   pergunta o status ao SPI depois de 40s, fecha o Pix. O dinheiro fica reservado
   nesse intervalo.
4. **DICT lento.** O breaker troca "esperar 2,4s e falhar" por "falhar na hora". A
   API continua respondendo, mas a disponibilidade cai. Quando o DICT volta, o
   breaker fecha sozinho.
5. **Conta PI sem saldo.** Não é saldo do cliente (esse a API recusa na hora): é o
   PSP sem liquidez junto ao regulador. Os Pix saem rejeitados com `AM04`.

## Como funcionam

- `lib.sh` é o esqueleto (condição prévia → falha → carga → alertas → recuperação →
  reconciliação). Cada script só define a falha (`inject`/`restore`) e os alertas esperados.
- `check.py` consulta o Prometheus: `quiet` (sem alertas graves), `watch` (espera os
  alertas esperados), `drained` (sistema parado) e `reconcile` (conta PI × PSPs).
- O cenário 1 usa `01-leak.compose.yaml`, que sobrepõe o conector do PSP A
  (`mem_limit`, `memswap_limit` e `LEAK_KB_PER_MESSAGE`); sem desligar o swap o
  contêiner passaria do limite trocando para disco e nunca seria morto.
- Os alertas de SLO dependem de janelas de burn rate curtas (`deploy/slo/windows/30d.yaml`),
  para poucos minutos de falha dispararem a página.
