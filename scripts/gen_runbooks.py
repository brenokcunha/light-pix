#!/usr/bin/env python3
"""Gera docs/runbooks/*.md a partir de um catálogo único, para todos terem o
mesmo formato (sintoma, diagnóstico, mitigação, depois) e para o índice
nunca ficar fora de sincronia com os alertas.

    python scripts/gen_runbooks.py
"""
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "docs" / "runbooks"

# (arquivo, título, alertas, cenário, sintoma, diagnóstico[], mitigação[], depois[])
RUNBOOKS = [
    ("slo-payer-experience", "SLO — experiência do pagador",
     "PixPayerExperience (SLO: 99% dos Pix em até 10s)", "2, 3",
     "Mais de 1% dos Pix leva mais de 10s para chegar ao estado final (confirmado ou rejeitado). O pagador espera sem resposta.",
     ["Painel **Pix — visão IGA** → *Experiência do pagador*: o P99 passou de 10s? Compare PSP × regulador: se o regulador também vê, o atraso é real, não de medição.",
      "Onde o tempo se perde: abra um Pix lento em *Pix lentos (> 5s)* (Tempo). Procure o maior intervalo entre spans: `outbox.publish` (relay), `process spi.out` (conector), `POST /api/v1/in` (SPI), `process pix.results` (worker).",
      "Filas: `QueueBacklog`, `OutboxBacklog` ou `MessageRetryRateHigh` disparando indicam o estágio.",
      "SPI instável: *Pix — serviços* → *SPI: mensagens recebidas* e `curl localhost:8091/admin/faults` (alguma falha injetada ativa?).",
      "Pix parados: `PaymentsStuck`. Uma `pacs.002` perdida só fecha pelo resolver, depois de 40s — isso sozinho estoura os 10s."],
     ["Retries: o SPI está falhando; tratar a causa (ver `dead-letter-queue.md`). Os Pix voltam sozinhos nas tentativas de 5s, 30s e 2min.",
      "`pacs.002` perdidas: confirmar que o resolver roda (`docker compose ps psp-a-resolver`) e que `RESOLVER_MIN_AGE` não está alto demais.",
      "Atraso no relay ou no conector: ver `outbox-backlog.md` e `queue-backlog.md`."],
     ["Anotar a causa e se o limiar de 40s do resolver (`RESOLVER_MIN_AGE`) é adequado para a perda de `pacs.002`.",
      "Conferir `slo:period_error_budget_remaining:ratio` do SLO: quanto do orçamento de 30 dias o incidente consumiu."]),

    ("slo-initiation", "SLO — iniciação",
     "PixInitiationLatency (SLO: 95% dos pedidos em até 1,5s)", "4",
     "A API demora para aceitar o pedido de Pix. Antes de qualquer SPI: a iniciação é a API, o DICT e a gravação no MongoDB.",
     ["*Pix — visão IGA* → *Iniciação*: o P95 da API passou de 1,5s?",
      "DICT lento? *Pix — serviços* → *DICT: consultas por resultado* e `curl localhost:8092/admin/faults`. O gateway tem timeout de 800ms e 3 tentativas: um DICT lento custa até ~2,4s antes de o circuit breaker abrir.",
      "MongoDB saturado: *MongoDB: conexões e operações/s* e CPU do contêiner `mongo`. A reserva de saldo é uma transação com cinco escritas; contas muito disputadas geram conflitos de escrita.",
      "Trace de um `POST /v1/pix/payments` lento: o span `HTTP GET` (DICT) ou `commitTransaction` mostra onde está a espera."],
     ["DICT: ver `dict-breaker.md`.", "MongoDB: reduzir a carga (taxa de entrada) ou aumentar recursos; em produção, escalar o banco."],
     ["Se a iniciação degrada com a carga, comparar a taxa atual com os ~20 Pix/s que esta máquina sustenta (README, \"Falhas e carga\")."]),

    ("slo-availability", "SLO — disponibilidade da API",
     "PixApiAvailability (SLO: 99,5% das requisições sem 5xx)", "4",
     "A payment-api devolve erros 5xx. O `DICT_UNAVAILABLE` (503) conta aqui, assim como qualquer erro interno.",
     ["Qual código? No Loki: `{service_name=~\"psp-.*-api\"} |= \"erro interno\"` ou o campo `status` dos logs de acesso; no Prometheus: `sum by (http_response_status_code, http_route) (rate(http_server_request_duration_seconds_count{job=\"light-pix/payment-api\", http_response_status_code=~\"5..\"}[5m]))`.",
      "503 em `/v1/pix/payments` com o breaker aberto: é o DICT (`dict-breaker.md`).",
      "500: erro interno, geralmente MongoDB. `docker compose logs psp-a-api | tail`; `mongo` saudável? Conflitos de transação esgotados sob carga alta aparecem como 500."],
     ["DICT: `make faults s=dict j=clear` se for falha injetada; senão ver `dict-breaker.md`.", "MongoDB: reiniciar só se não responder; sob carga, reduzir a taxa."],
     ["Pedidos que falharam com 5xx não deixam reserva de saldo (a transação desfaz), então não há o que acertar no razão."]),

    ("slo-success-rate", "SLO — taxa de sucesso",
     "PixSuccessRate (SLO: 99% dos Pix sem rejeição técnica)", "2, 3, 5",
     "Pix estão sendo rejeitados por falha técnica: `AB03` (o recebedor não respondeu no prazo), `ED05` (falha de liquidação) ou `AM04` (falta de saldo na conta PI do PSP). Rejeições de negócio do recebedor (`AC03`, `AC06`) não contam.",
     ["Painel **Pix — visão IGA** → *Pix finalizados por estado e motivo*: qual motivo cresceu?",
      "`AB03`: o recebedor não respondeu em 10s. O worker/conector do PSP recebedor está de pé? `docker compose ps`; fila `pix.incoming` do PSP recebedor (`QueueBacklog`). Em laboratório pode ser `creditorTimeoutRate` no SPI.",
      "`AM04`: ver `pi-liquidity.md`.",
      "`ED05`: o SPI não conseguiu liquidar no bacen-sim ou a mensagem foi estacionada; ver `dead-letter-queue.md`."],
     ["Tratar o motivo dominante conforme acima. Pix rejeitados já devolveram a reserva ao pagador; nada a corrigir no saldo."],
     ["Comparar com o SLO de liquidação (visão do regulador) para saber se o problema está no PSP ou no SPI."]),

    ("slo-settlement", "SLO — prazo de liquidação (regulador)",
     "PixSettlementDeadline (SLO: 99,9% das liquidações em até 40s)", "5",
     "A liquidação, medida pelo SPI da chegada da `pacs.008` até a liquidação, passa de 40s. Visão do regulador (IGA): o atraso é do SPI/bacen-sim, não do PSP.",
     ["*Pix — visão IGA* → *Liquidação no SPI e resposta do recebedor*. Se a resposta do recebedor está alta, o atraso é do PSP recebedor; se a liquidação está alta com resposta normal, é o SPI ou o bacen-sim.",
      "bacen-sim lento ou com erros: `curl localhost:8093/admin/faults`; *Bacen: liquidações por resultado*.",
      "O SPI repete liquidações paradas a cada `SPI_SETTLE_RETRY` (5s) enquanto o bacen-sim falha com erro temporário."],
     ["Limpar falhas injetadas no bacen-sim (`make faults s=bacen j=clear`) ou restabelecer o serviço.", "Liquidações em andamento terminam sozinhas quando o bacen-sim volta."],
     ["Conferir a reconciliação da conta PI depois do incidente (`reconciliation-divergence.md`)."]),

    ("outbox-backlog", "Outbox acumulando",
     "OutboxBacklog (mais de 100 eventos sem publicar por 1 min)", "1, 2",
     "O relay não está publicando no RabbitMQ no ritmo em que a API grava. Os Pix ficam `PENDING` com a reserva feita.",
     ["`docker compose ps psp-a-relay`: o relay está de pé? Logs: `docker compose logs psp-a-relay | tail`.",
      "RabbitMQ saudável? Se recusa publicações (alarme de memória/disco), o publish falha e o evento fica para a varredura: *Eventos publicados pelo relay* com `result=error`.",
      "Atraso do relay (`outbox_publish_lag_seconds`, painel *Outbox*): alto com o RabbitMQ saudável aponta para o MongoDB (change stream lento).",
      "Contagem exata: `docker compose exec mongo mongosh --eval \"db.getSiblingDB('psp_a').outbox.countDocuments({publishedAt: null})\"`."],
     ["Subir o relay (`docker compose up -d psp-a-relay`). Ele publica o acumulado ao iniciar; duplicatas são absorvidas pelos consumidores.", "Se for o RabbitMQ, restabelecê-lo primeiro."],
     ["Nenhuma perda: a outbox é a fonte da verdade. Conferir que os Pix pendentes terminaram (`PaymentsStuck` zerado)."]),

    ("queue-backlog", "Fila do RabbitMQ acumulando ou mensagens velhas",
     "QueueBacklog, QueueMessageAgeHigh", "1",
     "Uma fila de trabalho (`spi.out`, `pix.results`, `pix.incoming`) tem mensagens esperando: o consumidor está parado, lento ou dependendo de algo lento.",
     ["Qual fila? `spi.out` cheia: o conector não consegue enviar ao SPI (lento ou reiniciando). `pix.results`/`pix.incoming`: o worker.",
      "Consumidores: `rabbitmq_queue_consumers{queue=\"spi.out\"}` deve ser 1 por vhost. Zero = o processo caiu.",
      "Reinícios: `ContainerRestarting` / `ContainerOOMKilled` no conector explicam a fila subindo e descendo (cenário do vazamento de memória).",
      "SPI lento: o envio espera a resposta do SPI (a `pacs.002` dispara a liquidação). Aumentar `WORKER_CONCURRENCY` do conector dá vazão enquanto o SPI aguenta."],
     ["Subir ou reiniciar o consumidor. Mensagens não confirmadas voltam à fila sozinhas.", "Memória: ver `container-oomkilled.md`."],
     ["Idade das mensagens volta ao normal? `broker_message_age_seconds` P99 abaixo de 1s é o esperado."]),

    ("dead-letter-queue", "DLQ, retries e mensagens estacionadas",
     "MessageRetryRateHigh, MessagesDeadLettered, DeadLetterQueueNotEmpty, DeadLettersParked", "2",
     "Mensagens estão falhando. Falha transitória segue as filas de retry (5s, 30s, 2min); esgotadas, vão à DLQ; o resolver devolve da DLQ até 3 vezes e depois as estaciona em `dead_letters`.",
     ["`MessageRetryRateHigh`: uma dependência está instável. O erro está no log do consumidor (`message=\"falha ao processar mensagem\"`, campo `err`) e no header `x-last-error` da mensagem.",
      "Ver a mensagem na DLQ sem consumir: interface do RabbitMQ (`localhost:15672`, usuário `lightpix`) → fila `<fila>.dlq` → *Get messages* (Nack requeue).",
      "`DeadLettersParked` (crítico): `curl localhost:8083/admin/dead-letters` lista corpo, fila e último erro. Mensagem estacionada de `spi.out` com `pacs.008` já rejeitou o Pix (ED05) se o SPI nunca a recebeu.",
      "Erro permanente (mensagem malformada) nunca passa: corrigir a origem."],
     ["Falha transitória: tratar a dependência (SPI, DICT, MongoDB); o resolver reprocessa a DLQ a cada minuto sem ação manual.",
      "Estacionada: corrigir a causa e republicar manualmente o corpo no exchange `pix` com a chave da fila de origem, ou descartar se o Pix já foi resolvido."],
     ["Conferir a reconciliação (`reconciliation-divergence.md`): mensagem perdida pode ter deixado Pix sem liquidação local."]),

    ("stuck-payments", "Pix parados em estado não terminal",
     "PaymentsStuck, PaymentsStuckMany", "3",
     "Há Pix `PENDING`/`SENT` (enviados) ou `ACCEPTED` (recebidos) há mais que o limiar do resolver (40s). O dinheiro do pagador segue reservado.",
     ["O resolver está rodando? `docker compose logs psp-a-resolver | tail`; métrica `resolver_checked_total` por desfecho.",
      "Desfecho `in_flight`: o SPI ainda trabalha (aguardando o recebedor ou liquidando). Desfecho `not_found`: o SPI não conhece o Pix, a `pacs.008` ainda está na fila de retry ou na DLQ (ver `dead-letter-queue.md`) — o resolver não rejeita nesse caso, de propósito. Desfecho `error`: o SPI não respondeu ao status.",
      "`pacs.002` perdidas: o SPI liquidou e ninguém foi avisado; o resolver fecha sozinho no próximo ciclo (`confirmed`/`credited`). Confirmar com `make faults s=spi` se `dropPacs002` está ligado."],
     ["Em geral, esperar: o resolver roda a cada 10s.", "Resolver parado: `docker compose up -d psp-a-resolver`.", "`PaymentsStuckMany`: ver o motivo dominante em `resolver_checked_total`; se for `error`, o SPI está fora."],
     ["Garantir que `pix_payments_stuck` voltou a zero e rodar a reconciliação do dia."]),

    ("dict-breaker", "Circuit breaker do DICT aberto",
     "DictCircuitBreakerOpen", "4",
     "O gateway do DICT abriu o circuit breaker (5 falhas seguidas): Pix por chave falham na hora com `503 DICT_UNAVAILABLE` em vez de esperar. Depois de 15s o breaker deixa 3 chamadas de teste passarem.",
     ["`curl localhost:8092/admin/faults` e `curl localhost:8092/readyz`: o dict-sim está lento, com erros ou fora do ar?",
      "*Pix — serviços* → *Circuit breaker do DICT* (2 = aberto) e *DICT: consultas por resultado*.",
      "O gateway tem cache de 10s para chaves já consultadas: chaves recentes ainda resolvem com o DICT fora; as novas, não."],
     ["Restabelecer o DICT (`make faults s=dict j=clear` se for falha injetada).", "Não é preciso reiniciar a API: o breaker fecha sozinho quando o DICT volta."],
     ["Pedidos recusados com 503 não deixam reserva. O cliente pode repetir com a mesma `Idempotency-Key`.",
      "Avaliar se timeout (800ms) e tentativas (3) combinam com a latência normal do DICT."]),

    ("reconciliation-divergence", "Divergência na reconciliação da conta PI",
     "ReconciliationDivergence, ReconciliationStale", "2, 3",
     "A reconciliação achou diferença entre o extrato da conta PI (a verdade do regulador) e os pagamentos liquidados do PSP. Pode ser dinheiro liquidado que o PSP não reconhece (ou o contrário).",
     ["`curl localhost:8083/admin/reconciliation` devolve o último relatório com cada divergência (`endToEndId`, valores, estado local).",
      "`PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT` com estado local `OUT/SENT` ou `IN/ACCEPTED`: a confirmação se perdeu e o resolver ainda não fechou. Rodar de novo depois do resolver costuma limpar.",
      "`PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT` com `MISSING`: o PSP nunca teve o Pix (liquidação feita por fora do PSP, ou perda de dados).",
      "`LOCAL_SETTLEMENT_WITHOUT_PI_ENTRY`: o PSP deu como liquidado algo que a conta PI não tem. Sério: investigar o `endToEndId` no SPI (`GET /api/v1/status/{id}`).",
      "`AMOUNT_MISMATCH` / `DIRECTION_MISMATCH`: erro de dados; comparar o pagamento com a `pacs.008` original."],
     ["Esperar o resolver e conciliar de novo: `curl -X POST \"localhost:8083/admin/reconciliation?from=...&to=...\"` (datas RFC 3339).",
      "Divergências que persistem exigem ajuste manual aprovado (estorno/crédito pelo extrato) — fora do escopo do laboratório."],
     ["Registrar o `endToEndId` e a causa. `ReconciliationStale`: verificar se o resolver está de pé (a conciliação diária roda às 03:00 UTC)."]),

    ("pi-liquidity", "Liquidez da conta PI",
     "PILiquidityLow, PixRejectedForLiquidity", "5",
     "A conta PI do PSP está sem reserva para liquidar: os Pix saem rejeitados com `AM04` pelo SPI. Não é saldo do cliente (esse é recusado na API com `INSUFFICIENT_FUNDS`): é o PSP sem liquidez junto ao regulador.",
     ["Saldo: `curl localhost:8093/api/v1/pi-accounts/11111111` (e `22222222`); *Pix — serviços* → *Conta PI (saldo em R$)*.",
      "Quem esvaziou: o extrato `GET /api/v1/pi-accounts/{ispb}/statement` mostra as retiradas STR (`STR_OUT`) e as liquidações.",
      "Rejeições `AM04`: *Pix finalizados por estado e motivo*."],
     ["Aportar reserva (STR → PI): `curl -X POST localhost:8093/api/v1/pi-accounts/11111111/str-transfers -H 'Content-Type: application/json' -d '{\"transferId\":\"aporte-1\",\"direction\":\"IN\",\"amount\":500000.00}'`. O `transferId` é idempotente.",
      "Os Pix rejeitados não voltam sozinhos: o cliente precisa refazê-los (a reserva do cliente já foi devolvida)."],
     ["Definir um piso de liquidez e automatizar o aporte; o alerta dispara abaixo de R$ 10.000."]),

    ("container-oomkilled", "Contêiner morto por memória ou reiniciando",
     "ContainerOOMKilled, ContainerRestartedNearMemoryLimit, ContainerRestarting, ContainerMemoryNearLimit", "1",
     "Um contêiner estourou o limite de memória e foi morto (OOMKilled), ou reinicia repetidamente. O mini-case clássico: vazamento de memória no spi-connector, a fila `spi.out` sobe a cada reinício.",
     ["Qual serviço? O alerta traz o nome do serviço. `docker inspect <contêiner> --format '{{.State.OOMKilled}} {{.RestartCount}}'`. `ContainerOOMKilled` vem do evento do kernel (cAdvisor); no Docker Desktop esse evento não aparece, então `ContainerRestartedNearMemoryLimit` infere o OOM (a memória chegou perto do limite e depois caiu) e `ContainerRestarting` conta os reinícios pelo reset dos contadores do próprio processo.",
      "Memória: *Pix — serviços* → *Memória (working set) e limite*: uma rampa até o limite seguida de queda é vazamento; um degrau único é pico de carga.",
      "Memória do runtime Go: *Memória do runtime Go (heap + stacks)*: se sobe sem descer depois do GC, há objetos retidos (vazamento). Um perfil `pprof` confirmaria; no laboratório, o vazamento é injetado por `LEAK_KB_PER_MESSAGE`.",
      "Efeito: *Mensagens prontas por fila* (`spi.out` sobe nos reinícios) e *Reinícios e OOM*."],
     ["Imediato: subir o limite de memória ou reiniciar; mensagens não confirmadas voltam à fila e nada se perde (o SPI deduplica por `EndToEndId`).",
      "Definitivo: achar e corrigir o vazamento. No cenário, remover a variável `LEAK_KB_PER_MESSAGE`."],
     ["Manter `ContainerMemoryNearLimit` como aviso antecipado: ele dispara antes do OOM."]),

    ("target-down", "Alvo de monitoramento fora do ar",
     "TargetDown", "—",
     "O Prometheus não consegue raspar um alvo (`up == 0`) por 2 minutos.",
     ["`job` e `instance` do alerta dizem qual: `docker compose ps`.", "Serviços Go enviam métricas por OTLP (não são raspados): a falta deles aparece como métricas paradas, não como `TargetDown`."],
     ["`docker compose up -d <serviço>`; ver os logs do contêiner."],
     ["Se o alvo caiu por falta de memória, ver `container-oomkilled.md`."]),
]

INDEX_HEAD = """# Runbooks

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
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    index = INDEX_HEAD
    for slug, title, alerts, scenario, symptom, diag, mitig, after in RUNBOOKS:
        md = [f"# {title}", "", f"**Alertas:** {alerts}  ", f"**Cenário de reprodução:** {scenario if scenario != '—' else 'nenhum'}"
              + (" (`scenarios/`)" if scenario != "—" else ""), "",
              "## Sintoma e impacto", "", symptom, "", "## Diagnóstico", ""]
        md += [f"{i}. {d}" for i, d in enumerate(diag, 1)]
        md += ["", "## Mitigação", ""] + [f"- {m}" for m in mitig]
        md += ["", "## Depois", ""] + [f"- {a}" for a in after]
        (OUT / f"{slug}.md").write_text("\n".join(md) + "\n", encoding="utf-8", newline="\n")
        index += f"| [{title}]({slug}.md) | {alerts} | {scenario} |\n"
    index += ("\nOs cenários estão em [`scenarios/`](../../scenarios/README.md): cada um reproduz um incidente "
              "e confere que os alertas esperados disparam.\n\nEste diretório é gerado por `scripts/gen_runbooks.py`.\n")
    (OUT / "README.md").write_text(index, encoding="utf-8", newline="\n")
    print("escritos", len(RUNBOOKS) + 1, "arquivos em docs/runbooks")


if __name__ == "__main__":
    main()
