#!/usr/bin/env python3
"""Gera os dashboards do Grafana (JSON provisionado).

    python scripts/gen_dashboards.py

Escreve em deploy/observability/grafana/dashboards/:
  - pix-iga.json       visão do IGA: experiência do pagador, iniciação, liquidação, SLOs
  - pix-servicos.json  saúde dos serviços: HTTP, filas, outbox, resolver, DICT, MongoDB, carga

O JSON gerado é o artefato versionado; este script só evita editar centenas de
linhas à mão.
"""
import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "deploy" / "observability" / "grafana" / "dashboards"
PROM = {"type": "prometheus", "uid": "prometheus"}
TEMPO = {"type": "tempo", "uid": "tempo"}

_next_id = 0


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id


def target(expr, legend="", ref="A", instant=False, fmt=None, ds=PROM):
    t = {"datasource": ds, "expr": expr, "legendFormat": legend, "refId": ref}
    if instant:
        t["instant"] = True
    if fmt:
        t["format"] = fmt
    return t


def steps(*pairs):
    """steps((None,'green'),(6,'orange'),(10,'red'))"""
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in pairs]}


def thresholds_lines(*vals):
    """Linhas pontilhadas de meta no gráfico."""
    return {"mode": "absolute", "steps": [{"color": "transparent", "value": None}] + [{"color": "red", "value": v} for v in vals]}


def ts(title, targets, x, y, w=12, h=8, unit="short", desc="", thresholds=None, stack=False, min_=None, max_=None, fill=10):
    custom = {"drawStyle": "line", "lineWidth": 1, "fillOpacity": fill, "showPoints": "never", "spanNulls": True}
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    if thresholds:
        custom["thresholdsStyle"] = {"mode": "dashed"}
    defaults = {"unit": unit, "custom": custom}
    if thresholds:
        defaults["thresholds"] = thresholds
    if min_ is not None:
        defaults["min"] = min_
    if max_ is not None:
        defaults["max"] = max_
    return {"id": _id(), "type": "timeseries", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def stat(title, expr, x, y, w=4, h=4, unit="short", th=None, desc="", legend=""):
    return {"id": _id(), "type": "stat", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [target(expr, legend)],
            "fieldConfig": {"defaults": {"unit": unit, "thresholds": th or steps((None, "green")),
                                         "color": {"mode": "thresholds"}, "noValue": "—"}, "overrides": []},
            "options": {"colorMode": "background", "graphMode": "area", "reduceOptions": {"calcs": ["lastNotNull"]}}}


def bargauge(title, expr, x, y, w=12, h=8, unit="percentunit", legend="", th=None, min_=0, max_=1, desc=""):
    return {"id": _id(), "type": "bargauge", "title": title, "description": desc, "datasource": PROM,
            "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [target(expr, legend, instant=True)],
            "fieldConfig": {"defaults": {"unit": unit, "min": min_, "max": max_,
                                         "thresholds": th or steps((None, "red"), (0.5, "orange"), (0.9, "green"))}, "overrides": []},
            "options": {"displayMode": "gradient", "orientation": "horizontal", "reduceOptions": {"calcs": ["lastNotNull"]}}}


def table(title, expr, x, y, w=12, h=8, desc="", ds=PROM, tempo=False):
    t = target(expr, ds=ds, instant=True, fmt="table")
    if tempo:
        t = {"datasource": TEMPO, "queryType": "traceql", "query": expr, "refId": "A", "limit": 20, "tableType": "traces"}
    return {"id": _id(), "type": "table", "title": title, "description": desc, "datasource": ds,
            "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": [t],
            "fieldConfig": {"defaults": {}, "overrides": []}, "options": {"showHeader": True}}


def row(title, y):
    return {"id": _id(), "type": "row", "title": title, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def text(content, x, y, w=24, h=3):
    return {"id": _id(), "type": "text", "title": "", "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": content}}


def var_query(name, label, query, multi=True, all_=True):
    return {"name": name, "label": label, "type": "query", "datasource": PROM,
            "query": {"query": query, "refId": "v"}, "refresh": 2, "sort": 1, "multi": multi, "includeAll": all_,
            "allValue": ".*", "current": {"selected": True, "text": "All", "value": "$__all"}}


def dashboard(uid, title, panels, variables, tags, desc, annotations=True):
    ann = [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
            "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"}]
    if annotations:
        ann += [
            {"name": "Falhas injetadas", "datasource": PROM, "enable": True, "iconColor": "orange",
             "expr": "sum(increase(faults_injected_total[1m])) > 0", "step": "60s", "titleFormat": "Falhas injetadas",
             "textFormat": "falhas ativas nos simuladores"},
            {"name": "Reinício de container", "datasource": PROM, "enable": True, "iconColor": "red",
             "expr": 'changes(container_start_time_seconds{container_label_com_docker_compose_project="light-pix", container_label_com_docker_compose_service!=""}[1m]) > 0',
             "step": "60s", "titleFormat": "Reinício", "textFormat": "{{container_label_com_docker_compose_service}}"},
        ]
    return {"uid": uid, "title": title, "description": desc, "tags": tags, "schemaVersion": 41, "version": 1,
            "editable": True, "graphTooltip": 1, "refresh": "10s", "time": {"from": "now-30m", "to": "now"},
            "timepicker": {}, "templating": {"list": variables}, "annotations": {"list": ann}, "panels": panels,
            "links": [{"title": "Runbooks (docs/runbooks)", "type": "link", "url": "https://github.com/", "icon": "doc",
                       "tooltip": "Os runbooks ficam em docs/runbooks no repositório"}][:0]}


def q(expr_hist, quantile, by="le"):
    return f"histogram_quantile({quantile}, sum by ({by}) ({expr_hist}))"


# ---------------------------------------------------------------------------
# Dashboard 1: visão IGA
# ---------------------------------------------------------------------------
def iga():
    psp = 'psp_ispb=~"$psp"'
    ispb = 'ispb=~"$psp"'
    create = f'job="light-pix/payment-api", {psp}, http_request_method="POST", http_route="/v1/pix/payments"'
    dur = f"rate(pix_payment_duration_seconds_bucket{{{psp}}}[$__rate_interval])"
    p = []
    y = 0
    p.append(text("**Visão IGA.** Compara o que o PSP mede (do pedido ao estado final) com o que o regulador enxerga "
                  "(instantes do SPI e relatórios dos PSPs no bacen-sim). Metas: experiência do pagador P50 ≤ 6s e P99 ≤ 10s; "
                  "iniciação P50 ≤ 0,9s e P95 ≤ 1,5s; liquidação ≤ 40s. Alertas e orçamento de erro vêm dos SLOs (Sloth).", 0, y, 24, 3))
    y += 3
    p += [
        stat("Pix finalizados/s", f"sum(rate(pix_payments_finished_total{{{psp}}}[$__rate_interval]))", 0, y, unit="reqps", th=steps((None, "blue"))),
        stat("Taxa de sucesso (5m)", f'sum(rate(pix_payments_finished_total{{{psp}, state="CONFIRMED"}}[5m])) / sum(rate(pix_payments_finished_total{{{psp}}}[5m]))',
             4, y, unit="percentunit", th=steps((None, "red"), (0.95, "orange"), (0.99, "green")),
             desc="Inclui rejeições do recebedor (AC03/AC06) e do cliente; as técnicas (AB03, ED05, AM04) estão no SLO de sucesso."),
        stat("P50 experiência do pagador", q(dur, 0.5), 8, y, unit="s", th=steps((None, "green"), (6, "red"))),
        stat("P99 experiência do pagador", q(dur, 0.99), 12, y, unit="s", th=steps((None, "green"), (10, "red"))),
        stat("P95 iniciação",
             q(f"rate(http_server_request_duration_seconds_bucket{{{create}}}[$__rate_interval])", 0.95), 16, y, unit="s", th=steps((None, "green"), (1.5, "red"))),
        stat("Pix parados", f"sum(pix_payments_stuck{{{psp}}})", 20, y, th=steps((None, "green"), (1, "orange"), (50, "red")),
             desc="Pix em estado não terminal além do limiar do resolver."),
    ]
    y += 4
    p.append(row("Experiência do pagador (t0 → t6a)", y)); y += 1
    p.append(ts("PSP × regulador: P50 e P99", [
        target(q(dur, 0.5, "le, psp_ispb"), "PSP P50 {{psp_ispb}}", "A"),
        target(q(dur, 0.99, "le, psp_ispb"), "PSP P99 {{psp_ispb}}", "B"),
        target(q(f"rate(iga_payer_experience_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.5, "le, ispb"), "regulador P50 {{ispb}}", "C"),
        target(q(f"rate(iga_payer_experience_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.99, "le, ispb"), "regulador P99 {{ispb}}", "D"),
    ], 0, y, unit="s", thresholds=thresholds_lines(6, 10),
        desc="O regulador usa o t0 e o t6a reportados pelo próprio PSP, mas manda o prazo no relógio do SPI para a iniciação e a liquidação."))
    p.append(ts("Distribuição (heatmap de buckets, % por faixa)", [
        target(f"sum by (le) (rate(pix_payment_duration_seconds_bucket{{{psp}}}[$__rate_interval]))", "{{le}}s", "A")],
        12, y, unit="reqps", fill=0, desc="Taxa acumulada por limite de bucket: 0,9s e 1,5s (iniciação), 6s e 10s (pagador), 40s (liquidação)."))
    y += 8
    p.append(row("Iniciação e liquidação", y)); y += 1
    p.append(ts("Iniciação (pedido → SPI): PSP × regulador", [
        target(q(f"rate(http_server_request_duration_seconds_bucket{{{create}}}[$__rate_interval])", 0.5, "le, psp_ispb"), "API P50 {{psp_ispb}}", "A"),
        target(q(f"rate(http_server_request_duration_seconds_bucket{{{create}}}[$__rate_interval])", 0.95, "le, psp_ispb"), "API P95 {{psp_ispb}}", "B"),
        target(q(f"rate(iga_initiation_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.5, "le, ispb"), "regulador P50 {{ispb}}", "C"),
        target(q(f"rate(iga_initiation_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.95, "le, ispb"), "regulador P95 {{ispb}}", "D"),
    ], 0, y, unit="s", thresholds=thresholds_lines(0.9, 1.5),
        desc="A API mede só o aceite do pedido; o regulador mede até a pacs.008 chegar ao SPI (inclui outbox, fila e conector)."))
    p.append(ts("Liquidação no SPI e resposta do recebedor", [
        target(q(f"rate(iga_settlement_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.5, "le, ispb"), "liquidação P50 {{ispb}}", "A"),
        target(q(f"rate(iga_settlement_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.99, "le, ispb"), "liquidação P99 {{ispb}}", "B"),
        target(q(f"rate(iga_creditor_response_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.5, "le, ispb"), "recebedor P50 {{ispb}}", "C"),
        target(q(f"rate(iga_creditor_response_duration_seconds_bucket{{{ispb}}}[$__rate_interval])", 0.99, "le, ispb"), "recebedor P99 {{ispb}}", "D"),
    ], 12, y, unit="s", thresholds=thresholds_lines(40)))
    y += 8
    p.append(row("Desfechos", y)); y += 1
    p.append(ts("Pix finalizados por estado e motivo", [
        target(f"sum by (state, reason) (rate(pix_payments_finished_total{{{psp}}}[$__rate_interval]))", "{{state}} {{reason}}", "A")],
        0, y, unit="reqps", stack=True, desc="Motivos: AB03 (recebedor não respondeu no prazo), AM04 (liquidez da conta PI), ED05 (falha de liquidação), AC03/AC06 (conta do recebedor)."))
    p.append(ts("Transações vistas pelo SPI (regulador) por desfecho", [
        target(f"sum by (ispb, status, reason) (rate(iga_transactions_total{{{ispb}}}[$__rate_interval]))", "{{ispb}} {{status}} {{reason}}", "A")],
        12, y, unit="reqps", stack=True))
    y += 8
    p.append(row("SLOs (Sloth)", y)); y += 1
    p.append(bargauge("Orçamento de erro restante (30d)", 'slo:period_error_budget_remaining:ratio{psp_ispb=~"$psp"}', 0, y,
                      legend="{{sloth_service}} · {{sloth_slo}}", min_=-1, max_=1,
                      th=steps((None, "red"), (0, "orange"), (0.5, "green")),
                      desc="1 = orçamento intacto; negativo = estourado. Com pouco tráfego o número oscila."))
    p.append(ts("Burn rate atual (1 = consumindo no ritmo do orçamento)", [
        target('slo:current_burn_rate:ratio{psp_ispb=~"$psp"}', "{{sloth_service}} · {{sloth_slo}}", "A")],
        12, y, unit="short", thresholds=thresholds_lines(14.4), desc="14,4 é o limite do alerta de página (taxa de queima rápida)."))
    y += 8
    p.append(table("Alertas disparando", 'ALERTS{alertstate="firing", category=~"pix|laboratorio"}', 0, y, 12, 8,
                   desc="O Alertmanager tem o histórico; cada alerta traz o runbook na anotação `runbook`."))
    p.append(table("Pix lentos (> 5s): abra o trace",
                   '{ resource.service.name = "payment-api" && name = "POST /v1/pix/payments" && traceDuration > 5s }',
                   12, y, 12, 8, ds=TEMPO, tempo=True,
                   desc="Cada linha é um Pix; o trace sai da API e passa pelos dois PSPs e pelos simuladores."))
    return dashboard("pix-iga", "Pix — visão IGA", p,
                     [var_query("psp", "PSP (ISPB)", "label_values(pix_payment_duration_seconds_count, psp_ispb)")],
                     ["pix", "iga", "slo"], "SLIs do Pix alinhados ao IGA: PSP × regulador, desfechos e SLOs.")


# ---------------------------------------------------------------------------
# Dashboard 2: serviços
# ---------------------------------------------------------------------------
def servicos():
    job = 'job=~"$job"'
    svc = 'container_label_com_docker_compose_service=~"$service"'
    cont = f'container_label_com_docker_compose_project="light-pix", {svc}'
    p = []
    y = 0
    p.append(row("HTTP (por serviço)", y)); y += 1
    p.append(ts("Requisições/s", [target(f'sum by (job) (rate(http_server_request_duration_seconds_count{{{job}, http_route!~"/readyz|/healthz"}}[$__rate_interval]))', "{{job}}")],
                0, y, 8, unit="reqps"))
    p.append(ts("Erros 5xx (% das requisições)", [target(
        f'sum by (job) (rate(http_server_request_duration_seconds_count{{{job}, http_response_status_code=~"5.."}}[$__rate_interval])) / sum by (job) (rate(http_server_request_duration_seconds_count{{{job}, http_route!~"/readyz|/healthz"}}[$__rate_interval]))',
        "{{job}}")], 8, y, 8, unit="percentunit", min_=0))
    p.append(ts("Latência P95", [target(q(f'rate(http_server_request_duration_seconds_bucket{{{job}, http_route!~"/readyz|/healthz|/api/v1/out/.*|/admin.*"}}[$__rate_interval])', 0.95, "le, job"), "{{job}}")],
                16, y, 8, unit="s"))
    y += 8
    p.append(row("Contêineres e Go", y)); y += 1
    p.append(ts("CPU (núcleos)", [target(f'sum by (container_label_com_docker_compose_service) (rate(container_cpu_usage_seconds_total{{{cont}}}[$__rate_interval]))', "{{container_label_com_docker_compose_service}}")],
                0, y, 8, unit="none"))
    p.append(ts("Memória (working set) e limite", [
        target(f'max by (container_label_com_docker_compose_service) (container_memory_working_set_bytes{{{cont}}})', "{{container_label_com_docker_compose_service}}", "A"),
        target(f'max by (container_label_com_docker_compose_service) (container_spec_memory_limit_bytes{{{cont}}} > 0)', "limite {{container_label_com_docker_compose_service}}", "B")],
        8, y, 8, unit="bytes", desc="Um contêiner com limite e memória subindo até ele termina em OOMKilled (cenário do vazamento no connector)."))
    p.append(ts("Reinícios e OOM (10 min)", [
        target(f'changes(container_start_time_seconds{{{cont}}}[10m])', "reinícios {{container_label_com_docker_compose_service}}", "A"),
        target(f'increase(container_oom_events_total{{{cont}}}[10m])', "OOM {{container_label_com_docker_compose_service}}", "B")],
        16, y, 8, unit="none", min_=0))
    y += 8
    p.append(ts("Memória do runtime Go (heap + stacks)", [target(f'sum by (job) (go_memory_used_bytes{{{job}}})', "{{job}}")],
                0, y, 12, unit="bytes", desc="Um vazamento aparece como rampa que não desce depois do GC (cenário 1: spi-connector)."))
    p.append(ts("Goroutines", [target(f'sum by (job) (go_goroutine_count{{{job}}})', "{{job}}")], 12, y, 12, unit="none"))
    y += 8
    p.append(row("Mensageria (RabbitMQ) e outbox", y)); y += 1
    p.append(ts("Mensagens prontas por fila", [target('sum by (vhost, queue) (rabbitmq_queue_messages_ready{queue!~".*\\\\.retry\\\\..*"})', "{{vhost}} {{queue}}")],
                0, y, 8, unit="none", desc="Inclui as DLQs; as filas de espera (retry) têm painel próprio."))
    p.append(ts("Filas de espera do retry", [target('sum by (vhost, queue) (rabbitmq_queue_messages_ready{queue=~".*\\\\.retry\\\\..*"})', "{{vhost}} {{queue}}")],
                8, y, 8, unit="none"))
    p.append(ts("Idade da mensagem ao consumir (P99)", [target(q('rate(broker_message_age_seconds_bucket[$__rate_interval])', 0.99, "le, queue, psp_ispb"), "{{psp_ispb}} {{queue}}")],
                16, y, 8, unit="s", thresholds=thresholds_lines(10)))
    y += 8
    p.append(ts("Consumo por resultado", [target('sum by (queue, result) (rate(broker_messages_processed_total[$__rate_interval]))', "{{queue}} {{result}}")],
                0, y, 8, unit="reqps", stack=True, desc="retry = falha transitória; dead_letter = foi para a DLQ."))
    p.append(ts("Outbox: pendentes e atraso do relay", [
        target('max by (psp_ispb) (outbox_pending)', "pendentes {{psp_ispb}}", "A"),
        target(q('rate(outbox_publish_lag_seconds_bucket[$__rate_interval])', 0.99, "le, psp_ispb"), "atraso P99 (s) {{psp_ispb}}", "B")],
        8, y, 8, unit="none"))
    p.append(ts("Eventos publicados pelo relay", [target('sum by (psp_ispb, result) (rate(outbox_published_total[$__rate_interval]))', "{{psp_ispb}} {{result}}")],
                16, y, 8, unit="reqps"))
    y += 8
    p.append(row("Resolver, reconciliação e resiliência", y)); y += 1
    p.append(ts("Pendências consultadas no SPI", [target('sum by (direction, outcome) (rate(resolver_checked_total[$__rate_interval]))', "{{direction}} {{outcome}}")],
                0, y, 8, unit="reqps", stack=True, desc="confirmed/credited/rejected = o resolver fechou o Pix; in_flight = o SPI ainda trabalha; not_found = o SPI não conhece."))
    p.append(ts("Pix parados e DLQ", [
        target('sum by (psp_ispb, state) (pix_payments_stuck)', "parados {{psp_ispb}} {{state}}", "A"),
        target('sum by (psp_ispb, queue) (increase(resolver_dlq_redriven_total[5m]))', "reprocessados {{psp_ispb}} {{queue}}", "B"),
        target('sum by (psp_ispb, queue) (increase(resolver_dlq_parked_total[5m]))', "estacionados {{psp_ispb}} {{queue}}", "C")],
        8, y, 8, unit="none"))
    p.append(ts("Circuit breaker do DICT", [target('max by (psp_ispb) (resilience_breaker_state{breaker="dict"})', "{{psp_ispb}}")],
                16, y, 8, unit="none", min_=0, max_=2, desc="0 fechado, 1 meio-aberto, 2 aberto."))
    y += 8
    p.append(stat("Divergências na última conciliação", 'max by (psp_ispb) (reconciliation_last_divergences)', 0, y, 6, 4,
                  th=steps((None, "green"), (1, "red")), legend="PSP {{psp_ispb}}"))
    p.append(stat("Idade da última conciliação", 'time() - max by (psp_ispb) (reconciliation_last_timestamp_seconds)', 6, y, 6, 4, unit="s",
                  th=steps((None, "green"), (26 * 3600, "red")), legend="PSP {{psp_ispb}}"))
    p.append(ts("Falhas injetadas nos simuladores", [target('sum by (kind) (rate(faults_injected_total[$__rate_interval]))', "{{kind}}")],
                12, y, 12, 4, unit="reqps", stack=True))
    y += 4
    p.append(row("Simuladores e liquidez", y)); y += 1
    p.append(ts("SPI: mensagens recebidas", [target('sum by (msg_type, result) (rate(spi_sim_messages_received_total[$__rate_interval]))', "{{msg_type}} {{result}}")],
                0, y, 8, unit="reqps", stack=True))
    p.append(ts("SPI: transações finalizadas e caixa de entrada", [
        target('sum by (status, reason) (rate(spi_sim_transactions_completed_total[$__rate_interval]))', "{{status}} {{reason}}", "A"),
        target('sum by (ispb) (spi_sim_mailbox_pending)', "pendentes na caixa {{ispb}}", "B")], 8, y, 8, unit="none"))
    p.append(ts("DICT: consultas por resultado", [target('sum by (result) (rate(dict_sim_lookups_total[$__rate_interval]))', "{{result}}")],
                16, y, 8, unit="reqps", stack=True))
    y += 8
    p.append(ts("Conta PI (saldo em R$)", [target('min by (ispb) (bacen_sim_pi_account_balance)', "{{ispb}}")],
                0, y, 12, unit="currencyBRL", thresholds=thresholds_lines(10000), desc="Abaixo de R$ 10.000 o alerta de liquidez dispara."))
    p.append(ts("Bacen: liquidações por resultado", [target('sum by (result) (rate(bacen_sim_settlements_total[$__rate_interval]))', "{{result}}")],
                12, y, 12, unit="reqps", stack=True))
    y += 8
    p.append(row("MongoDB e carga (k6)", y)); y += 1
    p.append(ts("MongoDB: conexões e operações/s", [
        target('max(mongodb_ss_connections{conn_type="current"})', "conexões", "A"),
        target('sum by (legacy_op_type) (rate(mongodb_ss_opcounters[$__rate_interval]))', "{{legacy_op_type}}/s", "B")], 0, y, 12, unit="none"))
    p.append(ts("k6: usuários virtuais e Pix até o fim (P99)", [
        target('max(k6_vus)', "VUs", "A"),
        target('max(k6_pix_end_to_end_seconds_p99) / 1000', "P99 fim a fim (s)", "B"),
        target('sum(rate(k6_pix_final_total_total[1m]))', "Pix finalizados/s", "C")], 12, y, 12, unit="none",
        desc="Só há dados durante `make load`."))
    return dashboard("pix-servicos", "Pix — serviços", p, [
        var_query("job", "Serviço (job)", 'label_values(http_server_request_duration_seconds_count{job=~"light-pix/.*"}, job)'),
        var_query("service", "Contêiner (compose)", 'label_values(container_memory_working_set_bytes{container_label_com_docker_compose_project="light-pix"}, container_label_com_docker_compose_service)'),
    ], ["pix", "servicos"], "Saúde dos serviços do laboratório: HTTP, contêineres, filas, outbox, resolver, simuladores e carga.")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for name, dash in (("pix-iga.json", iga()), ("pix-servicos.json", servicos())):
        (OUT / name).write_text(json.dumps(dash, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
        print("escrito", name, len(dash["panels"]), "painéis")


if __name__ == "__main__":
    main()
