#!/usr/bin/env python3
"""Gera as especificações Sloth dos SLOs do Pix (um arquivo por PSP).

    python scripts/gen_slo.py        # escreve deploy/slo/specs/slo-pix-psp-*.yml
    make slo                         # gera as especificações e as regras do Prometheus

As regras (recording rules e alertas por burn rate) saem do Sloth e vão para
deploy/observability/prometheus/rules/. Os alertas apontam para o runbook em
docs/runbooks/.
"""
from pathlib import Path

PSPS = {"11111111": "psp-a", "22222222": "psp-b"}
OUT = Path(__file__).resolve().parent.parent / "deploy" / "slo" / "specs"

HEADER = """# Gerado por scripts/gen_slo.py; não edite à mão.
version: "prometheus/v1"
service: "pix-{name}"
labels:
  psp_ispb: "{ispb}"
  psp_name: "{name}"
slos:
"""

SLO = """  - name: "{slo}"
    objective: {objective}
    description: "{description}"
    sli:
      events:
        error_query: |
{error_query}
        total_query: |
{total_query}
    alerting:
      name: "{alert}"
      labels:
        category: "pix"
      annotations:
        summary: "{summary}"
        runbook: "docs/runbooks/{runbook}.md"
      page_alert:
        labels:
          severity: critical
      ticket_alert:
        labels:
          severity: warning
"""


def indent(q: str) -> str:
    return "\n".join("          " + line for line in q.strip().splitlines())


def slos(ispb: str):
    pix = f'psp_ispb="{ispb}"'
    api = f'job="light-pix/payment-api", {pix}'
    create = f'{api}, http_request_method="POST", http_route="/v1/pix/payments"'
    yield dict(
        slo="payer-experience", objective=99.0, alert="PixPayerExperience", runbook="slo-payer-experience",
        description="99% dos Pix enviados chegam ao estado final em até 10s (experiência do pagador, do pedido ao resultado)",
        summary="Pix do PSP {{ $labels.psp_name }} acima de 10s além do orçamento de erro",
        total=f'sum(rate(pix_payment_duration_seconds_count{{{pix}}}[{{{{.window}}}}]))',
        error=f'sum(rate(pix_payment_duration_seconds_count{{{pix}}}[{{{{.window}}}}])) - sum(rate(pix_payment_duration_seconds_bucket{{{pix}, le="10"}}[{{{{.window}}}}]))',
    )
    yield dict(
        slo="initiation", objective=95.0, alert="PixInitiationLatency", runbook="slo-initiation",
        description="95% dos pedidos de Pix aceitos pela API em até 1,5s (iniciação)",
        summary="Iniciação de Pix lenta no PSP {{ $labels.psp_name }}",
        total=f'sum(rate(http_server_request_duration_seconds_count{{{create}}}[{{{{.window}}}}]))',
        error=f'sum(rate(http_server_request_duration_seconds_count{{{create}}}[{{{{.window}}}}])) - sum(rate(http_server_request_duration_seconds_bucket{{{create}, le="1.5"}}[{{{{.window}}}}]))',
    )
    yield dict(
        slo="api-availability", objective=99.5, alert="PixApiAvailability", runbook="slo-availability",
        description="99,5% das requisições à payment-api sem erro 5xx",
        summary="payment-api do PSP {{ $labels.psp_name }} devolvendo erros 5xx",
        total=f'sum(rate(http_server_request_duration_seconds_count{{{api}, http_route!="/readyz"}}[{{{{.window}}}}]))',
        error=f'sum(rate(http_server_request_duration_seconds_count{{{api}, http_route!="/readyz", http_response_status_code=~"5.."}}[{{{{.window}}}}]))',
    )
    yield dict(
        slo="success-rate", objective=99.0, alert="PixSuccessRate", runbook="slo-success-rate",
        description="99% dos Pix enviados não são rejeitados por falha técnica (AB03 prazo, ED05 liquidação, AM04 liquidez do PSP)",
        summary="Pix do PSP {{ $labels.psp_name }} rejeitados por falha técnica",
        total=f'sum(rate(pix_payments_finished_total{{{pix}}}[{{{{.window}}}}]))',
        error=f'sum(rate(pix_payments_finished_total{{{pix}, state="REJECTED", reason=~"AB03|ED05|AM04"}}[{{{{.window}}}}]))',
    )
    # Visão do regulador (IGA), com o instante do SPI: prazo de liquidação de 40s.
    reg = f'ispb="{ispb}"'
    yield dict(
        slo="settlement", objective=99.9, alert="PixSettlementDeadline", runbook="slo-settlement",
        description="99,9% das liquidações em até 40s da chegada da pacs.008 ao SPI (IGA, visão do regulador)",
        summary="Liquidação do PSP {{ $labels.psp_name }} acima de 40s no SPI",
        total=f'sum(rate(iga_settlement_duration_seconds_count{{{reg}}}[{{{{.window}}}}]))',
        error=f'sum(rate(iga_settlement_duration_seconds_count{{{reg}}}[{{{{.window}}}}])) - sum(rate(iga_settlement_duration_seconds_bucket{{{reg}, le="40"}}[{{{{.window}}}}]))',
    )


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for ispb, name in PSPS.items():
        text = HEADER.format(ispb=ispb, name=name)
        for s in slos(ispb):
            text += SLO.format(
                slo=s["slo"], objective=s["objective"], description=s["description"],
                error_query=indent(s["error"]), total_query=indent(s["total"]),
                alert=s["alert"], summary=s["summary"], runbook=s["runbook"],
            )
        path = OUT / f"slo-pix-{name}.yml"
        path.write_text(text, encoding="utf-8", newline="\n")
        print("escrito", path.relative_to(OUT.parent.parent.parent))


if __name__ == "__main__":
    main()
