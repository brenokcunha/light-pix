# Divergência na reconciliação da conta PI

**Alertas:** ReconciliationDivergence, ReconciliationStale  
**Cenário de reprodução:** 2, 3 (`scenarios/`)

## Sintoma e impacto

A reconciliação achou diferença entre o extrato da conta PI (a verdade do regulador) e os pagamentos liquidados do PSP. Pode ser dinheiro liquidado que o PSP não reconhece (ou o contrário).

## Diagnóstico

1. `curl localhost:8083/admin/reconciliation` devolve o último relatório com cada divergência (`endToEndId`, valores, estado local).
2. `PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT` com estado local `OUT/SENT` ou `IN/ACCEPTED`: a confirmação se perdeu e o resolver ainda não fechou. Rodar de novo depois do resolver costuma limpar.
3. `PI_ENTRY_WITHOUT_LOCAL_SETTLEMENT` com `MISSING`: o PSP nunca teve o Pix (liquidação feita por fora do PSP, ou perda de dados).
4. `LOCAL_SETTLEMENT_WITHOUT_PI_ENTRY`: o PSP deu como liquidado algo que a conta PI não tem. Sério: investigar o `endToEndId` no SPI (`GET /api/v1/status/{id}`).
5. `AMOUNT_MISMATCH` / `DIRECTION_MISMATCH`: erro de dados; comparar o pagamento com a `pacs.008` original.

## Mitigação

- Esperar o resolver e conciliar de novo: `curl -X POST "localhost:8083/admin/reconciliation?from=...&to=..."` (datas RFC 3339).
- Divergências que persistem exigem ajuste manual aprovado (estorno/crédito pelo extrato) — fora do escopo do laboratório.

## Depois

- Registrar o `endToEndId` e a causa. `ReconciliationStale`: verificar se o resolver está de pé (a conciliação diária roda às 03:00 UTC).
