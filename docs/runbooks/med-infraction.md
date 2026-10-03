# Relato de infração (MED) fora do prazo

**Alertas:** InfractionAnalysisOverdue  
**Cenário de reprodução:** nenhum

## Sintoma e impacto

Um relato de infração do MED está aberto (PSP do pagador) ou com o valor bloqueado (PSP recebedor) além do prazo de análise. Enquanto isso o dinheiro da vítima segue parado na conta do recebedor, e o prazo é obrigação regulatória.

## Diagnóstico

1. `curl localhost:8082/v1/infraction-reports?state=BLOCKED` (PSP B; 8081 para o A) lista o que está bloqueado esperando decisão; o campo `deadline` mostra o prazo.
2. No DICT: `curl -H 'PI-RequestingParticipant: 22222222' localhost:8092/api/v1/infraction-reports?state=OPEN` (relatos a analisar pelo PSP 22222222); `overdue: true` marca os vencidos.
3. O ciclo do MED roda no resolver (`MED_INTERVAL`, 10s): `docker compose logs psp-b-resolver | grep infra`. Relato que não aparece como `BLOCKED` no recebedor ainda não foi trazido do DICT.
4. Decisão tomada e não enviada: o relato local fica `AGREED`/`DISAPPROVED` e o DICT `OPEN`. O resolver reenvia a cada ciclo; se não vai, o DICT está fora.

## Mitigação

- Analista decide: `POST /v1/infraction-reports/{id}/decision` com `{"result":"AGREE"}` (devolve o valor bloqueado por uma devolução especial `pacs.004` FR01) ou `DISAGREE` (libera o valor).
- Se o DICT estiver fora, a decisão fica registrada localmente e segue quando ele voltar.

## Depois

- Depois de `AGREE`, conferir que o relato chega a `RETURNED` nos dois PSPs; `RETURN_FAILED` exige ação manual (a reserva voltou ao saldo disponível).
- Limitação do laboratório: o bloqueio só alcança o saldo da conta que recebeu o Pix; o MED 2.0 real rastrearia o dinheiro já enviado a outras contas.
