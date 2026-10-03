# Outbox acumulando

**Alertas:** OutboxBacklog (mais de 100 eventos sem publicar por 1 min)  
**Cenário de reprodução:** 1, 2 (`scenarios/`)

## Sintoma e impacto

O relay não está publicando no RabbitMQ no ritmo em que a API grava. Os Pix ficam `PENDING` com a reserva feita.

## Diagnóstico

1. `docker compose ps psp-a-relay`: o relay está de pé? Logs: `docker compose logs psp-a-relay | tail`.
2. RabbitMQ saudável? Se recusa publicações (alarme de memória/disco), o publish falha e o evento fica para a varredura: *Eventos publicados pelo relay* com `result=error`.
3. Atraso do relay (`outbox_publish_lag_seconds`, painel *Outbox*): alto com o RabbitMQ saudável aponta para o MongoDB (change stream lento).
4. Contagem exata: `docker compose exec mongo mongosh --eval "db.getSiblingDB('psp_a').outbox.countDocuments({publishedAt: null})"`.

## Mitigação

- Subir o relay (`docker compose up -d psp-a-relay`). Ele publica o acumulado ao iniciar; duplicatas são absorvidas pelos consumidores.
- Se for o RabbitMQ, restabelecê-lo primeiro.

## Depois

- Nenhuma perda: a outbox é a fonte da verdade. Conferir que os Pix pendentes terminaram (`PaymentsStuck` zerado).
