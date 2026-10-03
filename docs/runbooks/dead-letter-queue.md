# DLQ, retries e mensagens estacionadas

**Alertas:** MessageRetryRateHigh, MessagesDeadLettered, DeadLetterQueueNotEmpty, DeadLettersParked  
**Cenário de reprodução:** 2 (`scenarios/`)

## Sintoma e impacto

Mensagens estão falhando. Falha transitória segue as filas de retry (5s, 30s, 2min); esgotadas, vão à DLQ; o resolver devolve da DLQ até 3 vezes e depois as estaciona em `dead_letters`.

## Diagnóstico

1. `MessageRetryRateHigh`: uma dependência está instável. O erro está no log do consumidor (`message="falha ao processar mensagem"`, campo `err`) e no header `x-last-error` da mensagem.
2. Ver a mensagem na DLQ sem consumir: interface do RabbitMQ (`localhost:15672`, usuário `lightpix`) → fila `<fila>.dlq` → *Get messages* (Nack requeue).
3. `DeadLettersParked` (crítico): `curl localhost:8083/admin/dead-letters` lista corpo, fila e último erro. Mensagem estacionada de `spi.out` com `pacs.008` já rejeitou o Pix (ED05) se o SPI nunca a recebeu.
4. Erro permanente (mensagem malformada) nunca passa: corrigir a origem.

## Mitigação

- Falha transitória: tratar a dependência (SPI, DICT, MongoDB); o resolver reprocessa a DLQ a cada minuto sem ação manual.
- Estacionada: corrigir a causa e republicar manualmente o corpo no exchange `pix` com a chave da fila de origem, ou descartar se o Pix já foi resolvido.

## Depois

- Conferir a reconciliação (`reconciliation-divergence.md`): mensagem perdida pode ter deixado Pix sem liquidação local.
