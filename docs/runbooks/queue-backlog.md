# Fila do RabbitMQ acumulando ou mensagens velhas

**Alertas:** QueueBacklog, QueueMessageAgeHigh  
**Cenário de reprodução:** 1 (`scenarios/`)

## Sintoma e impacto

Uma fila de trabalho (`spi.out`, `pix.results`, `pix.incoming`) tem mensagens esperando: o consumidor está parado, lento ou dependendo de algo lento.

## Diagnóstico

1. Qual fila? `spi.out` cheia: o conector não consegue enviar ao SPI (lento ou reiniciando). `pix.results`/`pix.incoming`: o worker.
2. Consumidores: `rabbitmq_queue_consumers{queue="spi.out"}` deve ser 1 por vhost. Zero = o processo caiu.
3. Reinícios: `ContainerRestarting` / `ContainerOOMKilled` no conector explicam a fila subindo e descendo (cenário do vazamento de memória).
4. SPI lento: o envio espera a resposta do SPI (a `pacs.002` dispara a liquidação). Aumentar `WORKER_CONCURRENCY` do conector dá vazão enquanto o SPI aguenta.

## Mitigação

- Subir ou reiniciar o consumidor. Mensagens não confirmadas voltam à fila sozinhas.
- Memória: ver `container-oomkilled.md`.

## Depois

- Idade das mensagens volta ao normal? `broker_message_age_seconds` P99 abaixo de 1s é o esperado.
