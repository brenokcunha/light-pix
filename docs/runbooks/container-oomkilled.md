# Contêiner morto por memória ou reiniciando

**Alertas:** ContainerOOMKilled, ContainerRestartedNearMemoryLimit, ContainerRestarting, ContainerMemoryNearLimit  
**Cenário de reprodução:** 1 (`scenarios/`)

## Sintoma e impacto

Um contêiner estourou o limite de memória e foi morto (OOMKilled), ou reinicia repetidamente. O mini-case clássico: vazamento de memória no spi-connector, a fila `spi.out` sobe a cada reinício.

## Diagnóstico

1. Qual serviço? O alerta traz o nome do serviço. `docker inspect <contêiner> --format '{{.State.OOMKilled}} {{.RestartCount}}'`. `ContainerOOMKilled` vem do evento do kernel (cAdvisor); no Docker Desktop esse evento não aparece, então `ContainerRestartedNearMemoryLimit` infere o OOM (a memória chegou perto do limite e depois caiu) e `ContainerRestarting` conta os reinícios pelo reset dos contadores do próprio processo.
2. Memória: *Pix — serviços* → *Memória (working set) e limite*: uma rampa até o limite seguida de queda é vazamento; um degrau único é pico de carga.
3. Memória do runtime Go: *Memória do runtime Go (heap + stacks)*: se sobe sem descer depois do GC, há objetos retidos (vazamento). Um perfil `pprof` confirmaria; no laboratório, o vazamento é injetado por `LEAK_KB_PER_MESSAGE`.
4. Efeito: *Mensagens prontas por fila* (`spi.out` sobe nos reinícios) e *Reinícios e OOM*.

## Mitigação

- Imediato: subir o limite de memória ou reiniciar; mensagens não confirmadas voltam à fila e nada se perde (o SPI deduplica por `EndToEndId`).
- Definitivo: achar e corrigir o vazamento. No cenário, remover a variável `LEAK_KB_PER_MESSAGE`.

## Depois

- Manter `ContainerMemoryNearLimit` como aviso antecipado: ele dispara antes do OOM.
