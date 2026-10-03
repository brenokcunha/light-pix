# Módulos do sistema

Dois mapas do mesmo sistema: os **pacotes Go** (quem depende de quem no código) e os
**componentes em execução** (quem fala com quem quando a stack está no ar). Os detalhes
de cada parte estão em [arquitetura.md](arquitetura.md).

## 1. Pacotes Go

Cada seta é um `import`. Ficaram de fora, para não poluir, as dependências que quase todos
têm de `config`, `telemetry` e `httpx`. A regra de leitura: o que está embaixo não conhece o
que está em cima; `clients` e `psp` só importam os **tipos** dos simuladores (os simuladores
nunca importam `clients`, o que evitaria um ciclo).

```mermaid
flowchart TB
    subgraph bin["Binários (cmd/)"]
        cpsp["cmd/psp<br/>api · relay · connector · worker · resolver"]
        cspi["cmd/spi-sim"]
        cdict["cmd/dict-sim"]
        cbacen["cmd/bacen-sim"]
    end

    app["internal/app<br/>esqueleto dos simuladores:<br/>subcomandos, Mongo, health, faults"]

    subgraph psp["O PSP"]
        pspcore["internal/psp<br/>contas e razão, Pix enviado e recebido,<br/>devolução, MED, resolver,<br/>reconciliação, IGA"]
        outbox["internal/outbox<br/>outbox transacional + relay<br/>(change stream)"]
        broker["internal/broker<br/>RabbitMQ: topologia, publisher,<br/>consumidor, retry e DLQ"]
        clients["internal/clients<br/>clientes HTTP: DICT (cache, retry,<br/>circuit breaker), SPI, Bacen"]
    end

    subgraph sims["Simuladores do BCB"]
        simspi["internal/sim/spi<br/>ICOM, liquidação, devolução,<br/>mailbox com lease"]
        simdict["internal/sim/dict<br/>chaves, rate limit, relatos (MED)"]
        simbacen["internal/sim/bacen<br/>conta PI, STR, extrato, IGA"]
    end

    subgraph dominio["Domínio e protocolo"]
        iso["internal/iso20022<br/>pacs.008 · pacs.002 · pacs.004"]
        pix["internal/pix<br/>Amount, EndToEndId, chaves"]
    end

    subgraph infra["Infraestrutura comum"]
        store["internal/store<br/>MongoDB: conexão, transações,<br/>tracing dos comandos"]
        resil["internal/resilience<br/>retry com jitter, circuit breaker"]
        faults["internal/faults<br/>falhas injetadas"]
        httpx["internal/httpx · config · telemetry"]
    end

    cpsp --> pspcore & outbox & broker & clients
    cspi & cdict & cbacen --> app
    app --> simspi & simdict & simbacen
    app --> faults & store

    pspcore --> outbox & broker & clients & store
    outbox --> broker & store
    clients --> resil
    clients -. tipos .-> simspi & simdict & simbacen
    pspcore -. tipos .-> simdict & simbacen

    simspi --> faults & simbacen
    simspi & simdict & simbacen --> store

    pspcore & clients & simspi --> iso
    iso & simspi & simdict & simbacen & pspcore & clients --> pix
```

## 2. Componentes em execução

Os dois PSPs (`psp-a`, ISPB 11111111, e `psp-b`, ISPB 22222222) rodam o mesmo código, cada
um com banco e vhost próprios. As setas contínuas são o caminho de um Pix; as pontilhadas são
processos de apoio (reprocessamento, conciliação, medição, MED).

```mermaid
flowchart LR
    cliente(["cliente"])

    subgraph PSP["PSP (psp-a e psp-b: mesmo código, banco e vhost próprios)"]
        api["payment-api<br/>pedido, idempotência,<br/>reserva de saldo"]
        relay["outbox-relay"]
        connector["spi-connector<br/>envio e long-polling"]
        worker["pix-worker<br/>resultados e<br/>Pix/devoluções recebidos"]
        resolver["pending-resolver<br/>pendências · DLQ · conciliação<br/>IGA · MED"]
    end

    subgraph dados["Dados e mensageria"]
        mongo[("MongoDB<br/>um banco por PSP<br/>e por simulador")]
        rabbit{{"RabbitMQ<br/>spi.out · pix.results<br/>pix.incoming · retry · DLQ"}}
    end

    subgraph bcb["Simuladores do BCB"]
        dict["dict-sim<br/>chaves e MED"]
        spi["spi-sim<br/>roteia e liquida"]
        bacen["bacen-sim<br/>conta PI e IGA"]
    end

    subgraph obs["Observabilidade"]
        otel["OTel Collector<br/>(tail sampling)"]
        prom["Prometheus<br/>Alertmanager"]
        tempo["Tempo"]
        loki["Loki + Alloy"]
        graf["Grafana"]
    end

    cliente --> api
    api -- "consulta a chave" --> dict
    api -- "transação:<br/>saldo, pagamento, outbox" --> mongo
    mongo -- "change stream" --> relay
    relay -- "publica (confirm)" --> rabbit
    rabbit -- "spi.out" --> connector
    connector -- "pacs.008 / pacs.004 / pacs.002" --> spi
    spi -- "mensagens destinadas<br/>ao PSP" --> connector
    connector --> rabbit
    rabbit -- "pix.results · pix.incoming" --> worker
    worker --> mongo
    spi -- "liquidação" --> bacen
    spi --> mongo
    dict --> mongo
    bacen --> mongo

    resolver -.-> spi
    resolver -.-> bacen
    resolver -.-> dict
    resolver -.-> rabbit
    resolver -.-> mongo
    spi -. "instantes (IGA)" .-> bacen

    PSP -. "OTLP" .-> otel
    bcb -. "OTLP" .-> otel
    otel --> prom
    otel --> tempo
    prom --> graf
    tempo --> graf
    loki --> graf
```

## Como ler os dois juntos

| Componente | Binário | Pacote principal |
|---|---|---|
| payment-api | `cmd/psp api` | `internal/psp` (serviço e API HTTP) |
| outbox-relay | `cmd/psp relay` | `internal/outbox` |
| spi-connector | `cmd/psp connector` | `internal/psp` (`Connector`) e `internal/broker` |
| pix-worker | `cmd/psp worker` | `internal/psp` (`HandleResult`, `HandleIncoming`) |
| pending-resolver | `cmd/psp resolver` | `internal/psp` (`Resolver`, DLQ, conciliação, IGA, MED) |
| spi-sim | `cmd/spi-sim` | `internal/sim/spi` |
| dict-sim | `cmd/dict-sim` | `internal/sim/dict` |
| bacen-sim | `cmd/bacen-sim` | `internal/sim/bacen` |
