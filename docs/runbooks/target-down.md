# Alvo de monitoramento fora do ar

**Alertas:** TargetDown  
**Cenário de reprodução:** nenhum

## Sintoma e impacto

O Prometheus não consegue raspar um alvo (`up == 0`) por 2 minutos.

## Diagnóstico

1. `job` e `instance` do alerta dizem qual: `docker compose ps`.
2. Serviços Go enviam métricas por OTLP (não são raspados): a falta deles aparece como métricas paradas, não como `TargetDown`.

## Mitigação

- `docker compose up -d <serviço>`; ver os logs do contêiner.

## Depois

- Se o alvo caiu por falta de memória, ver `container-oomkilled.md`.
