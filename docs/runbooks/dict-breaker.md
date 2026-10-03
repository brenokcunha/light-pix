# Circuit breaker do DICT aberto

**Alertas:** DictCircuitBreakerOpen  
**Cenário de reprodução:** 4 (`scenarios/`)

## Sintoma e impacto

O gateway do DICT abriu o circuit breaker (5 falhas seguidas): Pix por chave falham na hora com `503 DICT_UNAVAILABLE` em vez de esperar. Depois de 15s o breaker deixa 3 chamadas de teste passarem.

## Diagnóstico

1. `curl localhost:8092/admin/faults` e `curl localhost:8092/readyz`: o dict-sim está lento, com erros ou fora do ar?
2. *Pix — serviços* → *Circuit breaker do DICT* (2 = aberto) e *DICT: consultas por resultado*.
3. O gateway tem cache de 10s para chaves já consultadas: chaves recentes ainda resolvem com o DICT fora; as novas, não.

## Mitigação

- Restabelecer o DICT (`make faults s=dict j=clear` se for falha injetada).
- Não é preciso reiniciar a API: o breaker fecha sozinho quando o DICT volta.

## Depois

- Pedidos recusados com 503 não deixam reserva. O cliente pode repetir com a mesma `Idempotency-Key`.
- Avaliar se timeout (800ms) e tentativas (3) combinam com a latência normal do DICT.
