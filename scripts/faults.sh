#!/usr/bin/env bash
# Mostra, define ou limpa as falhas injetadas num simulador.
#
#   scripts/faults.sh spi                      mostra as falhas atuais
#   scripts/faults.sh spi '{"errorRate":0.3}'  define (substitui tudo)
#   scripts/faults.sh spi clear                desliga todas as falhas
#
# Simuladores: spi (8091), dict (8092), bacen (8093). Campos do JSON em
# docs/arquitetura.md, seção "Injeção de falhas".
set -euo pipefail

case "${1:-}" in
  spi) port=8091 ;;
  dict) port=8092 ;;
  bacen) port=8093 ;;
  *) echo "uso: $0 <spi|dict|bacen> [json|clear]" >&2; exit 2 ;;
esac
url="http://localhost:${port}/admin/faults"
arg="${2:-}"

if [ -z "$arg" ]; then
  curl -fsS "$url"
elif [ "$arg" = "clear" ]; then
  curl -fsS -X DELETE "$url"
else
  curl -fsS -X PUT -H 'Content-Type: application/json' -d "$arg" "$url"
fi
echo
