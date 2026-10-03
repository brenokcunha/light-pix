#!/usr/bin/env bash
# Esqueleto dos cenários de incidente. Cada cenário define as variáveis e as
# funções abaixo e chama run_scenario:
#
#   TITLE, DESCRIPTION   o que o cenário reproduz
#   EXPECT               alertas esperados, separados por vírgula (Nome ou Nome:page)
#   RATE, DURATION       carga do k6 (Pix/s e duração), DIRECTION opcional
#   inject()             liga a falha
#   restore()            desfaz a falha (roda sempre, inclusive em caso de erro)
#
# O cenário: confere que não há alerta crítico, liga a falha, aplica carga,
# espera os alertas esperados, desfaz a falha, espera o sistema drenar e
# concilia a conta PI com os PSPs. Sai com status ≠ 0 se algo não bateu.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CHECK="python scenarios/check.py"

say()  { printf '\n\033[1m== %s\033[0m\n' "$*"; }
step() { printf '  → %s\n' "$*"; }

DIRECTION="${DIRECTION:-both}"
LOAD_LOG="$(mktemp)"

# duration_seconds 4m → 240
duration_seconds() {
  local d="$1"
  case "$d" in
    *m) echo $(( ${d%m} * 60 )) ;;
    *s) echo "${d%s}" ;;
    *) echo "$d" ;;
  esac
}

run_scenario() {
  local result=0
  say "$TITLE"
  echo "$DESCRIPTION"
  echo "  Alertas esperados: ${EXPECT//,/, }"

  step "pré-condição: sem alertas críticos ou de página disparando"
  $CHECK quiet || return 1

  local start
  start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  trap 'restore' EXIT
  inject

  step "carga: $RATE Pix/s por $DURATION (sentido: $DIRECTION)"
  RATE="$RATE" DURATION="$DURATION" DIRECTION="$DIRECTION" \
    docker compose --profile load run --rm -e RATE -e DURATION -e DIRECTION k6 >"$LOAD_LOG" 2>&1 &
  local load_pid=$!

  step "esperando os alertas (até $(( $(duration_seconds "$DURATION") + 90 ))s)"
  $CHECK watch --expect "$EXPECT" --timeout $(( $(duration_seconds "$DURATION") + 90 )) || result=1

  wait "$load_pid" || true   # os limites do k6 falham de propósito sob falha
  step "resultado da carga (k6):"
  grep -E "pix_final_total|pix_end_to_end_seconds\.|pix_unfinished|pix_sync_errors|dropped_iterations" "$LOAD_LOG" | sed 's/^/      /'

  step "desfazendo a falha"
  restore
  trap - EXIT

  step "esperando o sistema drenar (retries, DLQs e resolver)"
  $CHECK drained || result=1
  if declare -F after_drain >/dev/null; then after_drain || result=1; fi

  step "reconciliando a conta PI com os PSPs (desde o início do cenário)"
  $CHECK reconcile --from "$start" || result=1

  if [ "$result" -eq 0 ]; then
    say "CENÁRIO OK: os alertas certos dispararam e o sistema se recuperou"
  else
    say "CENÁRIO FALHOU (veja as mensagens acima)"
  fi
  return "$result"
}
