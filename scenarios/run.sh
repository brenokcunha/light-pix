#!/usr/bin/env bash
# Roda cenários de incidente e resume o resultado.
#
#   scenarios/run.sh            roda os cinco
#   scenarios/run.sh 2 4        roda só o 2 e o 4
#
# Cada cenário confere que os alertas esperados disparam e que o sistema se
# recupera sozinho (drena as filas e a conta PI concilia com os PSPs). Sai com
# status ≠ 0 se algum falhar.
cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

nums=("$@")
[ ${#nums[@]} -eq 0 ] && nums=(1 2 3 4 5)

failed=0
summary=""
for n in "${nums[@]}"; do
  script=$(ls scenarios/0"$n"-*.sh 2>/dev/null | head -1)
  if [ -z "$script" ]; then echo "cenário $n não existe" >&2; exit 2; fi
  if bash "$script"; then
    summary+="  ✓ $(basename "$script" .sh)"$'\n'
  else
    summary+="  ✗ $(basename "$script" .sh)"$'\n'
    failed=1
  fi
done

printf '\n\033[1mResumo\033[0m\n%s' "$summary"
exit $failed
