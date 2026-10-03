#!/usr/bin/env bash
# Mini-case: vazamento de memória no spi-connector → OOMKilled → fila spi.out
# oscilando a cada reinício.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TITLE="Cenário 1 — vazamento de memória no spi-connector (OOMKilled)"
DESCRIPTION="O conector do PSP A retém 256 KiB por mensagem enviada (LEAK_KB_PER_MESSAGE) e roda com 96 MiB
  de limite. A memória sobe em rampa, o contêiner é morto (OOMKilled), reinicia e repete; a fila spi.out
  sobe a cada reinício e os Pix atrasam. Nada se perde: as mensagens não confirmadas voltam à fila.
  Observe: Grafana → Pix — serviços → Memória, Reinícios e OOM, Memória do runtime Go.
  Runbook: docs/runbooks/container-oomkilled.md"
EXPECT="ContainerRestarting,ContainerRestartedNearMemoryLimit"
RATE=10
DURATION=4m

inject() {
  step "psp-a-connector com limite de 96 MiB e vazamento de 256 KiB por mensagem"
  docker compose -f compose.yaml -f scenarios/01-leak.compose.yaml up -d psp-a-connector >/dev/null 2>&1
}

restore() {
  docker compose up -d psp-a-connector >/dev/null 2>&1
}

run_scenario
