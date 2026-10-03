#!/usr/bin/env bash
# Sobe o laboratório num cluster kind: cria o cluster, carrega as imagens, instala
# metrics-server e kube-prometheus-stack e, por fim, o chart light-pix.
# Precisa de kind e helm (scripts/k8s_tools.sh) e da stack do Compose parada, porque
# o cluster publica as mesmas portas no host.
#
#   scripts/k8s_up.sh
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PWD/.tools:$PATH"
CLUSTER=lightpix

if [ -n "$(docker compose ps --status running -q 2>/dev/null)" ]; then
  echo "A stack do Compose está no ar e usa as mesmas portas e a mesma memória." >&2
  echo "Pare com: docker compose stop   (os dados ficam nos volumes)" >&2
  exit 1
fi
command -v kind >/dev/null && command -v helm >/dev/null || { echo "faltam kind e helm: scripts/k8s_tools.sh" >&2; exit 1; }

echo "== imagens"
docker compose build psp-a-api spi-sim dict-sim bacen-sim

echo "== cluster"
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/k8s/kind-config.yaml
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo "== carregando imagens no cluster"
for img in light-pix/psp:dev light-pix/spi-sim:dev light-pix/dict-sim:dev light-pix/bacen-sim:dev \
           mongo:8.0 rabbitmq:4.3-management otel/opentelemetry-collector-contrib:0.161.0 \
           grafana/tempo:3.1.0 percona/mongodb_exporter:0.53.0; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull "$img"
  kind load docker-image --name "$CLUSTER" "$img"
done

echo "== metrics-server (para o HPA)"
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null
helm upgrade --install metrics-server metrics-server/metrics-server -n kube-system \
  --set 'args={--kubelet-insecure-tls}' --wait --timeout 5m

echo "== kube-prometheus-stack"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null
helm repo update >/dev/null
helm upgrade --install kps prometheus-community/kube-prometheus-stack -n monitoring --create-namespace \
  -f deploy/k8s/kps-values.yaml --wait --timeout 10m

echo "== light-pix"
python scripts/k8s_sync.py
helm upgrade --install light-pix deploy/helm/light-pix -n lightpix --create-namespace
kubectl -n lightpix rollout status statefulset/mongo statefulset/rabbitmq --timeout=5m
kubectl -n lightpix wait --for=condition=available deployment --all --timeout=8m
kubectl -n lightpix get pods
