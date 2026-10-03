#!/usr/bin/env bash
# Baixa kind e helm para .tools/ (fora do sistema; a pasta é ignorada pelo git).
# Versões fixas, as usadas para validar o laboratório.
#
#   scripts/k8s_tools.sh
set -euo pipefail
cd "$(dirname "$0")/.."

KIND_VERSION="${KIND_VERSION:-v0.33.0}"
HELM_VERSION="${HELM_VERSION:-v4.3.0}"
mkdir -p .tools

case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*) os=windows; ext=.exe; arch=amd64 ;;
  Darwin) os=darwin; ext=""; arch=$(uname -m | sed 's/x86_64/amd64/') ;;
  *) os=linux; ext=""; arch=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/') ;;
esac

if [ ! -x ".tools/kind$ext" ]; then
  echo "baixando kind $KIND_VERSION ($os-$arch)"
  curl -fsSL -o ".tools/kind$ext" "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-$os-$arch"
  chmod +x ".tools/kind$ext"
fi
if [ ! -x ".tools/helm$ext" ]; then
  echo "baixando helm $HELM_VERSION ($os-$arch)"
  if [ "$os" = windows ]; then
    curl -fsSL -o .tools/helm.zip "https://get.helm.sh/helm-$HELM_VERSION-$os-$arch.zip"
    python -c "import zipfile; zipfile.ZipFile('.tools/helm.zip').extractall('.tools/helm-x')"
    mv ".tools/helm-x/$os-$arch/helm.exe" .tools/helm.exe
    rm -rf .tools/helm-x .tools/helm.zip
  else
    curl -fsSL "https://get.helm.sh/helm-$HELM_VERSION-$os-$arch.tar.gz" | tar -xz -C .tools --strip-components=1 "$os-$arch/helm"
  fi
fi
".tools/kind$ext" version
".tools/helm$ext" version --short
