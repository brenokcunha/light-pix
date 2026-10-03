#!/usr/bin/env python3
"""Copia para o chart Helm os arquivos que ele empacota, a partir das fontes
únicas do repositório (o Helm não lê fora do diretório do chart).

    python scripts/k8s_sync.py        # ou: make k8s-sync

Copia dashboards, regras do Prometheus, a configuração do RabbitMQ e as cargas
iniciais para deploy/helm/light-pix/files/. As cópias são versionadas, para o
chart ser instalável sozinho, mas não devem ser editadas à mão.
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = ROOT / "deploy" / "helm" / "light-pix" / "files"

COPIES = {
    "dashboards": ("deploy/observability/grafana/dashboards", "*.json"),
    "rules": ("deploy/observability/prometheus/rules", "*.yml"),
    "rabbitmq": ("deploy/rabbitmq", None),
    "seeds": (None, None),
}


def main() -> None:
    if FILES.exists():
        shutil.rmtree(FILES)
    for name, (src, pattern) in COPIES.items():
        dst = FILES / name
        dst.mkdir(parents=True, exist_ok=True)
        if name == "rabbitmq":
            for f in ("rabbitmq.conf", "definitions.json", "enabled_plugins"):
                shutil.copy(ROOT / src / f, dst / f)
        elif name == "seeds":
            shutil.copy(ROOT / "deploy/psp/accounts-seed.json", dst / "accounts-seed.json")
            shutil.copy(ROOT / "deploy/sims/dict-seed.json", dst / "dict-seed.json")
        else:
            for f in sorted((ROOT / src).glob(pattern)):
                shutil.copy(f, dst / f.name)
    n = sum(1 for _ in FILES.rglob("*") if _.is_file())
    print(f"{n} arquivos copiados para {FILES.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
