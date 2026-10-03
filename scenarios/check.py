#!/usr/bin/env python3
"""Verificações dos cenários de incidente: alertas, drenagem e reconciliação.

    check.py quiet     [--timeout S]               espera não haver alerta crítico nem de página
    check.py watch     --expect A,B:page [--timeout S]  espera os alertas esperados dispararem
    check.py drained   [--timeout S]               espera nenhum Pix parado, filas e DLQs vazias
    check.py reconcile --from ISO                  concilia os dois PSPs; falha se houver divergência

Um alerta esperado é `Nome` ou `Nome:page|ticket` (gravidade Sloth). Só usa a
biblioteca padrão e o `docker compose` do host.
"""
import argparse
import base64
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

sys.stdout.reconfigure(encoding="utf-8")
PROM = "http://localhost:9090"
RESOLVERS = {"11111111": "http://localhost:8083", "22222222": "http://localhost:8084"}
RABBIT = "http://localhost:15672"


def get(url, auth=None):
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def firing():
    alerts = get(f"{PROM}/api/v1/alerts")["data"]["alerts"]
    return [a for a in alerts if a["state"] == "firing"]


def label(a):
    l = a["labels"]
    who = l.get("psp_ispb") or l.get("ispb") or l.get("queue") or l.get("kind") or l.get("container_label_com_docker_compose_service") or ""
    sev = l.get("sloth_severity") or l.get("severity", "")
    return f"{l['alertname']}[{sev}]" + (f"({who})" if who else "")


def quiet(args):
    deadline = time.time() + args.timeout
    while True:
        busy = [a for a in firing() if a["labels"].get("sloth_severity") == "page" or a["labels"].get("severity") == "critical"]
        if not busy:
            print("ok: nenhum alerta crítico ou de página disparando")
            return 0
        if time.time() > deadline:
            print("falha: ainda disparando:", ", ".join(sorted(label(a) for a in busy)))
            return 1
        print("  esperando os alertas anteriores resolverem:", ", ".join(sorted({label(a) for a in busy})))
        time.sleep(15)


def matches(a, expected):
    name, _, sev = expected.partition(":")
    return a["labels"]["alertname"] == name and (not sev or a["labels"].get("sloth_severity") == sev)


def watch(args):
    expected = [e for e in args.expect.split(",") if e]
    start = time.time()
    seen = {}
    while True:
        now = firing()
        for e in expected:
            if e not in seen and any(matches(a, e) for a in now):
                seen[e] = time.time() - start
                print(f"  ✓ {e} disparou após {seen[e]:.0f}s")
        if len(seen) == len(expected):
            print("ok: todos os alertas esperados dispararam")
            return 0
        if time.time() - start > args.timeout:
            missing = [e for e in expected if e not in seen]
            print("falha: não dispararam:", ", ".join(missing))
            print("  disparando agora:", ", ".join(sorted({label(a) for a in now})) or "nada")
            return 1
        time.sleep(5)


def mongo_count():
    js = ("let n = 0; for (const d of ['psp_a', 'psp_b']) n += db.getSiblingDB(d).payments.countDocuments("
          "{state: {$in: ['PENDING', 'SENT', 'ACCEPTED']}}); print(n)")
    out = subprocess.run(["docker", "compose", "exec", "-T", "mongo", "mongosh", "--quiet", "--eval", js],
                         capture_output=True, text=True, check=True).stdout.strip().splitlines()
    return int(out[-1])


def queued():
    total = 0
    for vhost in ("psp-a", "psp-b"):
        for q in get(f"{RABBIT}/api/queues/{vhost}", auth="lightpix:lightpix"):
            total += q.get("messages", 0)
    return total


def drained(args):
    deadline = time.time() + args.timeout
    calm = 0
    while True:
        pend, msgs = mongo_count(), queued()
        if pend == 0 and msgs == 0:
            calm += 1
            if calm >= 2:
                print("ok: nenhum Pix parado e nenhuma mensagem nas filas, retries e DLQs")
                return 0
        else:
            calm = 0
            print(f"  esperando drenar: {pend} Pix em andamento, {msgs} mensagens nas filas")
        if time.time() > deadline:
            print(f"falha: não drenou ({pend} Pix, {msgs} mensagens)")
            return 1
        time.sleep(10)


def reconcile(args):
    to = datetime.now(timezone.utc).replace(microsecond=0)
    # Uma folga para o relógio dos contêineres (Docker Desktop) não deixar liquidações fora da janela.
    to = datetime.fromtimestamp(to.timestamp() + 120, timezone.utc)
    status = 0
    for ispb, url in RESOLVERS.items():
        req = urllib.request.Request(f"{url}/admin/reconciliation?from={args.start}&to={to:%Y-%m-%dT%H:%M:%SZ}", method="POST")
        try:
            with urllib.request.urlopen(req, timeout=180) as r:
                rep = json.load(r)
        except urllib.error.HTTPError as e:
            print(f"falha: PSP {ispb}: {e.read().decode()[:200]}")
            status = 1
            continue
        print(f"  PSP {ispb}: {rep['status']}, {rep['piEntries']} liquidações na conta PI, {len(rep['divergences'])} divergências")
        for d in rep["divergences"][:5]:
            print("     ", d["type"], d["endToEndId"], d["piAmount"], d["localState"])
        if rep["divergences"]:
            status = 1
    print("ok: a conta PI e os PSPs batem" if status == 0 else "falha: há divergências")
    return status


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name, fn in (("quiet", quiet), ("watch", watch), ("drained", drained), ("reconcile", reconcile)):
        p = sub.add_parser(name)
        p.set_defaults(fn=fn)
        if name == "watch":
            p.add_argument("--expect", required=True)
        if name == "reconcile":
            p.add_argument("--from", dest="start", required=True)
        p.add_argument("--timeout", type=int, default=1200 if name != "watch" else 420)
    args = ap.parse_args()
    sys.exit(args.fn(args))


if __name__ == "__main__":
    main()
