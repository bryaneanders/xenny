"""Registers every <subject>.avsc in SCHEMA_DIR and sets the subject to BACKWARD compatibility.

Re-registering an unchanged schema is a no-op. An incompatible change is rejected
by the registry (HTTP 409), which fails this Job and the ArgoCD sync with it.
"""
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

REGISTRY = os.environ["REGISTRY_URL"].rstrip("/") + "/apis/ccompat/v7"
SCHEMA_DIR = pathlib.Path(os.environ.get("SCHEMA_DIR", "/schemas"))


def call(method, path, body):
    req = urllib.request.Request(
        REGISTRY + path,
        data=json.dumps(body).encode(),
        method=method,
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def wait_for_registry():
    for _ in range(60):
        try:
            urllib.request.urlopen(REGISTRY + "/subjects", timeout=5)
            return
        except (urllib.error.URLError, OSError):
            time.sleep(5)
    sys.exit("Registry not reachable at " + REGISTRY)


def main():
    wait_for_registry()
    failed = False
    for path in sorted(SCHEMA_DIR.glob("*.avsc")):
        subject = path.stem
        try:
            result = call("POST", f"/subjects/{subject}/versions", {"schema": path.read_text(), "schemaType": "AVRO"})
            call("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
            print(f"{subject}: schema id {result['id']}")
        except urllib.error.HTTPError as e:
            print(f"{subject}: rejected ({e.code}) {e.read().decode()}")
            failed = True
    sys.exit(1 if failed else 0)


main()
