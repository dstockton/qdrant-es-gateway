#!/usr/bin/env python3
"""Open disposable Qdrant 1.19.2 data with 1.20.0 and verify a point."""
import json
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path


def docker(*args):
    return subprocess.check_output(("docker", *args), text=True).strip()


def request(port, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def start(name, version, storage):
    docker("run", "-d", "--name", name, "--cpus", "1.5", "--memory", "2g",
           "-v", f"{storage}:/qdrant/storage", "-p", "127.0.0.1::6333",
           f"qdrant/qdrant:v{version}")
    port = int(docker("port", name, "6333/tcp").rsplit(":", 1)[1])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            request(port, "GET", "/")
            return port
        except (OSError, urllib.error.URLError, ValueError):
            time.sleep(.25)
    raise TimeoutError(f"Qdrant {version} did not become ready")


def main():
    name = "qeg-upgrade-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix="qeg-qdrant-upgrade-") as storage:
        try:
            port = start(name, "1.19.2", Path(storage))
            request(port, "PUT", "/collections/upgrade_smoke", {"vectors": {"size": 4, "distance": "Cosine"}})
            result = request(port, "PUT", "/collections/upgrade_smoke/points?wait=true",
                             {"points": [{"id": 1, "vector": [1, 0, 0, 0], "payload": {"marker": "preupgrade"}}]})
            assert result["status"] == "ok", result
            before = request(port, "GET", "/collections/upgrade_smoke/points/1?with_payload=true")
            assert before["result"]["payload"]["marker"] == "preupgrade"
            docker("rm", "-f", name)
            port = start(name, "1.20.0", Path(storage))
            after = request(port, "GET", "/collections/upgrade_smoke/points/1?with_payload=true")
            assert after["result"]["payload"]["marker"] == "preupgrade"
            count = request(port, "POST", "/collections/upgrade_smoke/points/count", {"exact": True})
            assert count["result"]["count"] == 1
            print(json.dumps({"status": "passed", "path": "1.19.2 -> 1.20.0",
                              "point_retained": True, "exact_count": 1}))
        finally:
            subprocess.run(("docker", "rm", "-f", name), capture_output=True, text=True)


if __name__ == "__main__":
    main()
