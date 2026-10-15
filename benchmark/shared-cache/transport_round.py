#!/usr/bin/env python3
"""Run one old/new round against the retained shared-cache corpus; never build."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
HERE = ROOT / "benchmark" / "shared-cache"
OLD_IMAGE_ID = "sha256:68784402708c0e61b027834ddc17734cec38ce87f825bb763261910aa74c49d4"
NEW_IMAGE_ID = "sha256:220649fc71be02210342fef90f9b8d2426cfb8013ee8285ce44cc8c41aa61fc4"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["off", "on"])
    parser.add_argument("version", choices=["old", "new"])
    parser.add_argument("round", choices=["1", "2"])
    parser.add_argument("kind", choices=["mixed", "warm"])
    parser.add_argument("--old-image", default="qdrant-es-gateway:cache-runtime")
    parser.add_argument("--new-image", default="qdrant-es-gateway:catalog-transport")
    parser.add_argument("--old-image-id", default=OLD_IMAGE_ID)
    parser.add_argument("--new-image-id", default=NEW_IMAGE_ID)
    args = parser.parse_args()

    image = args.old_image if args.version == "old" else args.new_image
    expected_id = args.old_image_id if args.version == "old" else args.new_image_id
    name = f"transport-{args.mode}-{args.version}-{args.round}"
    preparation = json.loads((HERE / "results" / "preparation.json").read_text())
    profiles = ["cold", "invalidation"] if args.kind == "mixed" else ["warm"]
    for profile in profiles:
        if (HERE / "results" / f"{name}-{profile}.json").exists():
            raise SystemExit("Refusing to overwrite retained evidence")

    env = dict(
        os.environ,
        CACHE_BENCH_DEPLOYMENT=preparation["deployment"],
        CACHE_BENCH_SERVE="false" if args.mode == "off" else "true",
        CATALOG_TRANSPORT_IMAGE=image,
    )
    actual_id = subprocess.check_output(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"],
        text=True,
        env=env,
    ).strip()
    if actual_id != expected_id:
        raise SystemExit("Image identity differs from expected build; no runtime change made")

    subprocess.run(
        [
            "docker", "compose", "-p", "qeg-shared-cache",
            "-f", "benchmark/three-engine/compose.yml",
            "-f", "benchmark/shared-cache/compose.yml",
            "-f", "benchmark/shared-cache/transport-image.yml",
            "up", "-d", "--no-build", "--no-deps", "gateway",
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )
    for _ in range(60):
        try:
            with urllib.request.urlopen("http://127.0.0.1:19302/readyz", timeout=2) as response:
                response.read()
            break
        except (urllib.error.URLError, TimeoutError, OSError):
            time.sleep(0.5)
    else:
        raise RuntimeError("Gateway readiness failed; no measurements started")

    for profile in profiles:
        output = HERE / "results" / f"{name}-{profile}.json"
        trial_env = env | {
            "OUTPUT": str(output),
            "REQUESTS": "100" if profile == "invalidation" else "200",
            "TRIALS": "3",
            "WARMUP": "40",
            "ROUND": f"{name}-{profile}",
            "PROFILE": profile,
            "TRACK_TOTAL_HITS": "true",
            "CONCURRENCY": "1,8",
            "DOCS": "10000",
            "REQUEST_CACHE": "default",
        }
        script = "measure.py" if profile == "warm" else "mixed.py"
        subprocess.run(
            [sys.executable, str(HERE / script)],
            cwd=ROOT,
            env=trial_env,
            check=True,
        )
        result = json.loads(output.read_text())
        if result.get("status") != "passed":
            raise RuntimeError("Benchmark failed; evidence retained")
        if result["provenance"]["gateway"]["image_id"] != expected_id:
            raise RuntimeError("Measured container image differs from selected build")
        result["config"].update(
            transport_experiment="catalog-transport-paired-v1",
            gateway_version=args.version,
        )
        output.write_text(json.dumps(result, indent=2) + "\n")
        print("COMPLETED", name, profile, len(result["measurements"]), flush=True)


if __name__ == "__main__":
    main()
