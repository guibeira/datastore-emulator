"""The Python client with gRPC disabled talks HTTP with protobuf bodies
(`application/x-protobuf`). Both servers must answer it the same way."""

import json
import os
import subprocess
import sys

from parity_common import GOOGLE_HOST, PROJECT, RUST_HOST, USE_REAL_DB, unique_kind

PROBE = os.path.join(os.path.dirname(__file__), "http_client_probe.py")


def run_probe(host, kind):
    env = {k: v for k, v in os.environ.items() if k != "DATASTORE_EMULATOR_HOST"}
    env["GOOGLE_CLOUD_DISABLE_GRPC"] = "true"
    if host is None:
        project = os.environ["DATASTORE_PROJECT_ID"]
        database = os.environ["DATASTORE_DATABASE_NAME"]
    else:
        env["DATASTORE_EMULATOR_HOST"] = host
        project, database = PROJECT, ""
    result = subprocess.run(
        [sys.executable, PROBE, project, database, kind],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_http_protobuf_client_matches_reference():
    kind = unique_kind("ParityHttp")
    rust = run_probe(RUST_HOST, kind)
    reference = run_probe(None if USE_REAL_DB else GOOGLE_HOST, kind)
    assert rust["transport"] == "HTTPDatastoreAPI"
    assert rust == reference
