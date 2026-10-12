"""Exercises the Datastore client over HTTP with protobuf bodies.

Run as a subprocess with GOOGLE_CLOUD_DISABLE_GRPC=true, which the client
reads at import time. Prints one JSON object with everything it observed, so
the parity test can compare two servers. Deletes only what it created.

Usage: http_client_probe.py PROJECT DATABASE KIND
"""

import json
import sys

from google.cloud import datastore
from google.cloud.datastore.query import PropertyFilter


def main():
    project, database, kind = sys.argv[1], sys.argv[2] or None, sys.argv[3]
    client = datastore.Client(project=project, database=database)
    out = {"transport": type(client._datastore_api).__name__}

    first = datastore.Entity(key=client.key(kind, "one"))
    first.update({"score": 7, "tags": ["a", "b"], "name": "x"})
    client.put(first)
    out["get"] = dict(client.get(first.key))

    query = client.query(kind=kind)
    query.add_filter(filter=PropertyFilter("score", "=", 7))
    out["query"] = [dict(entity) for entity in query.fetch()]

    with client.transaction():
        second = datastore.Entity(key=client.key(kind, "two"))
        second["score"] = 1
        client.put(second)
    out["count_after_transaction"] = len(list(client.query(kind=kind).fetch()))

    out["missing_is_none"] = client.get(client.key(kind, "nope")) is None
    out["allocated"] = len(client.allocate_ids(client.key(kind), 2))

    client.delete_multi([first.key, second.key])
    out["count_after_delete"] = len(list(client.query(kind=kind).fetch()))
    print(json.dumps(out, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
