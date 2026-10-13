window.BENCHMARK_DATA = {
  "lastUpdate": 1791921376470,
  "repoUrl": "https://github.com/guibeira/datastore-emulator",
  "entries": {
    "Datastore Emulator Benchmark": [
      {
        "commit": {
          "author": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "github-actions[bot]@users.noreply.github.com"
          },
          "committer": {
            "name": "github-actions[bot]",
            "username": "github-actions[bot]",
            "email": "github-actions[bot]@users.noreply.github.com"
          },
          "id": "2b8357eb6a477a3eb7d9a981562aeee096d21c97",
          "message": "chore(release): v0.1.1",
          "timestamp": "2026-05-16T16:04:55Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/2b8357eb6a477a3eb7d9a981562aeee096d21c97"
        },
        "date": 1791921376067,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.317435,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002678,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003039,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 8.821798,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.082555,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.153319,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 6.19567,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.061367,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.078715,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 20.491267,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.201221,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.400998,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.059312,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000555,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000934,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.139614,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001278,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.002522,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.276817,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.002766,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.004168,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.629393,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.00584,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.012181,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 8.95688,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.089447,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.112768,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 100.594437,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.94747,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 1.594293,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 1.052526,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.010346,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.016103,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 4.02938,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.038353,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.079089,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 7.2136,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.072584,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.109463,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 85.295211,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.774197,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 1.682539,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.065143,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000602,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.001063,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.387678,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003554,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.008187,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.225073,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002225,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002953,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.593904,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.005786,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.008627,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.115335,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001119,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001605,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.489155,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.004703,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.007831,
            "unit": "s",
            "extra": "10 clients, 100 runs/client, 10 warm-up, median of 3"
          }
        ]
      }
    ]
  }
}