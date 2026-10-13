window.BENCHMARK_DATA = {
  "lastUpdate": 1791921660555,
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
      },
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
        "date": 1791921656009,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.053282,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002668,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.002877,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.331652,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.063375,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.122645,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 1.124352,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.057402,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.071854,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 3.953797,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.200633,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.324661,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.009834,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000469,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000788,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.039423,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001878,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003457,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.039323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.00203,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.003396,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.125626,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.006017,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.010849,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.50412,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.025092,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.03368,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 3.891554,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.184548,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.332693,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.189525,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.00976,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.014051,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.992182,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.046868,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.103105,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.341285,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.017235,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.027318,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 3.291596,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.153918,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.352989,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.011905,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000578,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000951,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.07564,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003601,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006321,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.042494,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002074,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002802,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.109977,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.005292,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.008671,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.02316,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001128,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001619,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.104252,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.004986,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008009,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          }
        ]
      }
    ],
    "Datastore Emulator Speedup": [
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
        "date": 1791921377951,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 30.83,
            "unit": "x",
            "extra": "Java p50 0.0825549 s / Rust p50 0.0026781 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 3.28,
            "unit": "x",
            "extra": "Java p50 0.201221 s / Rust p50 0.0613671 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 2.3,
            "unit": "x",
            "extra": "Java p50 0.00127809 s / Rust p50 0.000554866 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.11,
            "unit": "x",
            "extra": "Java p50 0.00584037 s / Rust p50 0.00276599 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 10.59,
            "unit": "x",
            "extra": "Java p50 0.94747 s / Rust p50 0.0894469 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 3.71,
            "unit": "x",
            "extra": "Java p50 0.038353 s / Rust p50 0.0103459 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 10.67,
            "unit": "x",
            "extra": "Java p50 0.774197 s / Rust p50 0.0725843 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 5.9,
            "unit": "x",
            "extra": "Java p50 0.00355367 s / Rust p50 0.000602191 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 2.6,
            "unit": "x",
            "extra": "Java p50 0.00578586 s / Rust p50 0.00222542 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.2,
            "unit": "x",
            "extra": "Java p50 0.00470327 s / Rust p50 0.00111873 s"
          }
        ]
      },
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
        "date": 1791921658141,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 23.75,
            "unit": "x",
            "extra": "Java p50 0.0633754 s / Rust p50 0.00266789 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 3.5,
            "unit": "x",
            "extra": "Java p50 0.200633 s / Rust p50 0.0574017 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 4,
            "unit": "x",
            "extra": "Java p50 0.00187765 s / Rust p50 0.000469461 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.96,
            "unit": "x",
            "extra": "Java p50 0.00601687 s / Rust p50 0.00203048 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 7.35,
            "unit": "x",
            "extra": "Java p50 0.184548 s / Rust p50 0.0250923 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 4.8,
            "unit": "x",
            "extra": "Java p50 0.0468682 s / Rust p50 0.00975997 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 8.93,
            "unit": "x",
            "extra": "Java p50 0.153918 s / Rust p50 0.0172348 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 6.23,
            "unit": "x",
            "extra": "Java p50 0.00360073 s / Rust p50 0.000577553 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 2.55,
            "unit": "x",
            "extra": "Java p50 0.0052918 s / Rust p50 0.00207384 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.42,
            "unit": "x",
            "extra": "Java p50 0.0049864 s / Rust p50 0.00112753 s"
          }
        ]
      }
    ],
    "Datastore Emulator Resources": [
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
        "date": 1791921660132,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 173.6,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 208.22,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 65.41,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 65.25,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 145.42,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 205.73,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 2037.76,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 2037.76,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          }
        ]
      }
    ]
  }
}