window.BENCHMARK_DATA = {
  "lastUpdate": 1791922100800,
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
          "id": "29f1652e500eb09ed6308a9fa5b6a350e5f0ba49",
          "message": "chore(release): v0.1.2",
          "timestamp": "2026-05-19T23:21:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/29f1652e500eb09ed6308a9fa5b6a350e5f0ba49"
        },
        "date": 1791921876465,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.056034,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002809,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003091,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.399418,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.066811,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.124222,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.640939,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.032488,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.039934,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.334295,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.215489,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.406934,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.010503,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000521,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.00079,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.03418,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001513,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003321,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.052874,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.00259,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.004029,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.120964,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005813,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.010738,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.257122,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.012318,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.015801,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 5.072498,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.227063,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.455053,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.064736,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.00323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.004724,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.91731,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.041834,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.097547,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.142212,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.007116,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.010855,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 3.609701,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.173056,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.37191,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.010964,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000537,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000839,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.064498,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003087,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006275,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.04381,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002182,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002885,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.121603,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.005994,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.009717,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.024522,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001214,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001784,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.100812,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.005046,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.007572,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
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
          "id": "e209fdde0944cea92c6740ac33df792deab426b9",
          "message": "chore(release): v0.1.3",
          "timestamp": "2026-05-21T22:12:20Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/e209fdde0944cea92c6740ac33df792deab426b9"
        },
        "date": 1791922100388,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.055313,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.00274,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003108,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.402777,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.066416,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.125419,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.641332,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.032169,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.040447,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 3.973488,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.206487,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.39133,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.010939,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000536,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000902,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.034767,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001589,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003253,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.053473,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.00261,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.004082,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.145861,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005932,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.016971,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.277468,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.012911,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.016545,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 4.522071,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.21114,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.38605,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.063696,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.003204,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.004684,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.881454,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.041619,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.096441,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.135328,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.00651,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.010561,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 3.812972,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.17868,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.38564,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.010914,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000531,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000871,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.07149,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003319,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006719,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.043651,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002155,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002886,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.119489,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.005863,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.009869,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.023563,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001157,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001799,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.098134,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.004739,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008338,
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
          "id": "29f1652e500eb09ed6308a9fa5b6a350e5f0ba49",
          "message": "chore(release): v0.1.2",
          "timestamp": "2026-05-19T23:21:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/29f1652e500eb09ed6308a9fa5b6a350e5f0ba49"
        },
        "date": 1791921878889,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 23.79,
            "unit": "x",
            "extra": "Java p50 0.0668109 s / Rust p50 0.00280864 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 6.63,
            "unit": "x",
            "extra": "Java p50 0.215489 s / Rust p50 0.0324879 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 2.91,
            "unit": "x",
            "extra": "Java p50 0.00151301 s / Rust p50 0.000520709 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.24,
            "unit": "x",
            "extra": "Java p50 0.00581308 s / Rust p50 0.00258993 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 18.43,
            "unit": "x",
            "extra": "Java p50 0.227063 s / Rust p50 0.0123176 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 12.95,
            "unit": "x",
            "extra": "Java p50 0.0418342 s / Rust p50 0.00322956 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 24.32,
            "unit": "x",
            "extra": "Java p50 0.173056 s / Rust p50 0.00711563 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 5.75,
            "unit": "x",
            "extra": "Java p50 0.00308689 s / Rust p50 0.000536501 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 2.75,
            "unit": "x",
            "extra": "Java p50 0.00599377 s / Rust p50 0.00218165 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.16,
            "unit": "x",
            "extra": "Java p50 0.00504552 s / Rust p50 0.00121412 s"
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
          "id": "29f1652e500eb09ed6308a9fa5b6a350e5f0ba49",
          "message": "chore(release): v0.1.2",
          "timestamp": "2026-05-19T23:21:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/29f1652e500eb09ed6308a9fa5b6a350e5f0ba49"
        },
        "date": 1791921880943,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 161.84,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 201.06,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 71.41,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 71.41,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 141.81,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 205.1,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1928.19,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1928.19,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          }
        ]
      }
    ]
  }
}