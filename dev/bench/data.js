window.BENCHMARK_DATA = {
  "lastUpdate": 1793129558548,
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
          "id": "10382cff471456f2ece16c5f9865d7f7cf00f3be",
          "message": "chore(release): v0.1.4",
          "timestamp": "2026-05-23T18:55:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/10382cff471456f2ece16c5f9865d7f7cf00f3be"
        },
        "date": 1791922335226,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.060689,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.003006,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003312,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.337067,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.06383,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.120369,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.729871,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.036512,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.045407,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.148959,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.215194,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.328192,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.010303,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000489,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000858,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.043638,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.002195,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003298,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.05485,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.002709,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.004189,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.144299,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.00658,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.013788,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.287709,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.013792,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.018227,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 5.627328,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.259726,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.456171,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.076613,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.00379,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.005976,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.944021,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.044987,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.104228,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.154679,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.007855,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.011977,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 4.070605,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.193738,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.40821,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.01143,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000544,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000897,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.071448,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003377,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006436,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.044323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002242,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002907,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.12286,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.005844,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.009924,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.02358,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001149,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001724,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.094085,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.004721,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008337,
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
          "id": "6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428",
          "message": "chore(release): v0.1.5",
          "timestamp": "2026-10-11T13:59:59Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428"
        },
        "date": 1791922575598,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.13391,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.006596,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.007333,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.369524,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.063842,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.130363,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 1.019651,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.051604,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.062471,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 5.426323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.282678,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.444032,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.006569,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000317,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000516,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.027047,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001142,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003395,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.064576,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.003125,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.004883,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.12603,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005991,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.011641,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.662149,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.031809,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.043588,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 5.797551,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.263546,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.478843,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.16263,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.00826,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.011793,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 1.156533,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.055139,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.121248,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.415882,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.020552,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.033156,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 5.06941,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.243212,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.51224,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.009437,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000465,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000687,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.062906,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003032,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.005988,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.029915,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001469,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.00232,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.101143,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.004928,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.008797,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.0152,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000699,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.00112,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.084712,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.00412,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.006873,
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
          "id": "da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa",
          "message": "chore(release): v0.1.6",
          "timestamp": "2026-10-12T23:57:08Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa"
        },
        "date": 1791922747462,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.157321,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.007569,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.01245,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.576809,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.025382,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.06371,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.689233,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.034988,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.042082,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 2.348316,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.112534,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.220161,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.003603,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000175,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000281,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.014412,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.000648,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.001371,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.03743,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.00186,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.002821,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.080883,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.002815,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.008498,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.104527,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.00358,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.009218,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 2.536591,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.117807,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.227824,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.122914,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.006037,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.00959,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.486618,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.022713,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.052969,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.006565,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.00032,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000507,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 1.97157,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.096469,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.200868,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.004429,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000217,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000358,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.029461,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.001391,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.003561,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.016088,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.000785,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.00108,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.05353,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.002553,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.004551,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.009421,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000457,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.000682,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.043106,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.001982,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.00421,
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
          "id": "7a6d688fcc3f3c5876bc30da73d529054db7a9c4",
          "message": "chore(release): v0.1.7",
          "timestamp": "2026-10-13T13:12:34Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7a6d688fcc3f3c5876bc30da73d529054db7a9c4"
        },
        "date": 1791922980175,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.042889,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002129,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003396,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.467884,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.068205,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.130785,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.421612,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.02118,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.026213,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.187814,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.215579,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.373921,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.010138,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000476,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000796,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.039853,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001815,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.004018,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.047916,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.002319,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.003701,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.116983,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005361,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.010471,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.124773,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.005445,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.008864,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 5.568466,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.254391,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.436755,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.051399,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.002497,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.003861,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.989901,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.045662,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.11086,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.015227,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000715,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.001253,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 4.30953,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.205681,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.433022,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.010222,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000489,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000771,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.073263,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003752,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006303,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.039965,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001985,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002681,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.123337,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.006054,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.009972,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.021514,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001003,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.00167,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.102813,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.005113,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008248,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "76097ca1e69fea9abdd28f7b0758c7c222d017cf",
          "message": "chore(release): v0.1.8",
          "timestamp": "2026-10-21T17:14:59Z",
          "tree_id": "3f7c57ef9a4ae4845b18a494fb9d57c73fd4c0fd",
          "url": "https://github.com/guibeira/datastore-emulator/commit/76097ca1e69fea9abdd28f7b0758c7c222d017cf"
        },
        "date": 1792603996925,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.041577,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002059,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.003225,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.440781,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.069481,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.134217,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.382612,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.019345,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.024025,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.706343,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.230479,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.445471,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.010447,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000517,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000809,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.043949,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001984,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.004018,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.043215,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.002109,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.00314,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.160397,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.007418,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.015387,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.113557,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.005197,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.007622,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 4.404562,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.208323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.35848,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.048734,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.002466,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.00378,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.957879,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.043859,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.101034,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.017071,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000836,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.001279,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 3.238901,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.158386,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.3187,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.011916,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000583,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000947,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.071264,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003376,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006497,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.042076,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.002067,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002943,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.144554,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.00698,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.011103,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.023379,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.001163,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001733,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.113054,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.005518,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008818,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "4c566cefa0cf2d99dfc7275539fd920a3fe009f4",
          "message": "chore(release): v0.1.9",
          "timestamp": "2026-10-26T21:14:53Z",
          "tree_id": "231d71c22f6a1676d9a4dee4a13a33d2cd864f1f",
          "url": "https://github.com/guibeira/datastore-emulator/commit/4c566cefa0cf2d99dfc7275539fd920a3fe009f4"
        },
        "date": 1793050353685,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.020223,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.000981,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.001533,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.626724,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.027268,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.06504,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.163513,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.008099,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.010188,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 2.614762,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.126379,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.237968,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.004275,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000203,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000355,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.021012,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.000927,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.002539,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.022709,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001176,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.001743,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.063576,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.002972,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.00681,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.052611,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.002257,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.003062,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 2.095148,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.095649,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.175434,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.023031,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.001164,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.001675,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.607498,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.027904,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.065273,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.006601,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000322,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000508,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 1.658799,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.080364,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.165756,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.004231,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000195,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000323,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.036523,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.001703,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.003294,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.015071,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.000705,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.001144,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.059281,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.002731,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.005476,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.008966,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.00043,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.000659,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.053339,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.002359,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.00524,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "efd5db4dce4b87f617e07017ac372256f528d22f",
          "message": "chore(release): v0.1.10",
          "timestamp": "2026-10-26T23:19:31Z",
          "tree_id": "690ddb2f7b8b43b843452f50e50e4a9122079137",
          "url": "https://github.com/guibeira/datastore-emulator/commit/efd5db4dce4b87f617e07017ac372256f528d22f"
        },
        "date": 1793057822894,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.022851,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.001078,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.001909,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.631485,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.028732,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.068295,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.173111,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.008734,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.010777,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 2.896828,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.137259,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.264346,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.004196,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000188,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000346,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.026799,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001329,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.002328,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.021621,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001046,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.001646,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.071694,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.003344,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.009079,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.056474,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.002374,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.004853,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 2.883967,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.133347,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.269832,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.024034,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.001217,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.001778,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.600373,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.02874,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.061097,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.007198,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000357,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000513,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 2.073405,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.101482,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.214884,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.004419,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000206,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000339,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.039531,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.001956,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.003411,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.016054,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.000797,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.001177,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.062558,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.002659,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.006172,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.009118,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.00043,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.000635,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.049977,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.002216,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.004959,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "1ec10508bea3286896f70d1114e7f6fc318eb8a8",
          "message": "chore(release): v0.1.11",
          "timestamp": "2026-10-27T01:04:30Z",
          "tree_id": "3a65432531c6b360ecaeedcc18bf8ae5c4c29cac",
          "url": "https://github.com/guibeira/datastore-emulator/commit/1ec10508bea3286896f70d1114e7f6fc318eb8a8"
        },
        "date": 1793063975095,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.027863,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.001373,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.002151,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.94475,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.04239,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.110444,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.265962,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.013599,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.016295,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 3.697488,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.17954,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.332943,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.005854,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000277,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000438,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.02706,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001087,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.003427,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.030481,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001477,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.002316,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.104519,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.004823,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.009565,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.081739,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.003427,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.006699,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 3.430065,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.15627,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.312354,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.036122,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.001826,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.002683,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.807771,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.037174,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.086152,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.009817,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000468,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000785,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 2.557082,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.123752,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.251448,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.007266,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000332,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000559,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.054388,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.002614,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.004875,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.026606,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001265,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.001911,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.08425,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.003987,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.006968,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.012634,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000586,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.000924,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.076296,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.003754,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.005961,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "7016e9bcfe7804417e59d57df7f92e988bcc92aa",
          "message": "chore(release): v0.1.12",
          "timestamp": "2026-10-27T13:56:09Z",
          "tree_id": "81fd96a6bfeb073e83859b7cbe1a2d2984d13232",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7016e9bcfe7804417e59d57df7f92e988bcc92aa"
        },
        "date": 1793110260495,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.037753,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.001807,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.002955,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.039904,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.046732,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.11254,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.343816,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.017227,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.021392,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.812973,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.239743,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.450483,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.006266,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000288,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000501,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.030417,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001378,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.002659,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.029803,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001506,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.002191,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.107465,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005156,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.010346,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.087581,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.003627,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.005398,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 5.40678,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.259682,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.415154,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.040789,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.002065,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.003064,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 1.176868,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.051735,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.136871,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.010091,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000498,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000754,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 4.668074,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.226947,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.4616,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.007239,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000348,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000574,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.053026,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.002489,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.006565,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.027819,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001318,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.00192,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.121632,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.006084,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.009806,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.015093,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000729,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001265,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.119767,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.005095,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.008706,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "78835c2f7e0c5920e0d8bb8859db1ca5210fe882",
          "message": "chore(release): v0.1.13",
          "timestamp": "2026-10-27T15:54:48Z",
          "tree_id": "7dec2b6ada02970de1030ff0e17bd6e58870ece1",
          "url": "https://github.com/guibeira/datastore-emulator/commit/78835c2f7e0c5920e0d8bb8859db1ca5210fe882"
        },
        "date": 1793117628587,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.052654,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.002537,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.004162,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 1.264851,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.059693,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.118666,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.328437,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.016565,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.019827,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 4.751929,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.229089,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.42464,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.0087,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000411,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.00066,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.046302,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.002091,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.004034,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.037644,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001867,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.002742,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.128011,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.006202,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.011514,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.097699,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.00438,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.005937,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 3.77833,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.175871,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.322972,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.047348,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.002415,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.003455,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 1.009568,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.048114,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.105847,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.01315,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000634,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000962,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 2.955953,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.142491,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.287231,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.008986,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000435,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000699,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.068698,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.003433,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.005982,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.035116,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001741,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002414,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.141721,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.007036,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.010181,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.01883,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000892,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.001347,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.1195,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.00587,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.009386,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "2969f8586acc1fee634860f40b6a316be7a5380e",
          "message": "chore(release): v0.1.14",
          "timestamp": "2026-10-27T19:11:01Z",
          "tree_id": "d7bc7c8f7847a824027ea751f90a35a6c9ddb1aa",
          "url": "https://github.com/guibeira/datastore-emulator/commit/2969f8586acc1fee634860f40b6a316be7a5380e"
        },
        "date": 1793129557394,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.039588,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.001895,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.00306,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.945538,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.04251,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.096324,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - avg seconds per client",
            "value": 0.266576,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p50 s/op",
            "value": 0.013389,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Rust) - p95 s/op",
            "value": 0.016521,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - avg seconds per client",
            "value": 3.573251,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p50 s/op",
            "value": 0.170753,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Cursor Pagination (5x100) (Java) - p95 s/op",
            "value": 0.324047,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - avg seconds per client",
            "value": 0.006006,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p50 s/op",
            "value": 0.000292,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Rust) - p95 s/op",
            "value": 0.000484,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - avg seconds per client",
            "value": 0.030373,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p50 s/op",
            "value": 0.001433,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get (Java) - p95 s/op",
            "value": 0.002745,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - avg seconds per client",
            "value": 0.030426,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p50 s/op",
            "value": 0.001495,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Rust) - p95 s/op",
            "value": 0.002289,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - avg seconds per client",
            "value": 0.109242,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p50 s/op",
            "value": 0.005184,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Get Multi (100) (Java) - p95 s/op",
            "value": 0.010092,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - avg seconds per client",
            "value": 0.078977,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p50 s/op",
            "value": 0.00343,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Rust) - p95 s/op",
            "value": 0.004684,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - avg seconds per client",
            "value": 3.05681,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p50 s/op",
            "value": 0.146875,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java) - p95 s/op",
            "value": 0.251629,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - avg seconds per client",
            "value": 0.036349,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p50 s/op",
            "value": 0.001839,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Rust) - p95 s/op",
            "value": 0.002665,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - avg seconds per client",
            "value": 0.813611,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p50 s/op",
            "value": 0.038837,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Query Filter+Order (5k) (Java) - p95 s/op",
            "value": 0.088088,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.010554,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.000494,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.000787,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 2.208863,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.105431,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.22193,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.006935,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.000337,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.000542,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.058882,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.00288,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.005058,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - avg seconds per client",
            "value": 0.028216,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p50 s/op",
            "value": 0.001373,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Rust) - p95 s/op",
            "value": 0.002118,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - avg seconds per client",
            "value": 0.095592,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p50 s/op",
            "value": 0.004788,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Transaction RMW (Java) - p95 s/op",
            "value": 0.006742,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - avg seconds per client",
            "value": 0.014583,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p50 s/op",
            "value": 0.000714,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Rust) - p95 s/op",
            "value": 0.00103,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - avg seconds per client",
            "value": 0.078754,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p50 s/op",
            "value": 0.003791,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Update (Java) - p95 s/op",
            "value": 0.006279,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
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
        "date": 1791922102556,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 24.24,
            "unit": "x",
            "extra": "Java p50 0.0664163 s / Rust p50 0.00274041 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 6.42,
            "unit": "x",
            "extra": "Java p50 0.206487 s / Rust p50 0.0321692 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 2.96,
            "unit": "x",
            "extra": "Java p50 0.00158902 s / Rust p50 0.000535989 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.27,
            "unit": "x",
            "extra": "Java p50 0.00593157 s / Rust p50 0.00261018 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 16.35,
            "unit": "x",
            "extra": "Java p50 0.21114 s / Rust p50 0.0129106 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 12.99,
            "unit": "x",
            "extra": "Java p50 0.0416187 s / Rust p50 0.00320406 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 27.45,
            "unit": "x",
            "extra": "Java p50 0.17868 s / Rust p50 0.00650997 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 6.25,
            "unit": "x",
            "extra": "Java p50 0.00331856 s / Rust p50 0.000530911 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 2.72,
            "unit": "x",
            "extra": "Java p50 0.00586259 s / Rust p50 0.00215487 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.09,
            "unit": "x",
            "extra": "Java p50 0.0047385 s / Rust p50 0.00115742 s"
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
          "id": "10382cff471456f2ece16c5f9865d7f7cf00f3be",
          "message": "chore(release): v0.1.4",
          "timestamp": "2026-05-23T18:55:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/10382cff471456f2ece16c5f9865d7f7cf00f3be"
        },
        "date": 1791922337144,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 21.23,
            "unit": "x",
            "extra": "Java p50 0.06383 s / Rust p50 0.00300606 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 5.89,
            "unit": "x",
            "extra": "Java p50 0.215194 s / Rust p50 0.036512 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 4.49,
            "unit": "x",
            "extra": "Java p50 0.00219511 s / Rust p50 0.000488987 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.43,
            "unit": "x",
            "extra": "Java p50 0.0065797 s / Rust p50 0.00270878 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 18.83,
            "unit": "x",
            "extra": "Java p50 0.259726 s / Rust p50 0.0137915 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 11.87,
            "unit": "x",
            "extra": "Java p50 0.0449868 s / Rust p50 0.00379029 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 24.67,
            "unit": "x",
            "extra": "Java p50 0.193738 s / Rust p50 0.00785469 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 6.21,
            "unit": "x",
            "extra": "Java p50 0.00337653 s / Rust p50 0.000543759 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 2.61,
            "unit": "x",
            "extra": "Java p50 0.00584418 s / Rust p50 0.00224219 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.11,
            "unit": "x",
            "extra": "Java p50 0.00472126 s / Rust p50 0.00114908 s"
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
          "id": "6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428",
          "message": "chore(release): v0.1.5",
          "timestamp": "2026-10-11T13:59:59Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428"
        },
        "date": 1791922578412,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 9.68,
            "unit": "x",
            "extra": "Java p50 0.0638423 s / Rust p50 0.00659587 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 5.48,
            "unit": "x",
            "extra": "Java p50 0.282678 s / Rust p50 0.0516044 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 3.6,
            "unit": "x",
            "extra": "Java p50 0.00114169 s / Rust p50 0.000316946 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 1.92,
            "unit": "x",
            "extra": "Java p50 0.0059915 s / Rust p50 0.00312525 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 8.29,
            "unit": "x",
            "extra": "Java p50 0.263546 s / Rust p50 0.0318091 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 6.68,
            "unit": "x",
            "extra": "Java p50 0.0551389 s / Rust p50 0.00825984 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 11.83,
            "unit": "x",
            "extra": "Java p50 0.243212 s / Rust p50 0.0205518 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 6.52,
            "unit": "x",
            "extra": "Java p50 0.00303191 s / Rust p50 0.000464764 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.35,
            "unit": "x",
            "extra": "Java p50 0.004928 s / Rust p50 0.00146904 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 5.9,
            "unit": "x",
            "extra": "Java p50 0.00412036 s / Rust p50 0.000698848 s"
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
          "id": "da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa",
          "message": "chore(release): v0.1.6",
          "timestamp": "2026-10-12T23:57:08Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa"
        },
        "date": 1791922749867,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 3.35,
            "unit": "x",
            "extra": "Java p50 0.0253817 s / Rust p50 0.00756926 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 3.22,
            "unit": "x",
            "extra": "Java p50 0.112534 s / Rust p50 0.0349882 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 3.71,
            "unit": "x",
            "extra": "Java p50 0.000647662 s / Rust p50 0.000174546 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 1.51,
            "unit": "x",
            "extra": "Java p50 0.00281467 s / Rust p50 0.00185993 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 32.91,
            "unit": "x",
            "extra": "Java p50 0.117807 s / Rust p50 0.00357965 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 3.76,
            "unit": "x",
            "extra": "Java p50 0.0227133 s / Rust p50 0.00603655 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 301.04,
            "unit": "x",
            "extra": "Java p50 0.0964691 s / Rust p50 0.000320451 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 6.42,
            "unit": "x",
            "extra": "Java p50 0.00139069 s / Rust p50 0.000216729 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.25,
            "unit": "x",
            "extra": "Java p50 0.00255256 s / Rust p50 0.000785397 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.34,
            "unit": "x",
            "extra": "Java p50 0.00198225 s / Rust p50 0.000456513 s"
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
          "id": "7a6d688fcc3f3c5876bc30da73d529054db7a9c4",
          "message": "chore(release): v0.1.7",
          "timestamp": "2026-10-13T13:12:34Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7a6d688fcc3f3c5876bc30da73d529054db7a9c4"
        },
        "date": 1791922982301,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 32.04,
            "unit": "x",
            "extra": "Java p50 0.0682052 s / Rust p50 0.00212875 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 10.18,
            "unit": "x",
            "extra": "Java p50 0.215579 s / Rust p50 0.0211799 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 3.81,
            "unit": "x",
            "extra": "Java p50 0.00181478 s / Rust p50 0.000476135 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.31,
            "unit": "x",
            "extra": "Java p50 0.00536073 s / Rust p50 0.00231911 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 46.72,
            "unit": "x",
            "extra": "Java p50 0.254391 s / Rust p50 0.00544473 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 18.29,
            "unit": "x",
            "extra": "Java p50 0.0456622 s / Rust p50 0.00249663 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 287.76,
            "unit": "x",
            "extra": "Java p50 0.205681 s / Rust p50 0.000714777 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 7.68,
            "unit": "x",
            "extra": "Java p50 0.0037523 s / Rust p50 0.000488525 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.05,
            "unit": "x",
            "extra": "Java p50 0.00605374 s / Rust p50 0.00198494 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 5.1,
            "unit": "x",
            "extra": "Java p50 0.0051127 s / Rust p50 0.00100344 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "76097ca1e69fea9abdd28f7b0758c7c222d017cf",
          "message": "chore(release): v0.1.8",
          "timestamp": "2026-10-21T17:14:59Z",
          "tree_id": "3f7c57ef9a4ae4845b18a494fb9d57c73fd4c0fd",
          "url": "https://github.com/guibeira/datastore-emulator/commit/76097ca1e69fea9abdd28f7b0758c7c222d017cf"
        },
        "date": 1792603999680,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 33.75,
            "unit": "x",
            "extra": "Java p50 0.0694807 s / Rust p50 0.00205874 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 11.91,
            "unit": "x",
            "extra": "Java p50 0.230479 s / Rust p50 0.0193453 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 3.83,
            "unit": "x",
            "extra": "Java p50 0.00198367 s / Rust p50 0.000517338 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 3.52,
            "unit": "x",
            "extra": "Java p50 0.00741835 s / Rust p50 0.0021091 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 40.09,
            "unit": "x",
            "extra": "Java p50 0.208323 s / Rust p50 0.00519673 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 17.78,
            "unit": "x",
            "extra": "Java p50 0.0438586 s / Rust p50 0.00246636 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 189.48,
            "unit": "x",
            "extra": "Java p50 0.158386 s / Rust p50 0.000835887 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 5.79,
            "unit": "x",
            "extra": "Java p50 0.00337617 s / Rust p50 0.000582943 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.38,
            "unit": "x",
            "extra": "Java p50 0.00697988 s / Rust p50 0.00206692 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 4.74,
            "unit": "x",
            "extra": "Java p50 0.00551783 s / Rust p50 0.00116303 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "4c566cefa0cf2d99dfc7275539fd920a3fe009f4",
          "message": "chore(release): v0.1.9",
          "timestamp": "2026-10-26T21:14:53Z",
          "tree_id": "231d71c22f6a1676d9a4dee4a13a33d2cd864f1f",
          "url": "https://github.com/guibeira/datastore-emulator/commit/4c566cefa0cf2d99dfc7275539fd920a3fe009f4"
        },
        "date": 1793050355560,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 27.81,
            "unit": "x",
            "extra": "Java p50 0.0272682 s / Rust p50 0.000980619 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 15.6,
            "unit": "x",
            "extra": "Java p50 0.126379 s / Rust p50 0.0080989 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 4.56,
            "unit": "x",
            "extra": "Java p50 0.00092664 s / Rust p50 0.000203402 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 2.53,
            "unit": "x",
            "extra": "Java p50 0.00297207 s / Rust p50 0.00117607 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 42.37,
            "unit": "x",
            "extra": "Java p50 0.0956494 s / Rust p50 0.00225735 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 23.97,
            "unit": "x",
            "extra": "Java p50 0.0279041 s / Rust p50 0.00116403 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 249.76,
            "unit": "x",
            "extra": "Java p50 0.0803638 s / Rust p50 0.000321767 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 8.74,
            "unit": "x",
            "extra": "Java p50 0.00170336 s / Rust p50 0.000194955 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.87,
            "unit": "x",
            "extra": "Java p50 0.00273085 s / Rust p50 0.000704952 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 5.49,
            "unit": "x",
            "extra": "Java p50 0.00235949 s / Rust p50 0.00042974 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "efd5db4dce4b87f617e07017ac372256f528d22f",
          "message": "chore(release): v0.1.10",
          "timestamp": "2026-10-26T23:19:31Z",
          "tree_id": "690ddb2f7b8b43b843452f50e50e4a9122079137",
          "url": "https://github.com/guibeira/datastore-emulator/commit/efd5db4dce4b87f617e07017ac372256f528d22f"
        },
        "date": 1793057825449,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 26.66,
            "unit": "x",
            "extra": "Java p50 0.0287322 s / Rust p50 0.00107792 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 15.72,
            "unit": "x",
            "extra": "Java p50 0.137259 s / Rust p50 0.00873385 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 7.05,
            "unit": "x",
            "extra": "Java p50 0.00132857 s / Rust p50 0.000188454 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 3.2,
            "unit": "x",
            "extra": "Java p50 0.00334422 s / Rust p50 0.00104629 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 56.18,
            "unit": "x",
            "extra": "Java p50 0.133347 s / Rust p50 0.00237373 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 23.61,
            "unit": "x",
            "extra": "Java p50 0.0287399 s / Rust p50 0.0012174 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 284.6,
            "unit": "x",
            "extra": "Java p50 0.101482 s / Rust p50 0.000356573 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 9.51,
            "unit": "x",
            "extra": "Java p50 0.00195585 s / Rust p50 0.000205647 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.34,
            "unit": "x",
            "extra": "Java p50 0.00265875 s / Rust p50 0.000796563 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 5.16,
            "unit": "x",
            "extra": "Java p50 0.00221624 s / Rust p50 0.000429903 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "1ec10508bea3286896f70d1114e7f6fc318eb8a8",
          "message": "chore(release): v0.1.11",
          "timestamp": "2026-10-27T01:04:30Z",
          "tree_id": "3a65432531c6b360ecaeedcc18bf8ae5c4c29cac",
          "url": "https://github.com/guibeira/datastore-emulator/commit/1ec10508bea3286896f70d1114e7f6fc318eb8a8"
        },
        "date": 1793063978145,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 30.88,
            "unit": "x",
            "extra": "Java p50 0.0423899 s / Rust p50 0.0013729 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 13.2,
            "unit": "x",
            "extra": "Java p50 0.17954 s / Rust p50 0.0135993 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 3.92,
            "unit": "x",
            "extra": "Java p50 0.00108654 s / Rust p50 0.000277288 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 3.26,
            "unit": "x",
            "extra": "Java p50 0.0048226 s / Rust p50 0.00147734 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 45.61,
            "unit": "x",
            "extra": "Java p50 0.15627 s / Rust p50 0.00342652 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 20.36,
            "unit": "x",
            "extra": "Java p50 0.0371737 s / Rust p50 0.00182581 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 264.16,
            "unit": "x",
            "extra": "Java p50 0.123752 s / Rust p50 0.00046847 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 7.88,
            "unit": "x",
            "extra": "Java p50 0.00261408 s / Rust p50 0.000331838 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 3.15,
            "unit": "x",
            "extra": "Java p50 0.00398691 s / Rust p50 0.0012646 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 6.4,
            "unit": "x",
            "extra": "Java p50 0.00375386 s / Rust p50 0.000586354 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "7016e9bcfe7804417e59d57df7f92e988bcc92aa",
          "message": "chore(release): v0.1.12",
          "timestamp": "2026-10-27T13:56:09Z",
          "tree_id": "81fd96a6bfeb073e83859b7cbe1a2d2984d13232",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7016e9bcfe7804417e59d57df7f92e988bcc92aa"
        },
        "date": 1793110263612,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 25.85,
            "unit": "x",
            "extra": "Java p50 0.0467321 s / Rust p50 0.00180747 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 13.92,
            "unit": "x",
            "extra": "Java p50 0.239743 s / Rust p50 0.0172266 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 4.78,
            "unit": "x",
            "extra": "Java p50 0.00137759 s / Rust p50 0.000288463 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 3.42,
            "unit": "x",
            "extra": "Java p50 0.00515631 s / Rust p50 0.00150623 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 71.6,
            "unit": "x",
            "extra": "Java p50 0.259682 s / Rust p50 0.00362672 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 25.05,
            "unit": "x",
            "extra": "Java p50 0.0517355 s / Rust p50 0.00206498 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 455.87,
            "unit": "x",
            "extra": "Java p50 0.226947 s / Rust p50 0.000497835 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 7.16,
            "unit": "x",
            "extra": "Java p50 0.00248892 s / Rust p50 0.000347802 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 4.62,
            "unit": "x",
            "extra": "Java p50 0.0060843 s / Rust p50 0.00131754 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 6.99,
            "unit": "x",
            "extra": "Java p50 0.00509504 s / Rust p50 0.000728808 s"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "78835c2f7e0c5920e0d8bb8859db1ca5210fe882",
          "message": "chore(release): v0.1.13",
          "timestamp": "2026-10-27T15:54:48Z",
          "tree_id": "7dec2b6ada02970de1030ff0e17bd6e58870ece1",
          "url": "https://github.com/guibeira/datastore-emulator/commit/78835c2f7e0c5920e0d8bb8859db1ca5210fe882"
        },
        "date": 1793117631612,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 23.53,
            "unit": "x",
            "extra": "Java p50 0.0596929 s / Rust p50 0.00253666 s"
          },
          {
            "name": "Cursor Pagination (5x100) (Java/Rust) - p50 speedup",
            "value": 13.83,
            "unit": "x",
            "extra": "Java p50 0.229089 s / Rust p50 0.0165645 s"
          },
          {
            "name": "Get (Java/Rust) - p50 speedup",
            "value": 5.09,
            "unit": "x",
            "extra": "Java p50 0.00209083 s / Rust p50 0.000410591 s"
          },
          {
            "name": "Get Multi (100) (Java/Rust) - p50 speedup",
            "value": 3.32,
            "unit": "x",
            "extra": "Java p50 0.00620186 s / Rust p50 0.00186737 s"
          },
          {
            "name": "Mixed (get, get_multi, update, query) (Java/Rust) - p50 speedup",
            "value": 40.16,
            "unit": "x",
            "extra": "Java p50 0.175871 s / Rust p50 0.0043798 s"
          },
          {
            "name": "Query Filter+Order (5k) (Java/Rust) - p50 speedup",
            "value": 19.92,
            "unit": "x",
            "extra": "Java p50 0.0481139 s / Rust p50 0.00241526 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 224.61,
            "unit": "x",
            "extra": "Java p50 0.142491 s / Rust p50 0.000634403 s"
          },
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 7.9,
            "unit": "x",
            "extra": "Java p50 0.00343342 s / Rust p50 0.000434825 s"
          },
          {
            "name": "Transaction RMW (Java/Rust) - p50 speedup",
            "value": 4.04,
            "unit": "x",
            "extra": "Java p50 0.00703554 s / Rust p50 0.0017411 s"
          },
          {
            "name": "Update (Java/Rust) - p50 speedup",
            "value": 6.58,
            "unit": "x",
            "extra": "Java p50 0.00586984 s / Rust p50 0.000891965 s"
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
        "date": 1791922104390,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 161.35,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 198.8,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 70.78,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 70.78,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 142.31,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 207.73,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1867.78,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1866.75,
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
          "id": "10382cff471456f2ece16c5f9865d7f7cf00f3be",
          "message": "chore(release): v0.1.4",
          "timestamp": "2026-05-23T18:55:43Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/10382cff471456f2ece16c5f9865d7f7cf00f3be"
        },
        "date": 1791922338882,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 184.15,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 212.81,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 71.46,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 71.46,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 139.82,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 205.17,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1815.55,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1815.55,
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
          "id": "6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428",
          "message": "chore(release): v0.1.5",
          "timestamp": "2026-10-11T13:59:59Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/6163e37ac6ab5bf2e39fb9b1128bdc274d0b0428"
        },
        "date": 1791922581178,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 169.85,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 201.2,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 69.94,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 69.57,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 136.69,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 200.58,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1822.72,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1822.72,
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
          "id": "da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa",
          "message": "chore(release): v0.1.6",
          "timestamp": "2026-10-12T23:57:08Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/da07664cd4a34d8c5a5e4cabbd1c6671af7c60fa"
        },
        "date": 1791922752208,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 143.46,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 189.66,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 100.8,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 82.27,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 145.35,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 200.38,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 2245.63,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 2245.63,
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
          "id": "7a6d688fcc3f3c5876bc30da73d529054db7a9c4",
          "message": "chore(release): v0.1.7",
          "timestamp": "2026-10-13T13:12:34Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7a6d688fcc3f3c5876bc30da73d529054db7a9c4"
        },
        "date": 1791922984825,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 157.68,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 191.74,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 71.56,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 71.56,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 141.54,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 208.38,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1643.52,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1643.52,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 10 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "76097ca1e69fea9abdd28f7b0758c7c222d017cf",
          "message": "chore(release): v0.1.8",
          "timestamp": "2026-10-21T17:14:59Z",
          "tree_id": "3f7c57ef9a4ae4845b18a494fb9d57c73fd4c0fd",
          "url": "https://github.com/guibeira/datastore-emulator/commit/76097ca1e69fea9abdd28f7b0758c7c222d017cf"
        },
        "date": 1792604001927,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 116.22,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 175.18,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 75.24,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 75.24,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 148.91,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 205.1,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1827.84,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1816.58,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "4c566cefa0cf2d99dfc7275539fd920a3fe009f4",
          "message": "chore(release): v0.1.9",
          "timestamp": "2026-10-26T21:14:53Z",
          "tree_id": "231d71c22f6a1676d9a4dee4a13a33d2cd864f1f",
          "url": "https://github.com/guibeira/datastore-emulator/commit/4c566cefa0cf2d99dfc7275539fd920a3fe009f4"
        },
        "date": 1793050357307,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 0.01,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 0.01,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 14.15,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 14.15,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 150.29,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 202.61,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1973.25,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1973.25,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "efd5db4dce4b87f617e07017ac372256f528d22f",
          "message": "chore(release): v0.1.10",
          "timestamp": "2026-10-26T23:19:31Z",
          "tree_id": "690ddb2f7b8b43b843452f50e50e4a9122079137",
          "url": "https://github.com/guibeira/datastore-emulator/commit/efd5db4dce4b87f617e07017ac372256f528d22f"
        },
        "date": 1793057827217,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 54.84,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 164.49,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 56.58,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 56.58,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 148.3,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 200.2,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 2118.66,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 2114.56,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "1ec10508bea3286896f70d1114e7f6fc318eb8a8",
          "message": "chore(release): v0.1.11",
          "timestamp": "2026-10-27T01:04:30Z",
          "tree_id": "3a65432531c6b360ecaeedcc18bf8ae5c4c29cac",
          "url": "https://github.com/guibeira/datastore-emulator/commit/1ec10508bea3286896f70d1114e7f6fc318eb8a8"
        },
        "date": 1793063980195,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 85.19,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 170.35,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 56.3,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 56.3,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 144.53,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 200.69,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1940.48,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1940.48,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "7016e9bcfe7804417e59d57df7f92e988bcc92aa",
          "message": "chore(release): v0.1.12",
          "timestamp": "2026-10-27T13:56:09Z",
          "tree_id": "81fd96a6bfeb073e83859b7cbe1a2d2984d13232",
          "url": "https://github.com/guibeira/datastore-emulator/commit/7016e9bcfe7804417e59d57df7f92e988bcc92aa"
        },
        "date": 1793110266883,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 103,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 181.58,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 118.9,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 118.9,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 137.99,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 200.96,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1697.79,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1691.65,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "distinct": true,
          "id": "78835c2f7e0c5920e0d8bb8859db1ca5210fe882",
          "message": "chore(release): v0.1.13",
          "timestamp": "2026-10-27T15:54:48Z",
          "tree_id": "7dec2b6ada02970de1030ff0e17bd6e58870ece1",
          "url": "https://github.com/guibeira/datastore-emulator/commit/78835c2f7e0c5920e0d8bb8859db1ca5210fe882"
        },
        "date": 1793117633269,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "CPU (Rust) - mean %",
            "value": 99.57,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Rust) - peak %",
            "value": 169.98,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - peak MB",
            "value": 125.1,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Rust) - end MB",
            "value": 125.1,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - mean %",
            "value": 148.68,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "CPU (Java) - peak %",
            "value": 202.71,
            "unit": "%",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - peak MB",
            "value": 1815.55,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Memory (Java) - end MB",
            "value": 1815.55,
            "unit": "MB",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          }
        ]
      }
    ]
  }
}