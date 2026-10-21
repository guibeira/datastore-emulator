window.BENCHMARK_DATA = {
  "lastUpdate": 1792603997918,
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
      }
    ]
  }
}