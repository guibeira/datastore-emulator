window.BENCHMARK_DATA = {
  "lastUpdate": 1791909903029,
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
        "date": 1791909899774,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.117364,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p50 s/op",
            "value": 0.004618,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Rust) - p95 s/op",
            "value": 0.014559,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.132438,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p50 s/op",
            "value": 0.005943,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Single Insert (Java) - p95 s/op",
            "value": 0.011813,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.80518,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p50 s/op",
            "value": 0.037125,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Rust) - p95 s/op",
            "value": 0.092223,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 3.102025,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p50 s/op",
            "value": 0.057107,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Bulk Insert (50) (Java) - p95 s/op",
            "value": 0.790776,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.415961,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p50 s/op",
            "value": 0.015718,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Rust) - p95 s/op",
            "value": 0.053565,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 2.526301,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p50 s/op",
            "value": 0.104562,
            "unit": "s",
            "extra": "10 clients, 20 runs/client, 5 warm-up, median of 3"
          },
          {
            "name": "Simple Query (Java) - p95 s/op",
            "value": 0.303534,
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
        "date": 1791909902466,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "Single Insert (Java/Rust) - p50 speedup",
            "value": 1.29,
            "unit": "x",
            "extra": "Java p50 0.00594256 s / Rust p50 0.00461753 s"
          },
          {
            "name": "Bulk Insert (50) (Java/Rust) - p50 speedup",
            "value": 1.54,
            "unit": "x",
            "extra": "Java p50 0.0571071 s / Rust p50 0.0371246 s"
          },
          {
            "name": "Simple Query (Java/Rust) - p50 speedup",
            "value": 6.65,
            "unit": "x",
            "extra": "Java p50 0.104562 s / Rust p50 0.0157177 s"
          }
        ]
      }
    ]
  }
}