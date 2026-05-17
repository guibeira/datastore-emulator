window.BENCHMARK_DATA = {
  "lastUpdate": 1779056693827,
  "repoUrl": "https://github.com/guibeira/datastore-emulator",
  "entries": {
    "Datastore Emulator Benchmark": [
      {
        "commit": {
          "author": {
            "name": "Guilherme Beira",
            "username": "guibeira",
            "email": "guilherme.vieira.beira@gmail.com"
          },
          "committer": {
            "name": "Guilherme Beira",
            "username": "guibeira",
            "email": "guilherme.vieira.beira@gmail.com"
          },
          "id": "3f3ac42faf0338fb1c76d3fc45740e51d7c5e436",
          "message": "chore(release): v0.1.2-rc3",
          "timestamp": "2026-05-17T22:24:29Z",
          "url": "https://github.com/guibeira/datastore-emulator/commit/3f3ac42faf0338fb1c76d3fc45740e51d7c5e436"
        },
        "date": 1779056693465,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Single Insert (Rust) - avg seconds per client",
            "value": 0.05971,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 0.1791s"
          },
          {
            "name": "Single Insert (Java) - avg seconds per client",
            "value": 0.290697,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 0.8721s"
          },
          {
            "name": "Bulk Insert (50) (Rust) - avg seconds per client",
            "value": 0.05769,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 0.1731s"
          },
          {
            "name": "Bulk Insert (50) (Java) - avg seconds per client",
            "value": 0.34549,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 1.0365s"
          },
          {
            "name": "Simple Query (Rust) - avg seconds per client",
            "value": 0.017707,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 0.0531s"
          },
          {
            "name": "Simple Query (Java) - avg seconds per client",
            "value": 0.297605,
            "unit": "s",
            "extra": "3 clients, 5 runs/client, total 0.8928s"
          }
        ]
      }
    ]
  }
}