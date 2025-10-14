
### How to run the benchmark


First step is make sure you have python installed. This project uses poetry, so you need to install it first. You can find the installation instructions [here](https://python-poetry.org/docs/#installation). 

For convinience, you use a docker-compose file to spin up the datastore emulatores. This means, you need to have docker and docker-compose installed. You can find the installation instructions [here](https://docs.docker.com/get-docker/) and [here](https://docs.docker.com/compose/install/).


Once you have docker and docker-compose installed, you can run the following command to start the emulators:

```bash
docker compose up --build -d
```
This will start the emulators in the background. You can stop them by running:

```bash
docker compose down
```


Once you have poetry installed, you can install the dependencies by running the following commands in the project root directory:

```bash
poetry install
```

Once the dependencies are installed, you can run the benchmark by running the following command:

```bash
poetry run python benchmark/test_benchmark.py --num-clients 30 --num-runs 5
```

Done! You should see the benchmark results printed in the console like this:

```bash
--- Benchmark Summary ---

Operation: Single Insert
  - Rust (30 clients, 5 runs each):
    - Total time: 0.8413 seconds
    - Avg time per client: 0.0280 seconds
  - Java (30 clients, 5 runs each):
    - Total time: 48.1050 seconds
    - Avg time per client: 1.6035 seconds
  - Verdict: Rust was 57.18x faster overall.

Operation: Bulk Insert (50)
  - Rust (30 clients, 5 runs each):
    - Total time: 9.5209 seconds
    - Avg time per client: 0.3174 seconds
  - Java (30 clients, 5 runs each):
    - Total time: 163.7277 seconds
    - Avg time per client: 5.4576 seconds
  - Verdict: Rust was 17.20x faster overall.

Operation: Simple Query
  - Rust (30 clients, 5 runs each):
    - Total time: 2.2610 seconds
    - Avg time per client: 0.0754 seconds
  - Java (30 clients, 5 runs each):
    - Total time: 29.3397 seconds
    - Avg time per client: 0.9780 seconds
```





