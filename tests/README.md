# Running the Tests

This directory contains the compatibility tests for the Rust Datastore emulator. The tests are designed to be run against both the local emulator and a real Google Cloud Datastore database to ensure maximum fidelity.

## Testing Methods

There are two main ways to run the test suite.

### 1. Using Local Emulators (Docker Compose)

This is the standard and fastest method for daily development. It uses `docker-compose` to start our Rust emulator and the official Google emulator, and then runs the tests comparing the two.

**Steps:**

1.  **Start the emulators:**
    From the project root, run the following command to build and start the containers in the background.

    ```bash
    docker compose up --build -d
    ```

2.  **Run the tests:**
    With the emulators running, execute the test suite with `pytest`.

    ```bash
    poetry run pytest -vv
    ```

### 2. Using a Real Google Cloud Datastore Database

To ensure full compatibility with the production environment, the most reliable way is to run the tests directly against a real Google Cloud Datastore database. This validates our emulator's behavior against the actual service, rather than another emulator.

**Prerequisites:**

-   Have a Google Cloud project with the Datastore API enabled.
-   Be authenticated locally. The easiest way is through the gcloud CLI:
    ```bash
    gcloud auth application-default login
    ```

**Create the indexes:**
-   Ensure that the necessary indexes are created in your Google Cloud Datastore. You can find the required index definitions in the `tests/index.yaml` file in this repository. Use the following command to create them:

    ```bash
    gcloud datastore indexes create index.yaml --project=<your-project-id> --database=<your-database-name>
    ```

**Execution:**

To run the tests against your real database, set the following environment variables in the `pytest` command:

-   `USE_REAL_DB=1`: Enables testing mode with the real database.
-   `DATASTORE_PROJECT_ID`: Your Google Cloud project ID.
-   `DATASTORE_DATABASE_NAME`: The name of your Datastore database (often `(default)` or a custom name).

**Example command:**

```bash
USE_REAL_DB=1 DATASTORE_DATABASE_NAME=<your-database-name> DATASTORE_PROJECT_ID=<your-project-id> poetry run pytest -vv
```
