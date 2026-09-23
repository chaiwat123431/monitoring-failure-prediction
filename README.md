# monitoring-failure-prediction

See [PLANNING.md](./PLANNING.md) for architecture decisions and rationale.

## Run

```bash
cp .env.example .env
./scripts/fetch_nab_data.sh   # downloads the 2 NAB CSVs this project ingests, into data/ (gitignored)
docker compose up --build
```

- Backend: http://localhost:8000 (`/health`, `/health/ready`)
- Frontend: http://localhost:3000
- `migrate` creates the schema and seeds NAB's anomaly windows, then exits.
- `producer` replays the 2 NAB CSVs into Kafka (real per-row timestamps, not wall-clock time) and
  exits. Set `REPLAY_SPEED` in `.env` to pace it in simulated time (e.g. `60` = 1 simulated hour
  per real minute); default `0` sends rows as fast as Kafka accepts them.
- `consumer` runs continuously, upserting into TimescaleDB's `raw_metrics` hypertable.

## Train the model

Once `raw_metrics` is populated (the ingestion steps above):

```bash
cd backend
uv sync
DATABASE_URL=postgresql://app:app@localhost:${POSTGRES_PORT:-5432}/monitoring uv run python scripts/train.py
```

Trains one `IsolationForest` on both series, evaluates against `nab_anomaly_windows`, prints
precision/recall/F1, and saves `models/isolation_forest.joblib` (gitignored, regenerable). See
`PLANNING.md` §4 AD-15..AD-19 and §6 for the real measured metrics.

## Smoke test

```bash
./scripts/smoke.sh
```

## Tests

```bash
cd backend
uv sync --group dev
uv run pytest                 # unit tests, no real dependencies
uv run pytest -m integration  # requires `docker compose up` running, incl. migrate/producer/consumer
```
