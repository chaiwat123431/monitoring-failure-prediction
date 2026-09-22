# monitoring-failure-prediction

See [PLANNING.md](./PLANNING.md) for architecture decisions and rationale.

## Run

```bash
cp .env.example .env
docker compose up --build
```

- Backend: http://localhost:8000 (`/health`, `/health/ready`)
- Frontend: http://localhost:3000

## Smoke test

```bash
./scripts/smoke.sh
```

## Tests

```bash
cd backend
uv sync --group dev
uv run pytest                 # unit tests, no real dependencies
uv run pytest -m integration  # requires `docker compose up` running
```
