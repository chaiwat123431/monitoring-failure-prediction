# PLANNING.md — AI Monitoring & Failure Prediction Platform

## 1. Project Goal

Portfolio project (solo, not production-grade) demonstrating a streaming data pipeline with
ML-based anomaly/failure detection, built to match keywords explicitly listed in IBM Consulting's
Machine Learning Developer Intern / AI & Automation Data Scientist Associate postings: Kafka,
scikit-learn, real-time pipelines, MLOps-adjacent workflow. Secondary target: CGI AI innovation
cell posting.

Not a deadline-blocking project — Project 1 (RAG Knowledge Assistant) already covers the CGI
application. Priority here is doing the new stack (Kafka, TimescaleDB, WebSocket) correctly over
doing it fast.

## 2. MVP Scope (decided)

- **Data source**: NAB (Numenta Anomaly Benchmark) — real server/system metrics with labeled
  anomaly windows. Rejected NASA Turbofan (wrong domain — engine degradation, not server/IoT
  metrics; doesn't match the "monitoring platform" narrative).
- **Streaming simulation**: replay NAB CSV rows chronologically through Kafka (via Redpanda —
  Kafka-API compatible, single container, no Zookeeper needed, lighter for local dev) as a producer
  script, at an accelerated pace, to simulate a live metrics feed without needing a real data
  source.
- **Storage**: TimescaleDB hypertable for raw metrics + computed anomaly scores.
- **Model**: unsupervised anomaly detection (Isolation Forest, evaluated against NAB's labeled
  windows for precision/recall/F1) trained on rolling-window features (mean, std, rate of change).
  Time-based train/test split (train on early portion of each series, evaluate on later portion) —
  never random split, to avoid leakage across a time series.
- **Real-time dashboard**: full WebSocket live streaming (Kafka → FastAPI consumer → WebSocket →
  Next.js/Recharts), not polling. Single-user MVP scope: no horizontal scaling of WS connections
  needed, but connect/disconnect/reconnect and error handling must be done properly.
- **Cost**: Docker Compose (Redpanda + TimescaleDB + backend + frontend) for local dev, fully free.
  For the public demo: real Kafka/TimescaleDB running 24/7 on a paid host is likely too costly for
  a portfolio project — to decide during the deployment slice (options: one-command
  `docker-compose up` demo + recorded video/gif on README, vs. a lightweight always-on "replay
  mode" deploy).

## 3. Build Order (slices, one PR per slice — same discipline as Project 1)

1. **Scaffold** — repo structure, `docker-compose.yml` (Redpanda, TimescaleDB, backend, frontend
   skeletons), this PLANNING.md, base FastAPI + Next.js scaffolds.
2. **Ingestion** — NAB dataset selection + producer script (replay to Kafka topic), consumer script
   (Kafka → TimescaleDB), verified end-to-end with real messages flowing.
3. **Model** — offline training script/notebook: feature engineering (rolling window stats),
   time-based split, Isolation Forest training, evaluation against NAB labels (precision/recall/F1
   logged in this file), model artifact saved via `joblib`.
4. **API** — FastAPI: REST endpoints (historical query, model metadata) + WebSocket endpoint
   broadcasting live metric + anomaly flag to connected clients; inference integrated into the
   consumer path.
5. **Frontend** — Next.js + Recharts dashboard: live chart via WebSocket client, anomaly points
   highlighted, alerts panel, historical view.
6. **Deployment** — decide and implement the zero/low-cost demo strategy above.

## 4. Architecture Decisions

_(filled in as each slice is built — include rejected alternatives and why)_

### Slice 1 — Scaffold (status: DONE)

Scope guard: Slice 1 delivers structure + a runnable, empty skeleton. No ingestion, no schema, no
model. Decisions that depend on later slices are listed under "Deferred" instead of being guessed.

#### AD-1. Monorepo, two independent apps

```
monitoring-failure-prediction/
├── docker-compose.yml
├── .env.example              # committed; real .env is gitignored (already is)
├── PLANNING.md
├── README.md                 # minimal: how to run, link to PLANNING.md
├── backend/
│   ├── Dockerfile
│   ├── pyproject.toml        # + uv.lock (pinned, reproducible)
│   ├── app/
│   │   ├── main.py           # FastAPI app + lifespan (startup/shutdown hooks live here)
│   │   ├── config.py         # pydantic-settings: all env access goes through this one module
│   │   └── api/health.py     # /health, /health/ready
│   └── tests/
│       ├── unit/             # no I/O
│       └── integration/      # real dependencies, marked so they can be run separately
├── frontend/
│   ├── Dockerfile
│   ├── package.json / package-lock.json
│   └── src/app/{layout,page}.tsx   # App Router, empty page
├── data/                     # NAB dataset (Slice 2) — gitignored, never committed
└── scripts/                  # smoke test now; producer/train scripts in later slices
```

- **Why**: one repo = one PR per slice touches whatever it needs atomically (same discipline as
  Project 1); the two apps share nothing at build time, so they get separate Dockerfiles and
  dependency manifests.
- **Rejected**: (a) *polyrepo* — pointless overhead for a solo portfolio project, breaks
  "one PR per slice". (b) *`services/` wrapper folder* — extra nesting for 2 apps, no benefit.
  (c) *creating empty `ingestion/`, `ml/` dirs now* — git doesn't track empty dirs and speculative
  layout gets rewritten when the real code shows up. Layout for those is reserved in AD-9 only.

#### AD-2. Docker Compose topology (4 services)

| Service | Image / build | Host port (bound to 127.0.0.1) | Healthcheck | Depends on (healthy) |
|---|---|---|---|---|
| `redpanda` | `redpandadata/redpanda`, version pinned | 19092 (Kafka, external listener) | `rpk cluster health` | — |
| `timescaledb` | `timescale/timescaledb`, `-pg17` variant, version pinned | 5432 | `pg_isready` | — |
| `backend` | `./backend` (target `dev`) | 8000 | `GET /health` | redpanda, timescaledb |
| `frontend` | `./frontend` (target `dev`) | 3000 | HTTP GET on `/` | backend |

- One default compose network; services reach each other by service name.
- Named volumes for TimescaleDB and Redpanda data (survive `down`, wiped by `down -v`).
- Every published port bound to `127.0.0.1` (a dev DB and broker with default creds must not be
  reachable from the LAN) and configurable from `.env` (5432 is often already taken by a local
  Postgres).
- `depends_on: condition: service_healthy` everywhere — plain `depends_on` only waits for the
  container to *start*, which is exactly the race that makes "compose up" flaky.
- **Image tags are pinned, never `latest`.** The exact tags are chosen and verified at pull time in
  the implementation step (not yet verified — the Docker daemon was not running when this was
  drafted); both images publish arm64 builds, which matters as the dev machine is Apple Silicon.
- **Rejected**: (a) *Kafka + Zookeeper / KRaft Kafka image* — already decided against (PLANNING §2),
  heavier. (b) *Redpanda Console as a 5th service* — useful, but the requested scope is 4 services;
  `rpk` inside the container covers inspection. Can be added later behind a compose `profile`.
  (c) *Plain `postgres` + manual extension install* — the Timescale image ships the extension.
  (d) *Bind-mounting DB data to a host folder* — permission/perf quirks on macOS; named volumes are
  simpler.

#### AD-3. Redpanda: two listeners (the classic Kafka-in-Docker trap)

A Kafka client connects to the bootstrap address, then is told to reconnect to the broker's
**advertised** address. A single advertised address cannot be correct both for containers
(`redpanda:9092`) and for tools run on the host (`localhost:19092`) — the failure mode is "TCP
connect works, then the client hangs". So:

- `internal://redpanda:9092` — used by `backend` inside the compose network.
- `external://localhost:19092` — used by host-side tools (`rpk`, and the Slice 2 producer script if
  run from the host).
- Run single-node in `--mode dev-container` (relaxed durability defaults — **dev only**), `--smp 1`
  and a capped `--memory` so it doesn't eat the Docker Desktop VM.
- Topics are **not** auto-created implicitly; Slice 2 creates them explicitly (partition count and
  key choice matter for per-series ordering and are a Slice 2 decision).

#### AD-4. TimescaleDB: extension only, no schema yet

- Slice 1 only proves the container is healthy and reachable. The hypertable schema depends on the
  NAB dataset shape and is a Slice 2 deliverable.
- **Rejected**: creating a placeholder `metrics` table now — it would be a guess about the data
  model and would be rewritten in Slice 2 anyway.

#### AD-5. Backend: FastAPI, Python 3.12, uv

- **Python 3.12 in the image** (`python:3.12-slim`), not the 3.13 present on the host: scikit-learn
  and the data stack have the longest-tested wheels on 3.12, and the container is the source of
  truth, not the laptop.
- **uv + `pyproject.toml` + committed lockfile**: reproducible installs, fast image builds.
  *Rejected*: `requirements.txt` (no lock/resolution guarantees), Poetry (slower, heavier for no gain
  here).
- **`config.py` (pydantic-settings)** is the only place that reads env vars — typed, validated at
  startup (fail fast on a missing `DATABASE_URL` rather than at first query).
- **Dev image runs `uvicorn --reload`** with `./backend/app` bind-mounted. Compose here is a
  local-dev tool (PLANNING §2); the prod image is a Slice 6 concern (see AD-8).
- **Non-root user** in the image, even in dev — cheap and avoids root-owned files in bind mounts.

#### AD-6. Health endpoints: liveness vs readiness

- `GET /health` — **liveness**: process is up, does **no I/O**. Used by the compose healthcheck of
  `backend`. Must never fail because a dependency is down (otherwise an orchestrator would restart a
  healthy process in a loop).
- `GET /health/ready` — **readiness**: checks TimescaleDB (`SELECT 1`) and Redpanda (cluster
  metadata fetch), each with a short hard timeout, returns `200` or `503` with a per-dependency
  JSON body (`{"database": "ok", "kafka": "error: timeout"}`), never a stack trace.
- **Why include readiness in Slice 1** (this is the item most worth your validation): without it,
  "4 containers are green" only proves the containers *started*. It would not catch a wrong
  hostname, wrong credentials, or the advertised-listener problem from AD-3 — bugs that would
  otherwise surface in Slice 2 in a much harder-to-debug context. Cost: two client libs pulled in
  early (provisionally `psycopg[binary]` 3.x and `aiokafka`; both re-confirmed in Slice 2).
- **Fallback if you prefer strict scope**: readiness via a bare TCP connect
  (`asyncio.open_connection`), zero extra dependencies, but blind to auth and to the listener issue.
- **Rejected**: a single `/health` that also checks dependencies (conflates liveness/readiness, see
  above).

#### AD-7. Frontend: Next.js (App Router, TypeScript, npm)

- Scaffolded by hand (a few files) rather than `create-next-app`, to avoid its generated demo
  content and unpinned choices; page is intentionally empty.
- Dev container runs `next dev` with source bind-mounted; `node_modules` lives in an anonymous
  volume so the host's macOS-built modules never shadow the container's. File-watching over the
  Docker Desktop bind mount can miss changes, hence polling enabled in dev.
- **Two different backend URLs, on purpose**: the browser (REST + the Slice 4/5 WebSocket) reaches
  the API at `http://localhost:8000` (`NEXT_PUBLIC_*`, baked in for the client), while any
  server-side fetch from the Next container uses `http://backend:8000`. Conflating them is a common
  "works in dev, breaks in compose" bug; the split is documented here so Slice 5 doesn't rediscover
  it. Backend enables CORS for `http://localhost:3000` only.
- **Rejected**: Vite/CRA SPA (Next.js is the stack named in the target job postings and in
  PLANNING §2); `pnpm`/`yarn` (no benefit, npm is already on the host).

#### AD-8. Configuration & secrets

- `.env.example` committed with dev-only defaults; `.env` is gitignored (already). Compose reads it.
  Nothing secret-shaped is ever hard-coded in `docker-compose.yml`.
- **Deliberately single-target for now**: only a `dev` Docker target exists. A production/demo
  image and compose override are deferred to Slice 6, where the demo strategy is actually decided —
  building it now would be designing for an undecided requirement.

#### AD-9. Reserved decisions for later slices (documented now so Slice 1 doesn't block them)

- **ML code lives in the backend package** (`backend/app/ml/features.py`, training script in
  `backend/scripts/`), *not* in a separate top-level `ml/` project. Feature engineering must be the
  *same code* in offline training and in live inference; two copies is how train/serve skew is
  born. Trade-off accepted: the backend image carries scikit-learn.
- **Where the Kafka consumer runs** (in-process asyncio task in FastAPI vs a separate worker
  container) is decided in Slice 2/4. In-process is simplest for WebSocket fan-out (no extra broker
  between consumer and sockets) but couples ingestion availability to API restarts; a separate
  worker is more robust but needs an extra channel (e.g. Postgres `LISTEN/NOTIFY` or Redis) to reach
  the WebSocket. Slice 1 keeps the option open by making startup logic go through `lifespan` in
  `main.py`; it is *not* a fifth compose service.

**Deferred (need Slice 2+ information, intentionally not decided now):** hypertable schema and
chunk interval; Kafka topic/partition/key design; DB migration tool (Alembic vs plain SQL init
scripts); final DB/Kafka client libraries; consumer topology (above).

## 5. Testing Strategy

_(to define per slice — QE-senior posture: not just happy-path unit tests; realistic edge cases
such as missing Kafka messages, model drift, outlier bursts, TimescaleDB connection loss; explicit
call on which tests need real dependencies (e.g. real Redpanda in CI via testcontainers) vs mocks,
and why)_

### Slice 1 — Scaffold (done)

Nothing here has ML or streaming logic yet, so tests target wiring and failure behaviour:

| Test | Kind | Real deps or mocks? | Why |
|---|---|---|---|
| `/health` returns 200 and does no I/O (deps unreachable → still 200) | unit | none | proves liveness can't be taken down by a dependency |
| `/health/ready`: DB down / Kafka down / both down / both slow (timeout) → 503 with per-dependency detail, bounded latency | unit | **fakes** injected for the two checks | branching and timeout logic is deterministic with fakes; forcing real outages in unit tests is slow and flaky |
| `/health/ready` against real Redpanda + TimescaleDB, incl. stopping each container mid-run | integration / compose smoke (`scripts/smoke.sh`) | **real** | the bugs this slice can actually have (hostnames, credentials, advertised listeners, start-up ordering) exist only against real services — a mock would pass while the stack is broken |
| Config: missing/invalid env var fails at startup with a clear error | unit | none | fail-fast behaviour |

Real-dependency tests are marked so they run separately from the fast unit suite. Whether CI
uses testcontainers or compose services is decided when CI is introduced.

## 6. Measurements Log

_(real measured numbers only, no unverified estimates — latency, throughput, model
precision/recall, etc., filled in as the project progresses)_

## 7. Bugs & False Starts

_(transparency on what didn't work is part of the discipline — filled in as they occur)_

### Slice 1 — Scaffold

- **Webpack dev-server polling (AD-7) was unnecessary and incompatible.** AD-7 proposed enabling
  webpack `watchOptions.poll` so file changes on the bind-mounted source would be picked up inside
  the container. Next.js 16 uses Turbopack by default, which has no `watchOptions.poll` knob, and
  passing a `webpack()` config with Turbopack active makes `next build` fail outright ("This build
  is using Turbopack, with a `webpack` config and no `turbopack` config"). Removed the config
  entirely and verified hot-reload against the running container by editing
  `frontend/src/app/page.tsx` and confirming the change was served within seconds — Docker
  Desktop's VirtioFS propagates the bind-mounted change without polling, so nothing had to replace
  it.
- **Named volumes for `node_modules`/`.next` caused EACCES, and were unnecessary.** AD-7 called for
  an anonymous volume over `node_modules` so host-built modules wouldn't shadow the container's.
  The actual compose file only bind-mounts `./frontend/src` (not the whole `frontend/` directory),
  so `node_modules` was never at risk of being shadowed in the first place — the volume was
  solving a problem that didn't exist in this layout. Adding named volumes for `node_modules` and
  `.next` on top of that instead broke the container: Docker creates named volumes owned by `root`,
  and the frontend image runs as the non-root `node` user, so `next dev` failed with
  `EACCES: permission denied, mkdir '/app/.next/dev'`. Fix was to drop both named volumes rather
  than chown them.
- **Open risk (not resolved): possible unhandled exception from `aiokafka` `producer.stop()` after
  a timeout-cancelled `producer.start()`.** Flagged by `/code-review` in `backend/app/api/health.py`
  `_check_kafka`: if `asyncio.wait_for` cancels `start()` mid-connection, the `finally: await
  producer.stop()` runs against a partially-initialized client, which could in principle raise
  something outside `_run_check`'s caught exception types (`psycopg.Error`, `KafkaError`,
  `OSError`) and turn `/health/ready` into an unhandled 500 instead of the intended graceful 503 —
  breaking AD-6's "never a stack trace" guarantee. Tried to reproduce it directly: ran the real
  `_check_kafka`/`_run_check` code and the live HTTP endpoint against a blackholed address
  (`10.255.255.1`, TCP SYN gets no response) with `asyncio.wait_for` timeouts from 1ms to 500ms, on
  the pinned `aiokafka==0.12.0`. Every run resolved cleanly to `503 {"kafka": "error: timeout"}` —
  did not reproduce. Left as-is rather than adding a defensive catch-all for a failure mode that
  couldn't be triggered; this is a known-absent-evidence gap, not a confirmed-safe guarantee — worth
  revisiting if `aiokafka` is upgraded or if `/health/ready` is ever seen to 500 in practice.
