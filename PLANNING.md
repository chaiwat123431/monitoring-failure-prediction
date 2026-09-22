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

### Slice 2 — Ingestion (status: DONE)

Scope guard: this slice covers exactly NAB replay → Kafka → TimescaleDB. No feature engineering, no
model, no inference API — those are Slices 3 and 4. Verified against the real NAB corpus (fetched
from `github.com/numenta/NAB`, not guessed from memory) before writing this section.

#### AD-10. Dataset: two `realAWSCloudwatch` CPU-utilization series, not the full corpus

- **Primary — `realAWSCloudwatch/ec2_cpu_utilization_825cc2.csv`.** 4,032 rows, `timestamp,value`,
  5-minute cadence (a few 10-minute gaps — real data, not synthetic-uniform), spanning
  2014-04-10 → 2014-04-24. CPU utilization is the single most canonical "server health" metric, and
  this file has **exactly one** labeled anomaly window
  (`2014-04-15 07:24:00` → `2014-04-16 11:54:00`, per NAB's `combined_windows.json`) — a single,
  isolated window is deliberately the easiest case to hand-verify end to end (the whole point of
  this slice, per the scope guard below).
- **Secondary — `realAWSCloudwatch/rds_cpu_utilization_cc0c53.csv`.** Same shape (4,032 rows, same
  5-minute-ish cadence, `timestamp,value`), same metric family (CPU%) but a different resource type
  (RDS vs EC2) and **two** non-overlapping anomaly windows. This forces the label-matching logic to
  handle more than one window per series before the slice is called done, without adding a second
  code path — the file format is identical to the primary series.
- **Rejected**: (a) *the full NAB corpus* (58 real + 4 artificial files) — the scope guard is
  "replay NAB → Kafka → TimescaleDB", not "ingest all of NAB"; once the pipeline is proven correct
  against 2 known-good files (this slice's actual risk, per the user's own framing below), adding
  more files is a one-line config change, not new code. (b) *`realKnownCause`* — checked concretely:
  `machine_temperature_system_failure.csv` is 22,695 rows over ~79 days (vs. 4,032 rows / 14 days
  for `realAWSCloudwatch`), and `nyc_taxi.csv` is 30-minute cadence and measures taxi passenger
  counts, not a server/IoT metric. The category is narratively appealing (documented root causes)
  but mixes cadences and domains — that's variable-cadence-handling scope this slice doesn't need
  yet. (c) *`artificialWithAnomaly`/`artificialNoAnomaly`* — synthetic sine/step signals, don't
  resemble real infrastructure telemetry. (d) *`realTraffic`/`realTweets`/`realAdExchange`* —
  off-topic domains (road sensors, tweet mentions, ad clicks), not server/IoT.

#### AD-11. TimescaleDB schema: anomaly labels live in a separate table, never a column on the raw hypertable

```sql
CREATE TABLE raw_metrics (
    time      TIMESTAMPTZ      NOT NULL,  -- the row's own NAB timestamp (UTC), never replay wall-clock time
    series_id TEXT             NOT NULL,  -- e.g. 'realAWSCloudwatch/ec2_cpu_utilization_825cc2'
    value     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (series_id, time)
);
SELECT create_hypertable('raw_metrics', by_range('time', INTERVAL '1 day'));

CREATE TABLE nab_anomaly_windows (
    id           SERIAL PRIMARY KEY,
    series_id    TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end   TIMESTAMPTZ NOT NULL,
    UNIQUE (series_id, window_start, window_end)
);
```

- **Partitioning key: `time`**, chunk interval **1 day** — the dataset is tiny (14 days × 2 series),
  so performance doesn't force this, but daily chunks match TimescaleDB's own sizing guidance for
  data at this density and are a sane default now that the tag/chunk decision AD-4 explicitly
  deferred needs to be made. Space-partitioning by `series_id` is deferred — not worth it at 2
  series.
- **Why the label is a separate table, not `raw_metrics.is_anomaly`** (this is the item most worth
  your validation): NAB's ground truth is fundamentally a set of labeled *periods*
  (`window_start`–`window_end`), not a per-point human judgment — a separate table is a faithful
  representation of the source, not a derived encoding. More importantly, it structurally prevents
  the leak the task called out: Slice 3's feature-engineering code will have to *deliberately JOIN*
  `raw_metrics` against `nab_anomaly_windows` (a range-containment query) to get ground truth,
  instead of the label sitting right next to `value` where a careless `SELECT *` into a feature
  matrix would silently include it. The **ingestion pipeline itself (producer + consumer) never
  reads or writes `nab_anomaly_windows` at all** — that table is seeded once, separately, as
  reference data for later evaluation/training, not part of the streaming path.
- **Anomaly windows are seeded as literal data, not parsed from NAB's `combined_windows.json` at
  runtime.** There are exactly 3 windows total across our 2 fixed series — hardcoding them (as a
  small seed/migration insert) avoids depending on NAB's GitHub repo being reachable at ingestion
  time, and avoids building a generic "parse the whole-corpus label file" loader to read 3 known
  values. If the series list grows later, this is the first thing to replace with a real loader.
- **Rejected**: (a) *`is_anomaly BOOLEAN` column on `raw_metrics`* — the leak risk above. (b)
  *materializing per-point labels at write time* — same leak risk, plus it would make the label a
  property of the row instead of the window, losing the distinction between "inside a labeled
  anomaly window" and "NAB provides no judgment here" if that nuance ever matters later.

#### AD-12. Producer: replay pacing and event time are two different clocks, kept structurally separate

- **The Kafka message payload carries the CSV row's own timestamp, parsed as UTC** (NAB timestamps
  carry no timezone; UTC is the standard community convention for this corpus) —
  `{"series_id": "...", "timestamp": "2014-04-15T07:24:00Z", "value": 91.958}`. This value is
  read once from the CSV and never recomputed. **`now()` is used only to control how fast rows are
  published** (a `REPLAY_SPEED` config: e.g. `60` replays 1 simulated hour per 1 real minute,
  `0`/unset fires rows back-to-back as fast as Kafka accepts them — the mode used for the empirical
  verification below, since waiting out 14 real-time days per series isn't practical). These two
  clocks — event time (from the file) and publish pacing (wall clock) — are separate variables in
  the code so they structurally cannot be conflated into "timestamp = time of replay," which is
  exactly the bug the task flagged as silently poisoning Slice 3's training data.
- **Rows are read and published in file order** — verified the 2 chosen CSVs are already strictly
  timestamp-sorted with no duplicate timestamps (checked directly: `Δt ∈ {300s, 600s}` throughout,
  monotonically increasing) — the producer does not sort or reorder; it trusts and preserves NAB's
  own row order.
- **Ordering guarantee**: the topic is created explicitly (per Slice 1's AD-3, which deferred this)
  with one partition per series, and every message is **keyed by `series_id`**. Kafka guarantees
  per-partition ordering, so keying by series routes every row of a given series to the same
  partition — produce order (= file order = chronological order) is preserved through to the
  consumer.
- **Rejected**: (a) *producer-side idempotent-producer mode* — aiokafka's support for this is not
  as complete as e.g. the Java client's; the more meaningful idempotence guarantee for this slice is
  at the consumer/DB boundary (AD-13), so producer-side dedup is redundant. (b) *a single shared
  partition for both series* — would still preserve per-series order via the key's hash routing in
  practice, but an explicit partition-per-series makes the ordering guarantee structural rather than
  incidental.

#### AD-13. Consumer: idempotent upsert + commit-after-write

- **Idempotence on redelivery**: the consumer writes with
  `INSERT INTO raw_metrics (time, series_id, value) VALUES (...) ON CONFLICT (series_id, time) DO
  NOTHING`. A replayed NAB row is immutable data — the same `(series_id, time)` always carries the
  same `value` — so `DO NOTHING` is the correct semantics (`DO UPDATE` was considered and rejected:
  it would silently mask a genuine bug if two different values ever arrived for the same key,
  instead of that being visible as a conflict).
- **Offset commit strategy**: the consumer commits the Kafka offset for a message *only after* the
  DB write for that message has succeeded — never before. This is at-least-once delivery
  (a crash between DB write and offset commit reprocesses that row) combined with the idempotent
  upsert above, which converts "at-least-once delivery" into "exactly-once effect" on the table
  without needing Kafka transactions — overkill for this slice's scope. This is the direct answer to
  "what happens if a message is read twice": it becomes a no-op `ON CONFLICT DO NOTHING`.
- **TimescaleDB unavailable at write time**: the consumer retries the write with bounded exponential
  backoff and does **not** commit the offset until a write succeeds — it blocks consumption rather
  than dropping the message or crashing past its position. Kafka retains the message (topic
  retention covers this small dataset's full replay window many times over), so nothing is lost;
  the consumer simply stalls until TimescaleDB comes back, then catches up from where it left off.
- **Writes are one row at a time** (not batched): throughput here is a few thousand rows total, so
  batching buys nothing, and batching would complicate the idempotence story for no benefit — a
  partially-failed batch still needs per-row retry logic, so row-at-a-time keeps commit granularity,
  DB-write granularity, and idempotence granularity all equal to message granularity.

#### AD-14. Where ingestion runs, and how the schema/topic get created

- **Two new compose services, not in-process FastAPI**: AD-9 (Slice 1) explicitly deferred this
  question to "Slice 2/4" — this is that decision. A `producer` (one-shot job: reads the 2 CSVs,
  replays to Kafka, exits) and a `consumer` (long-running: subscribes, upserts, retries per AD-13).
  Slice 2 has no WebSocket fan-out yet (that's Slice 4/5), so AD-9's stated argument *for*
  in-process (avoiding an extra channel to reach the WebSocket) doesn't apply yet, while its
  argument *against* (coupling ingestion availability to API restarts, which will be frequent
  during Slices 3–5 development) applies immediately. Revisit when the WebSocket exists.
- **Topic creation is explicit** (per AD-3): the producer creates the topic with
  `AIOKafkaAdminClient.create_topics()` before publishing, idempotently (a "topic already exists"
  response is not an error).
- **Schema creation is a plain SQL init script, not Alembic**: two tables, no anticipated schema
  churn yet — Alembic's versioned-migration machinery is overkill at this size (AD-8 in Slice 1
  deferred exactly this choice). Revisit if/when the schema needs to evolve across environments.
- **NAB CSVs are fetched by a small script (`scripts/fetch_nab_data.sh`), not committed**: consistent
  with AD-1's `data/` being gitignored. The script pulls just the 2 chosen files from NAB's public
  GitHub raw URLs into `data/realAWSCloudwatch/`; the producer container itself needs no internet
  access.

**Deferred (need Slice 3+ information):** any use of `nab_anomaly_windows` for training/eval
splits; ingesting further NAB series beyond the 2 above; space-partitioning `raw_metrics`; Alembic
migration; consumer running anywhere other than a single compose service (no horizontal scaling
yet — not needed at this volume).

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

### Slice 2 — Ingestion (done)

The task's own bar for "done" here is not "the service runs" — it's a row-by-row, source-CSV-vs-TimescaleDB
diff:

| Check | Kind | Real deps or mocks? | Why |
|---|---|---|---|
| Row-by-row diff: read a known slice of the source CSV, query the same time range from `raw_metrics`, assert equal `time` and `value` for every row | integration, against real stack | **real** | this is the actual claim the slice has to survive — anything less doesn't rule out the timestamp-mapping bug the task specifically warned about |
| Row counts match exactly between source CSV and `raw_metrics` per series (no dropped/duplicated rows) | integration | **real** | catches both under-delivery (consumer crash) and the idempotence path (over-delivery from a forced consumer restart mid-replay) |
| Anomaly window check: for both series, every timestamp NAB places inside a labeled window matches a row present in `raw_metrics`, and `nab_anomaly_windows` contains exactly the windows from `combined_windows.json` for these 2 files | integration | **real** | validates AD-11's label representation end to end, not just that the table exists |
| Consumer restart mid-replay: kill and restart the consumer partway through, confirm no duplicate rows and no gaps once replay finishes | integration / compose | **real** | direct test of AD-13's idempotence claim, not just code review of the `ON CONFLICT` clause |
| TimescaleDB stopped mid-replay: consumer blocks (doesn't crash, doesn't lose its offset), resumes and catches up once the DB is back | integration / compose | **real** | direct test of AD-13's retry-and-stall claim |
| Producer emits messages keyed by `series_id` with the CSV's own timestamp in the payload (not `now()`) | unit | none — inspect produced messages directly | fast, deterministic check that the two-clocks separation in AD-12 wasn't accidentally collapsed |

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

### Slice 2 — Ingestion

- **Consumer stdout was invisible in `docker compose logs` — Python buffers stdout when it's not a
  TTY.** First empirical pass showed the consumer stuck with no progress logs at all (only
  `aiokafka`'s own logger output, which flushes per-record); actual DB state showed it had in fact
  written all 8,064 rows. `print()` output was sitting in Python's block-buffered stdout inside the
  container, invisible to `docker logs` until the buffer filled or the process exited — for a
  long-running consumer that's effectively "never." Fixed with `ENV PYTHONUNBUFFERED=1` in
  `backend/Dockerfile` (affects every container built from that image: backend, migrate, producer,
  consumer). Reproduced the silent-logs state, applied the fix, rebuilt, and confirmed the
  `consumer: N rows written` lines appear in real time before trusting any other test result that
  depended on reading these logs.
- **AD-13's "retries, then catches up once the DB comes back" was false as first written — the
  consumer never actually recovered.** Caught by literally doing the test AD-13 promised, not by
  reading the code: reset to an empty `raw_metrics`, killed `timescaledb` mid-replay
  (`docker compose stop timescaledb`) with the consumer partway through, watched it correctly enter
  the retry-backoff loop without crashing — then brought `timescaledb` back and watched the consumer
  stay stuck retrying with `the connection is closed` for 20+ seconds after the DB was healthy
  again, permanently. Root cause: `write_with_retry` retried `conn.execute(...)` on the *same*
  `psycopg.AsyncConnection` object every time; once a connection is terminated it stays closed
  forever, so retrying a write on it can never succeed no matter how long you wait or how healthy
  the DB is. Fixed by replacing the bare connection with a small `ReconnectingConn` wrapper
  (`backend/app/ingestion/consumer.py`) that discards the dead connection on any write failure and
  opens a fresh one on the next attempt. Re-ran the identical kill/restart sequence after the fix:
  consumer resumed on its own and reached the full 8,064/8,064 rows with zero duplicates. This is
  exactly the kind of bug "the service is up" would never have caught — the container stayed
  `Up` and un-crashed the entire time, both before and after the fix; only forcing the actual DB
  outage and watching for real recovery (not just no-crash) exposed it.
