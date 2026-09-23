# PLANNING.md — AI Monitoring & Failure Prediction Platform

## 1. Project Goal

Portfolio project (solo, not production-grade) demonstrating a streaming data pipeline with
ML-based anomaly/failure detection, built to match the skills typical ML/data-engineering and
AI-automation job postings list explicitly: Kafka, scikit-learn, real-time pipelines, an
MLOps-adjacent workflow, and containerized deployment.

Not a deadline-blocking project — another portfolio project already covers general
application-readiness needs. Priority here is doing the new stack (Kafka, TimescaleDB, WebSocket)
correctly over doing it fast.

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

### Slice 3 — Model (status: DONE)

Scope guard: feature engineering + training + evaluation of one Isolation Forest model. No FastAPI
endpoint, no WebSocket, no dashboard — those are Slices 4 and 5. Numbers below are measured
directly against the real ingested data (`SELECT min(time), max(time), count(*) ... GROUP BY
series_id` and `SELECT * FROM nab_anomaly_windows`), not estimated.

#### AD-15. Feature engineering: one 60-minute trailing window, 4 features, computed per series

For each row (`series_id`, `time`, `value`), computed over the trailing window **within that
series only** (rolling state never crosses a `series_id` boundary):

| Feature | Definition | Why |
|---|---|---|
| `value` | the raw reading itself | the window-smoothed features below can hide a sharp deviation *at* the current instant — this keeps the instant itself visible to the model |
| `rolling_mean_1h` | mean of `value` over the trailing 60 minutes | characterizes the recent baseline level |
| `rolling_std_1h` | sample std (`ddof=1`) of `value` over the trailing 60 minutes | characterizes recent dispersion/volatility — a level shift *and* a volatility shift are both meaningful CPU-utilization anomaly signatures |
| `rate_of_change` | `value[t] − value[t−1]` (previous sample in the same series) — deliberately **not** windowed | captures short-term momentum; averaging this over the same 1h window as the others would smooth out exactly the fast movement this feature exists to detect |

- **Window is time-based (a real 60-minute wall-clock span via pandas' offset-based
  `.rolling('60min')`), not row-count-based (`.rolling(12)`).** The ingested data isn't perfectly
  uniform — checked directly: both series have occasional 10-minute gaps alongside the mostly
  5-minute cadence (Slice 2 AD-12's own verification). A row-count window would silently span more
  than an hour whenever a gap falls inside it; a time-based window doesn't have that failure mode
  and costs no real extra complexity in pandas.
- **Why 60 minutes**: both series' 5 labeled-window-days aside, every actual anomaly window is
  16–28 hours long (the shortest, `rds` window 2, is 16h40m; the longest, `ec2`, is 28h30m). A
  60-minute feature window reaches full contamination (i.e. every point in the window is inside
  the anomaly) within roughly 4–6% of the anomaly's own duration — fast relative to how long the
  anomaly actually lasts, without being so short that `rolling_std_1h` is dominated by 1–2 point
  noise.
- **Rejected**: (a) *15 minutes* — only 3 samples at nominal cadence; `rolling_std` over 3 points
  is itself noisy, defeating the point of smoothing. (b) *6h or 24h* — for most of a 16–28h
  anomaly's duration, a 6h+ window would contain a mix of anomalous and normal points, diluting
  the very deviation the feature is meant to surface, right when it should be sharpest. (c) *also
  windowing `rate_of_change`* — would make it redundant with `rolling_mean`'s trend rather than a
  distinct momentum signal.
- **Rows without a full hour of preceding history are dropped, not backfilled** (the first ~12
  rows of each series). This is ≈0.3% of each series and, checked directly, falls entirely within
  the pre-anomaly training portion for both series (first anomaly is 5+ days into `ec2`, 10+ days
  into `rds`) — no evaluation data is lost to this.
- Lives in `backend/app/ml/features.py` — the path Slice 1's AD-9 reserved specifically so this is
  the *same code* Slice 4's live inference imports later, not a re-implementation that can drift.

#### AD-16. Train/test split: temporal, cut per-series at that series' first labeled anomaly

| Series | Train | Test | Train rows (post window-drop) |
|---|---|---|---|
| `ec2_cpu_utilization_825cc2` | `[2014-04-10 00:04, 2014-04-15 07:24)` | `[2014-04-15 07:24, 2014-04-24 00:09]` | 1,526 / 4,032 = **37.9%** |
| `rds_cpu_utilization_cc0c53` | `[2014-02-14 14:30, 2014-02-24 22:50)` | `[2014-02-24 22:50, 2014-02-28 14:30]` | 2,980 / 4,032 = **73.9%** |

(Row counts measured directly against `raw_metrics`, before the window-drop in AD-15 removes ~12
rows per series from the front of train.)

- **Cutoff = the start of that series' first labeled anomaly window** — never a shared global
  fraction. Verified directly why a shared fraction would be wrong: `ec2`'s anomaly
  (07:24 on day 5 → 11:54 on day 6, i.e. 37.9%–46.4% of the series) would fall entirely *inside* a
  naive 60% (or even 50%) global split — training the "what does normal look like" baseline on the
  one pattern the model most needs to flag as abnormal. The two series' anomalies are unrelated
  events on unrelated calendar dates; only a per-series cutoff, placed by where each series'
  *actual* anomaly falls, avoids this.
- **Cutoff is placed at, not before, the anomaly start** — this maximizes real training data while
  still guaranteeing zero anomaly contamination: the constraint is "no training row's own
  timestamp is inside a labeled window," and a trailing (backward-looking) feature window can
  never pull anomalous data into a training row whose own timestamp already precedes the anomaly —
  so no extra safety buffer before the cutoff is needed.
- **The resulting train fractions are deliberately different per series (37.9% vs. 73.9%) — this
  is correct, not an inconsistency.** It's mechanically forced by where each series' real anomaly
  happens to sit in its own 14-day window; forcing a uniform ratio would mean picking a wrong
  number for one series to keep the other tidy.
- `rds`'s test period contains **both** of its labeled windows (window 2 starts 2014-02-26 16:30,
  after the cutoff) — confirmed directly from `nab_anomaly_windows`, not assumed.
- **Rejected**: a fixed 80/20 (or any uniform) split ignoring where the labeled anomalies fall —
  demonstrated above to contaminate `ec2`'s training set.

#### AD-17. Evaluation: `nab_anomaly_windows` is joined only after `.fit()`, never before

- **`IsolationForest.fit(X_train)` takes features only — scikit-learn's unsupervised API has no `y`
  parameter to accept a label in the first place.** This is a stronger guarantee than "we chose
  not to pass the label": there is no code path through which `nab_anomaly_windows` could leak into
  training even by mistake, structurally continuing Slice 2 AD-11's separate-table design.
  `nab_anomaly_windows` is touched exactly once in the whole pipeline — after training, to score
  predictions.
- **Ground truth for evaluation**: for each test-set row, `y_true = 1` if
  `row.time BETWEEN window_start AND window_end` for that `series_id` in `nab_anomaly_windows`,
  else `0` — computed with a join/range check, entirely after `.fit()` has already happened.
- **Metrics are standard point-wise precision/recall/F1** (`sklearn.metrics`), comparing `y_true`
  (from the join above) against `y_pred = 1 if IsolationForest.predict(x) == -1 else 0` for every
  test row. **Rejected**: NAB's own official windowed/weighted scoring profiles (standard /
  reward-low-FP / reward-low-FN) — that's a separate scoring framework built for crediting early
  detection in a live streaming benchmark; the task asks for precision/recall/F1 directly, and
  building NAB's scorer is out of this slice's scope.
- **Both per-series and combined (both test sets concatenated) metrics are computed and kept** —
  the real measured numbers, not estimated, land in the model artifact's metadata (AD-19) once
  training actually runs.
- **Statistical caveat, stated plainly**: these precision/recall/F1 numbers are computed over
  exactly **3 labeled anomaly windows total** (1 for `ec2`, 2 for `rds`) — the entire ground truth
  this slice has to evaluate against. That's genuinely useful signal for this portfolio/demo
  project, but it is not a statistically significant validation at the scale a real production
  system would require; a handful of windows means a single early/late/missed detection swings the
  metrics substantially. Not a reason to skip measuring them — just a reason not to over-read them.

#### AD-18. One unified model across both series, not one model per series

- A single `IsolationForest` is fit on the concatenated training rows from both series (1,526 +
  2,980 = 4,506 rows). `series_id` itself is **not** a feature — only 2 distinct values exist,
  wouldn't generalize to a series Slice 4 sees later, and Isolation Forest partitions on numeric
  geometry, not categorical identity.
- **Rejected: one model per series** — (a) far less training data per model (1,526 rows alone for
  `ec2`), (b) doesn't generalize if Slice 4 ever scores a series neither model was trained on, (c)
  both series measure the literal same physical quantity (CPU utilization, 0–100%) on the same
  scale, so there's no unit-mismatch forcing separation the way e.g. mixing CPU% with raw network
  bytes would.
- **Hyperparameters**: `n_estimators=100` (sklearn default — no tuning data yet to justify
  deviating), `contamination="auto"` (training data is anomaly-free by construction, so there's no
  principled empirical contamination rate to supply; `"auto"` uses the original Isolation Forest
  paper's score-offset formula instead of a guessed fraction — guessing one tuned toward the test
  set's real anomaly rate would itself be a subtle form of leakage), `random_state=42` (fixed, so
  the metrics embedded in the artifact are reproducible run to run).

#### AD-19. Serialization: one joblib artifact, metadata embedded (not a sidecar file)

- `backend/scripts/train.py` (the path Slice 1's AD-9 reserved) pulls `raw_metrics` +
  `nab_anomaly_windows` from TimescaleDB, calls `app.ml.features` (AD-15), splits (AD-16), fits
  (AD-18), evaluates (AD-17), and saves to `models/isolation_forest.joblib`.
- **`models/` is a new top-level, gitignored directory** — mirrors AD-1's `data/`: an artifact
  regenerable from code + data doesn't belong in git.
- **One joblib file holding `{"model": ..., "metadata": {...}}`, not a model file plus a separate
  metadata sidecar** — a sidecar can drift out of sync with the model it describes; a single
  artifact can't.
- **Metadata embedded**: `feature_version` (a string tied to AD-15's logic, so Slice 4 can assert
  the model it loads matches the feature code it's running before trusting it), `window_minutes`,
  `feature_names` (exact ordered column list — sklearn is order-sensitive at predict time),
  `trained_at` (UTC ISO8601), `series_used`, `split_cutoffs` (the AD-16 timestamps, per series),
  `model_params` (`n_estimators`, `contamination`, `random_state`), `sklearn_version`, and
  `metrics` (AD-17's real measured precision/recall/F1, per-series and combined) — so anyone
  loading the artifact later (Slice 4) sees what to expect without re-running evaluation.

**Deferred (need Slice 4+ information):** how Slice 4's API loads/reloads this artifact; model
retraining/versioning policy beyond a single `isolation_forest.joblib`; any feature beyond the 4 in
AD-15 (e.g. day-of-week/hour-of-day seasonality) — not justified without first seeing whether the
4 measured metrics need it.

### Slice 4 — API (status: DONE)

Scope guard: this slice exposes the existing pipeline over HTTP — REST (history, model metadata) +
one WebSocket broadcasting live scored metrics. No frontend/dashboard (Slice 5) and no change to
the ingestion `producer`/`consumer` services (Slice 2, DONE) — they keep writing raw rows to
TimescaleDB exactly as before, untouched by this slice.

#### AD-20. REST endpoints: 3 routes, all under `/api`

| Route | Purpose |
|---|---|
| `GET /api/series` | List the configured series (`series_id` only, from `app.ingestion.series_registry.SERIES` — the same registry the producer/train.py already use, not a second hardcoded list). Lets a client discover what exists without hardcoding series IDs. |
| `GET /api/series/{series_id}/history?start=<iso8601>&end=<iso8601>` | Raw `raw_metrics` rows for that series in `[start, end]`, each with `time`, `value`, and `is_anomaly` (computed the same way as live scoring, AD-21 — never a stored column, consistent with AD-11's "labels are never merged onto the raw row" principle applying equally to model output). Response envelope also carries `model_loaded: bool` (top-level, once) and a `labeled_windows: [...]` array — the `nab_anomaly_windows` rows overlapping `[start, end]` for this series, via the same range-join `train.py` already does — so a chart can distinguish "NAB ground truth" from "model prediction" without a second endpoint. `start`/`end` are both **required**, no default range: the dataset is 2 fixed 14-day series (AD-10), so there's no "give me everything" use case worth guessing a default for, and an explicit range keeps the query trivially boundable without needing a row-limit/pagination scheme this project's data volume doesn't justify. |
| `GET /api/model` | The loaded model's metadata dict, verbatim (`feature_version`, `window_minutes`, `feature_names`, `trained_at`, `series_used`, `split_cutoffs`, `model_params`, `sklearn_version`, `metrics` — exactly AD-19's schema, not a re-shaped subset). `200` if a model is loaded, `503 {"status": "not_trained", "detail": "..."}` if not (AD-24). |

- **Why this 3-way split and not one combined "everything" endpoint**: `/series` is discovery,
  `/series/{id}/history` is the (potentially large-ish, range-bound) time-series payload, `/model`
  is a small static-ish blob unrelated to any specific range — three different cache/consumption
  patterns for a future frontend, so they're three routes rather than one endpoint with optional
  query params silently changing its response shape.
- **Rejected**: (a) *per-series model metadata endpoint* (`/api/series/{id}/model`) — AD-18 already
  decided one unified model, not one per series; a per-series URL would imply a distinction that
  doesn't exist. (b) *embedding `labeled_windows` as a per-row column on history* — exactly the leak
  vector AD-11 designed the separate table to avoid; keeping it a separate array in the response
  preserves that same discipline at the API boundary.

#### AD-21. Inference integration: `app.ml.features.compute_features` is imported, never re-implemented, against a model loaded once at startup

- **The model is loaded exactly once**, in `main.py`'s `lifespan`, via a small
  `app.ml.model_store.load_model(path) -> tuple[IsolationForest | None, dict | None]` (isolates the
  `joblib.load` + missing-file handling in one place, reused by both the app and by tests). The
  result is stored on `app.state.model` / `app.state.model_metadata` — plain attribute reads from
  request handlers and the live-feed task (AD-22), not a per-request reload. `IsolationForest` is
  stateless at predict time and the artifact never changes while the process runs (no hot-reload
  endpoint in this slice's scope — retraining/versioning was already deferred in AD-19), so a single
  shared in-memory reference is correct and requires no locking: FastAPI's default event loop is
  single-threaded, and nothing ever mutates `app.state.model` after startup.
- **Both the REST history endpoint (AD-20) and the WebSocket live path (AD-22) call the *same*
  `compute_features(df) -> DataFrame` from `app/ml/features.py`** — confirmed by construction: there
  is exactly one feature-computation function in the codebase (Slice 3's AD-15 reserved this file
  precisely so Slice 4 would import it, not rewrite it), and both call sites pass it a small
  `(series_id, time, value)` DataFrame and read `FEATURE_NAMES` off the result before calling
  `model.predict(...)`. No inference code anywhere reimplements rolling mean/std/rate-of-change.
- **Rejected**: *loading the model inside each request handler* — reopening/deserializing the
  joblib file per request is pure waste for an artifact that never changes at runtime, and would
  make "is the model loaded" a per-request race instead of a single startup fact.
- **`model.predict()`/`decision_function()` are synchronous, CPU-bound scikit-learn calls — measured
  directly against the real `models/isolation_forest.joblib` (100 estimators) before deciding
  anything**: **~2.2 ms** for a single feature row (the shape the live-feed task calls with, AD-22)
  and **~8.4 ms** for a 4,032-row batch (a full series, the shape `/api/series/{id}/history` calls
  with, AD-20) — averaged over hundreds of warm runs. Both are non-negligible on a single-threaded
  `asyncio` event loop: calling either directly would block that loop for the duration, delaying
  every other connected WebSocket and even `/health` for that window. Under `REPLAY_SPEED=0` (the
  mode already used for empirical verification, per AD-12/Slice 2) messages arrive back-to-back, so
  the live-feed task would otherwise issue single-row `predict()` calls in a tight sequence with no
  room for the loop to service anything else in between. **Both call sites therefore run the
  scikit-learn call via `asyncio.to_thread(model.predict, X)`** (a plain daemon thread pool, no new
  dependency — `asyncio.to_thread` is stdlib), so the event loop stays free to accept/serve other
  connections while inference runs. This adds a small `run_in_executor` scheduling overhead per call,
  which is accepted: it is orders of magnitude cheaper than the blocking it avoids.
- **Rejected**: (a) *calling `model.predict()` directly on the event loop* — the measurement above is
  the direct reason not to; single-threaded correctness (no lock needed, per the point above) doesn't
  mean single-threaded *cheap*. (b) *a separate process/worker pool for inference* — the measured
  costs (single-digit milliseconds) don't justify inter-process serialization overhead; a thread is
  enough since the GIL is released during scikit-learn's native (Cython/numpy) inner loop.

#### AD-22: Live path: the API runs its own second, ephemeral Kafka consumer — it does not go through the ingestion `consumer` service

- **A background `asyncio` task, started in `lifespan`, subscribes directly to the `raw-metrics`
  topic** (the same topic Slice 2's producer/consumer already use) — it is a **second, independent
  reader of the same topic**, not a relay chained after the ingestion `consumer` service. The two
  have entirely different jobs and failure domains: the ingestion `consumer` (AD-13/AD-14, unchanged)
  durably upserts every row into TimescaleDB with commit-after-write and infinite retry; this new
  live-feed task only exists to push already-durable data to whoever's watching right now, so it can
  be lossy/best-effort without any risk to the source of truth.
- **The live-feed task uses no consumer group and no committed offsets** (`assign()` to the topic's
  partitions directly, `auto_offset_reset` effectively "latest"/tip-of-topic at task start, never
  `commit()`). Justification: this consumer's state (the rolling per-series buffers, below) is
  rebuilt from TimescaleDB on every startup anyway, so replaying old Kafka messages after a restart
  would only mean re-broadcasting stale data as if it were live — actively wrong for a "live" feed.
  Using a real consumer group here would also risk rebalancing against the *ingestion* consumer's
  group if the group ID were ever reused by mistake; a groupless assign structurally can't collide.
- **Per-series rolling buffer, seeded from TimescaleDB at startup**: `compute_features` needs ~60
  minutes of trailing history (AD-15) to produce a non-degraded feature row for the *first* live
  point — without seeding, the first several live points after any API restart would silently score
  against an incomplete window. So at startup, for each `series_id` in the registry, the task queries
  `raw_metrics` for the last `WINDOW_MINUTES` (imported from `app.ml.features`, not a second hardcoded
  60) and keeps that as an in-memory deque; each new Kafka message appends to its series' deque and
  evicts entries older than the window. Each new point is scored by calling `compute_features` on
  that small buffer and reading the **last** row's features — the same function, called on a bounded
  slice, not a hand-rolled incremental version of the rolling stats.
- **Fan-out**: a small in-memory `ConnectionManager` (`app/live/broadcaster.py`) keyed by
  `series_id -> set[WebSocket]`. The live-feed task, after scoring a point, looks up that series'
  connected sockets and sends `{"series_id", "timestamp", "value", "is_anomaly", "model_loaded"}` to
  each; a send failure removes that socket (treated the same as an explicit disconnect, AD-23).
  This is the "internal channel" AD-9 (Slice 1) flagged as an open question (Postgres
  `LISTEN/NOTIFY` vs. Redis vs. something else) — resolved here as: **no new channel at all**, Kafka
  itself is the internal channel, since both the durable path and the live path already read from it
  natively.
- **Rejected**: (a) *the live-feed task consuming from the ingestion `consumer` service somehow
  (e.g. it calls into the API)* — would couple two independently-scoped compose services and add a
  network hop for no benefit, since both can read the same Kafka topic directly. (b) *reading
  `raw_metrics` via `LISTEN/NOTIFY` triggers instead of Kafka* — would require adding a trigger to a
  table Slice 2 deliberately kept simple (AD-14 rejected Alembic/schema churn), and duplicates
  information already flowing through Kafka. (c) *polling TimescaleDB from the API on an interval*
  — adds latency proportional to the poll period and DB load proportional to connected clients for a
  feed that Kafka already pushes.

#### AD-23. WebSocket: `GET /ws/series/{series_id}/live`, per-series, no automatic history backfill on connect

- **One endpoint per series** (`series_id` in the path), not one global multiplexed socket carrying
  both series — mirrors AD-20's per-series history route, and means the server never has to filter
  a shared stream on the client's behalf; a client that wants both series opens two connections
  (trivial at this project's 2-series scale).
- **Connect**: validate `series_id` against the registry (`404` on close code / rejection if
  unknown, mirroring the REST 404 a client would get from an unknown series on `/history`),
  `accept()`, register the socket in `ConnectionManager` under that `series_id`.
- **Disconnect/reconnect**: on `WebSocketDisconnect` (client closes) or any send exception (broken
  pipe, timeout), the handler removes the socket from `ConnectionManager` and exits cleanly — no
  process-wide state depends on any single connection. A client that reconnects (browser tab
  refresh, network blip) is just a new `connect()`; nothing server-side needs to recognize it as
  "the same" client, since the server holds no per-client session state (single-user MVP, PLANNING
  §2).
- **A newly-connected client does *not* receive a replay of recent history over the socket** — it
  only receives points scored *after* it connected. **Why**: the REST history endpoint (AD-20)
  already answers "what happened before now" for an arbitrary range chosen by the client; teaching
  the WebSocket handler to *also* do a bounded backfill would duplicate that query logic in a second
  place and hardcode a "how much" decision (last N minutes? last N points?) that belongs to whoever
  is building the chart (Slice 5), not to this transport layer. The documented contract for Slice 5
  is: call `/api/series/{id}/history` for the initial chart range, then open the WebSocket for the
  live tail — one read path per concern, and it costs the frontend exactly one extra `fetch()` on
  mount.
- **Rejected**: *auto-backfill last N minutes on WS connect* — duplicates AD-20's query, and bakes a
  frontend-shaped decision (how much history a chart wants) into the transport layer instead of
  leaving it to the caller who actually knows.

#### AD-24. Robustness: `models/isolation_forest.joblib` missing at API startup is a handled state, not a crash

- **`app.ml.model_store.load_model()` catches `FileNotFoundError` explicitly** (the file legitimately
  doesn't exist yet if `scripts/train.py` hasn't been run — a normal state for a fresh checkout, not
  a corrupt-artifact error, which would instead surface as an unhandled `joblib`/pickle exception and
  is *not* swallowed the same way). On a missing file: `lifespan` logs one clear line
  (`"api: no model found at <path>, starting without inference — run scripts/train.py"`), sets
  `app.state.model = None` / `app.state.model_metadata = None`, and startup continues — `backend`
  must still reach the compose healthcheck (`GET /health`, which per AD-6 does no I/O and can't be
  affected by this anyway) and `/health/ready` (checks only DB/Kafka, unaffected by model state).
- **Every consumer of the model handles `None` explicitly, never by exception**:
  - `GET /api/model` → `503 {"status": "not_trained", "detail": "run scripts/train.py to produce models/isolation_forest.joblib"}`.
  - `GET /api/series/{id}/history` → still returns raw values + `labeled_windows` (neither needs the
    model); response envelope sets `"model_loaded": false` and every row's `is_anomaly` is `null`
    (never silently `false` — `false` would mean "the model checked and said normal", `null` means
    "no model was available to check," a real distinction a chart should render differently, e.g.
    greyed out vs. green).
  - The AD-22 live-feed task still runs and still broadcasts `{series_id, timestamp, value,
    "is_anomaly": null, "model_loaded": false}` — the live metric feed itself doesn't depend on
    scoring being available, only the anomaly flag does.
- **This must be demonstrated, not just coded**: verification (below) includes starting the stack
  with `models/isolation_forest.joblib` renamed out of the way and confirming `backend` still
  reaches healthy, `/api/model` returns `503` cleanly, and the WebSocket still streams
  `is_anomaly: null` points — then restoring the file and confirming a fresh `backend` restart picks
  it up and starts scoring.
- **Rejected**: (a) *raising at startup / failing the healthcheck if the model is missing* — would
  make `docker compose up` order-dependent on `train.py` having been run first, which is exactly the
  kind of foot-gun AD-6 already reasoned about for DB/Kafka; a missing *model* is even more clearly
  not a liveness problem. (b) *falling back to some naive scoring rule when the model is absent* —
  would silently produce fabricated anomaly flags a viewer can't distinguish from real model output;
  `null` is honest, a fake rule isn't.

**Empirical verification (done, per the task — real stack, not asserted from code review):** with a
freshly reset compose stack (`docker compose down -v` then back up, so the topic and TimescaleDB
started genuinely empty), a real `websockets` client connected to
`/ws/series/realAWSCloudwatch/ec2_cpu_utilization_825cc2/live`, then a real producer replay was
triggered. All 1,434 scored live messages received matched `model.predict()` run independently on
the same feature rows — **0 mismatches**. The AD-24 missing-model demonstration was run for real
(renamed `models/isolation_forest.joblib` out of the way, restarted `backend`, confirmed `/health`
stayed 200, `/api/model` returned 503 `not_trained`, `/history` returned `is_anomaly: null` +
`model_loaded: false`, and the WebSocket kept streaming `is_anomaly: null` points; then restored the
file and confirmed a restart picked it back up and resumed scoring). `/api/series/{id}/history` was
checked row-for-row against `raw_metrics` and `nab_anomaly_windows` (`tests/integration/test_api.py`,
mirroring Slice 2's own verification bar in its testing table). This first pass also caught the two
real bugs recorded in §7 (the groupless-consumer zero-partition race, and the `:path` routing fix
for `series_id` values containing `/`).

**Deferred (need Slice 5+ information):** any pagination/downsampling of `/history` for a longer
future dataset; multi-client scaling of the live-feed task (currently one groupless consumer per API
process — fine for single-user MVP, PLANNING §2; would need a real consumer group + fan-out design
if the API were ever run with >1 replica); a model hot-reload endpoint (retraining policy already
deferred in AD-19).

### Slice 5 — Frontend (status: DONE)

Scope guard: this slice covers only the Next.js/Recharts dashboard consuming the existing API
(Slice 4, frozen). No change to `backend/`, `docker-compose.yml`'s non-frontend services, ingestion,
or the model. All endpoint/field names below are read directly from `app/api/metrics.py` and
`app/api/ws.py`, not assumed from PLANNING's own AD-20/22/23 prose.

#### AD-25. Component structure: one dashboard page, a data-fetching hook holding all state, dumb presentational components

```
frontend/src/
├── app/
│   ├── layout.tsx            # existing, untouched apart from <title>/metadata
│   └── page.tsx              # composes the dashboard; owns the selected series_id
├── components/
│   ├── SeriesSelector.tsx    # <select> populated from GET /api/series
│   ├── ModelStatusBanner.tsx # renders only when model_loaded === false
│   ├── ConnectionBadge.tsx   # renders the WS connection state
│   ├── MetricChart.tsx       # Recharts composed chart (line + scatter overlay + window bands)
│   └── AlertsPanel.tsx       # list of is_anomaly === true points, most recent first
├── hooks/
│   └── useLiveSeries.ts      # AD-26/AD-27: history fetch + WS + reconnect + gap-fill, one hook
└── lib/
    ├── api.ts                # typed fetch wrappers: getSeries(), getHistory(id, range)
    ├── types.ts              # TS types mirroring the API's actual JSON shapes verbatim
    └── config.ts             # NEXT_PUBLIC_API_URL (already wired, docker-compose.yml AD-7) → http(s)
                               # base + derived ws(s) base for the live socket
```

- `page.tsx` holds exactly one piece of state, `selectedSeriesId`, and fetches `GET /api/series`
  once on mount to populate `SeriesSelector`; everything else for the *selected* series (history,
  live points, connection state, model status) lives inside `useLiveSeries(selectedSeriesId)`, so
  switching series is "unmount the old hook instance, mount a new one" rather than a pile of
  `if (seriesId changed) reset(...)` logic spread across components.
- Layout: a header row (`SeriesSelector` + `ConnectionBadge`, both small and always visible),
  `ModelStatusBanner` directly under it (full-width, only rendered when relevant), then a two-column
  body — `MetricChart` (primary, wide) and `AlertsPanel` (narrow side column) — collapsing to
  stacked on narrow viewports. This is a portfolio dashboard, not a design-system deliverable, so no
  extra chrome (tabs, settings, theming) beyond what the 5 requirements below need.
- **Rejected**: (a) *a state-management library (Redux/Zustand/Context-as-store)* — one hook, one
  page, two consumer components; React's own state is enough at this scope and a library would be
  unused-abstraction weight. (b) *one "smart" `MetricChart` that fetches its own data* — keeping data
  fetching in a hook and charts/panels presentational makes `AlertsPanel` and `MetricChart` trivially
  testable/storyboardable against fixed prop data, and keeps exactly one code path that talks to the
  network.

#### AD-26. Data lifecycle: `/history` fetched with a deliberately wide, fixed bracket — not the dataset's real date range

- On mount (and on every `selectedSeriesId` change), `useLiveSeries` calls
  `GET /api/series/{id}/history?start=<HISTORY_RANGE_START>&end=<HISTORY_RANGE_END>` **exactly
  once**, seeds chart state from its `history` array, then opens the WebSocket for the live tail —
  never a poll loop calling `/history` on an interval. This is the literal contract AD-23 documents
  ("call `/history` for the initial range, then open the WebSocket for the tail").
- **The two constants are a fixed, wide bracket** (`HISTORY_RANGE_START = 2000-01-01T00:00:00Z`,
  `HISTORY_RANGE_END = 2100-01-01T00:00:00Z`), not the two NAB series' real 2014 date ranges from
  AD-16. **Why this is the item most worth your validation**: AD-20 made `start`/`end` both
  required with no server-side default specifically because "there's no give-me-everything use case
  worth guessing a default for" at this dataset's *size* — but that's a statement about row count
  (4,032 rows/series, trivial), not about the frontend needing to know the *real* boundary dates.
  The only way to learn the real range without guessing would be a new field on `GET /api/series`
  (e.g. `first_time`/`last_time`) — a backend change this slice's scope guard forbids. A wide,
  fixed bracket gets the same practical result (every row currently in `raw_metrics` for that
  series, regardless of when ingestion happens to have reached) without embedding this slice's
  frontend code with knowledge of a specific dataset's specific calendar dates, and without
  depending on client wall-clock time (which bears no relationship to the events' 2014 timestamps,
  since AD-12 pointed out event time and replay-pacing wall time are unrelated clocks).
- **Rejected**: (a) *hardcoding each series' real min/max timestamp from AD-16* — works today, but
  ties the frontend to values that live in this document, not in an API response; a future series
  added to `SERIES` (AD-10) would silently render an empty chart until someone remembered to update
  a second hardcoded table. (b) *`end = new Date()` (client "now")* — technically works (any bound
  at or after the true max returns the same rows) but reads as if it means something ("up to the
  present moment") when the data's own event time is nowhere near the present; a clearly-synthetic
  far-future constant doesn't invite that misreading. (c) *no `end` bound / omit the param* — AD-20
  requires both params; not an option without a backend change.

#### AD-27. WebSocket client: exponential-backoff reconnect, and a re-`/history` gap-fill on every reconnect — confirmed against AD-23, not assumed

- **Not polling**: after the one `/history` call in AD-26, the only ongoing data source is
  `new WebSocket(\`${WS_BASE}/ws/series/${id}/live\`)`; no `setInterval` anywhere re-fetches
  `/history` on a timer.
- **Reconnection**: on `onclose` or `onerror`, `useLiveSeries` retries with exponential backoff
  (250ms → 500ms → 1s → … capped at 8s, uncapped attempt count — a single-user dev/demo dashboard
  left open has no reason to ever give up) and exposes a `connectionState` of
  `"connecting" | "open" | "reconnecting" | "closed"` (`"closed"` only after an explicit unmount or
  a server close with code 1008 for an unknown `series_id`, which shouldn't happen from a value the
  dashboard itself populated from `/api/series`) — `ConnectionBadge` renders this directly, so a
  disconnect is always visible, never silent.
- **Gap-fill on reconnect, confirmed as the right read of AD-23, not a guess**: AD-23 states the
  socket never replays missed history and that `/history` is the caller's tool for "what happened
  before now" over *any* range the caller chooses — it does not say the caller may only use that
  tool once at mount. So on every successful reconnect (not the first connect), `useLiveSeries`
  calls `GET /api/series/{id}/history?start=<timestamp of the last point currently held>&end=<the
  same far-future constant from AD-26>`, merges the returned `history` array into existing state
  keyed by `time` (a plain `Map` dedupes overlap — the boundary point at `start` is fetched again on
  purpose rather than tracked as an exclusive bound, which is simpler and costs one duplicate row),
  and only then resumes appending live WS messages. **The alternative — accept the gap and let the
  chart show a visible hole** — was rejected: a `raw_metrics` row's `time` is the point's real event
  time, so a reconnect gap here means an actual missing measurement or missing anomaly flag on the
  chart, and this project already has the exact tool (`/history`) built to answer "what happened in
  this range" — not using it would be answering the assignment's own question ("que fait le
  frontend pendant la reconnexion ?") with a shrug when a correct answer is directly available.
  **Cost, stated plainly**: on a long/flappy disconnect this refetches a small range every time,
  and if the socket is down for the whole `HISTORY_RANGE` bracket at connect time, this degrades to
  exactly AD-26's mount-time fetch, which is fine — the dataset is a few thousand rows total.
- **`WebSocketDisconnect`/close vs. genuine network drop are not distinguished client-side** (the
  browser `WebSocket` API doesn't expose that distinction on `onclose`), so both go through the same
  reconnect-and-gap-fill path — a deliberate simplification, matching the backend's own
  "reconnect is just a new `connect()`, no session state" stance (AD-23).
- **Rejected**: (a) *no reconnect (page-refresh-to-recover)* — a single dropped WS shouldn't require
  the user to reload; the backend already treats reconnects as free (AD-23). (b) *reconnect without
  gap-fill* — addressed above. (c) *a fixed reconnect delay instead of backoff* — a fixed short
  delay hammers the backend during a real outage (e.g. `backend` restarting after a code change
  during this slice's own development); backoff is one `setTimeout` more complex and avoids that.

#### AD-28. Visual encoding: `is_anomaly` is a 3-state field, and NAB ground truth is drawn differently from model predictions

- **Chart** (`MetricChart`, Recharts `ComposedChart`): the raw `value` series is one continuous
  `Line` in a neutral color, always drawn regardless of anomaly/model state — the line must never
  visually disappear just because the model is absent (AD-29). On top of it, a `Scatter` layer plots
  every point with a marker whose color/shape is a function of `is_anomaly`:
  - `false` → small filled circle, muted/neutral color (a point the model checked and called normal
    — visually recedes, it's the expected case).
  - `true` → filled circle, alert color (red/orange), slightly larger — a real detection, meant to
    draw the eye.
  - `null` → a distinct outline-only / hollow marker in a grey tone — **explicitly not styled as
    either "normal" or "absent-from-chart"**, so a viewer can tell "no model was available for this
    point" apart from "the model checked and it was fine," per AD-24's own point that conflating
    those would be dishonest.
  - A small legend under the chart names all three states explicitly (this is the one place a
    generic 2-state anomaly chart would silently collapse `null` into `false`, which is exactly the
    mistake being avoided here).
  - **NAB's `labeled_windows` (AD-20) are rendered separately from the point markers above** — as
    shaded vertical bands (Recharts `ReferenceArea`) behind the line, one per `{window_start,
    window_end}` — never as extra scatter points and never in the same visual language as
    `is_anomaly`. This preserves, at the chart itself, the same distinction AD-11/AD-20 built into
    the data model: "NAB says this period was anomalous" (ground truth, a period) is a different
    kind of claim than "the model flagged this exact point" (a prediction, a point), and the Slice 3
    metrics (AD-17, precision ~0.18–0.32 combined) mean the two will visibly *disagree* on real
    data — the chart should make that disagreement legible, not paper over it by drawing both the
    same way.
- **AlertsPanel** lists only `is_anomaly === true` points (most recent first, capped to e.g. the
  last 50 to keep the list bounded during a long-running session), each showing timestamp + value;
  it does not list `null` points (that's the banner's job, AD-29) or ground-truth windows (shown on
  the chart's shaded bands, not duplicated as a second list).
- **Rejected**: (a) *`is_anomaly == null` treated as `false` (silently "not anomalous")* — this is
  the exact 2-state collapse AD-24 was written to prevent at the API layer; doing it in the
  frontend would undo that at the last step. (b) *a 4th visual state for "inside a labeled window
  AND flagged by the model"* — would require per-point band/marker interaction logic for a
  distinction the shaded-band + marker overlay already conveys spatially (a red marker sitting
  inside a shaded band reads as "the model correctly caught this" without a special-cased color).

#### AD-29. `model_loaded: false`: a persistent, reactive banner — not a silently empty alerts panel

- **Source of truth is reactive, not a one-time check**: both `GET /api/series/{id}/history`'s
  top-level `model_loaded` field (AD-26's initial fetch) and every live WS message's own
  `model_loaded` field (AD-22 — the live feed broadcasts this per message, not just once) feed the
  same `modelLoaded` state in `useLiveSeries`. If `backend` is restarted mid-session after
  `scripts/train.py` finally produces `models/isolation_forest.joblib` (AD-24's own documented
  recovery path), the *next* live message flips `model_loaded` to `true` and the banner disappears
  without a page reload — the dashboard doesn't need its own polling of `/api/model` to learn this,
  since the live feed already carries the flag on every message.
- **`ModelStatusBanner`** renders a persistent (non-auto-dismissing) warning banner exactly when
  `modelLoaded === false`: *"No trained model detected — anomaly detection is unavailable. All
  points are shown as unscored. Run `scripts/train.py` to enable detection."* — visible the whole
  time the condition holds, not a toast that disappears before the user reads it.
  `GET /api/model` itself is **not** additionally polled by the dashboard for this — AD-26/AD-27
  already establish `/history` + the live feed as the two data sources, and both already carry
  `model_loaded`; calling `/api/model` too would be a third source of the same one boolean fact for
  no added information (it's useful for a human hitting the endpoint directly, per AD-20, not for
  this dashboard's own state).
  While this holds, the chart still renders the raw `value` line in full (AD-28's "line never
  disappears") — every point's marker is the `null`-state hollow marker described in AD-28, so the
  banner and the chart tell a consistent story instead of the chart silently looking identical to
  "the model checked everything and found nothing."
- **Rejected**: (a) *hiding `AlertsPanel` entirely when no model is loaded* — silent absence is
  exactly the failure mode the task named ("au lieu de juste masquer silencieusement les alertes");
  an explicit banner plus an panel that's empty *for a stated reason* (its own empty-state message
  echoing the banner) is the honest version. (b) *polling `/api/model` on an interval* — redundant
  with the always-present `model_loaded` field on both existing data sources, per above.

**Deferred (need Slice 6+ information or explicit go-ahead):** any authentication/multi-user
concern (PLANNING §2: single-user MVP); responsive/mobile polish beyond the basic two-column-to-
stacked breakpoint in AD-25; a loading skeleton beyond a plain "Loading…" state for the first
`/history` call; persisting `selectedSeriesId` across a reload (e.g. URL query param) — not
required by the 5 points this slice was scoped against, easy to add later if desired.

**Empirical verification (done against the real running stack, not asserted from code review):**
with `docker compose` already up (`backend`/`redpanda`/`timescaledb`/`consumer` healthy, both NAB
series already ingested from an earlier producer replay) and the frontend image rebuilt to bake in
the new `recharts` dependency, the dashboard was opened in a real Chrome tab
(`http://localhost:3000`) via `claude-in-chrome` — not just checked for a console-error-free load:

- **Chart correctness, point-for-point**: hovered a red (anomaly) marker inside `ec2`'s shaded
  ground-truth band; the tooltip read "4/15/2014, 11:09:00 PM · value: 98.29 · Anomaly detected".
  Cross-checked directly against a fresh `curl` of the same `/history` endpoint: no `ec2` row on
  2014-04-15 has that value, but `{"time": "2014-04-16T03:09:00+00:00", "value": 98.292,
  "is_anomaly": true}` matches exactly — the apparent mismatch was the browser rendering the tooltip
  in its own local timezone (UTC-4 that day), not a data bug; value and `is_anomaly` agree bit for
  bit with the raw API response once the timezone conversion is accounted for.
- **Series switch (AD-25's remount-on-`key` design)**: selecting `rds_cpu_utilization_cc0c53`
  correctly unmounted/remounted the whole chart+alerts subtree — new y-axis scale (0–28% vs. `ec2`'s
  0–100%), **two** shaded ground-truth bands (AD-10's `rds` has 2 labeled windows, `ec2` has 1), and
  a freshly repopulated alerts list, with no stale state bleeding over from the previous series.
- **Reconnect + gap-fill (AD-27)**: `docker compose restart backend` (a real, momentary outage) was
  triggered while the dashboard was connected. Captured network traffic showed exactly the designed
  sequence: a `GET /history?start=<last known point's own timestamp>&...` first returned `503`
  (backend still restarting) — caught, surfaced as an internal error state, backoff scheduled — then
  the identical request retried and returned `200`, after which the WebSocket reconnected and the
  connection badge returned to "Live". No console errors during the outage/recovery window.
- **`model_loaded: false` (AD-24/AD-29), triggered for real**: `models/isolation_forest.joblib` was
  moved out of the way and `backend` restarted (mirroring Slice 4's own AD-24 verification, now
  checked from the frontend's side). The banner appeared ("No trained model detected…"), the alerts
  panel switched to its stated-empty-state message instead of silently showing nothing, and a new
  live point that arrived during the outage rendered as the hollow "no model available" marker —
  while every already-rendered point kept its earlier `true`/`false` color, since neither the
  frontend nor the backend retroactively rescores already-broadcast data. Restoring the model file
  and restarting `backend` again flipped the banner off and the alerts list back to live detections
  within one WS message, with no page reload — confirming AD-29's reactive (not one-time) read of
  `model_loaded`.
- **3-state marker rendering**: zoomed on `ec2`'s leading edge and confirmed the hollow "no model
  available" marker renders for the pre-full-window rows AD-15 drops from `compute_features`
  (`is_anomaly: null` even though `model_loaded: true` for that request) — the one case where a
  loaded model still legitimately produces a null point, and the chart didn't collapse it into
  either colored state.

All of the above were restored to their original state afterward (`models/isolation_forest.joblib`
moved back, `backend` restarted once more, confirmed healthy and scoring again) before this section
was written.

### Slice 6 — Deployment (status: PROPOSED — revised architecture, pending validation; supersedes an earlier Fly.io-based version that was implemented, tested against a real account, and reverted — see PLANNING §7 for the full story of that pivot, not glossed over here)

Scope guard: deploy the existing 4-service stack (Slices 1–5, frozen) to one public host, as close to
`docker-compose.yml` as the host allows — not a re-architecture into per-provider managed services.
Decided before this section was first written (chat, not re-litigated here): a single paid host
(~$5–7/mo) over either a $0 local-video demo or a $0-if-lucky multi-vendor managed-services split,
specifically *because* running a real self-managed multi-container deployment demonstrates container
orchestration/deployment skills this project exists to demonstrate (PLANNING §1).

**This section was rewritten once already.** The original version chose Fly.io on the strength of its
documented `[build.compose]` support (deploy the compose file as one Machine). That was implemented in
full (PR #6, merged) and then tested against a real Fly account: `fly deploy` never actually acted on
`[build.compose]`, and Fly's own community forum confirms the feature is genuinely unstable
("Docker Compose Compatibility: The Journey Begins" — the maintainer's own words: *"this is just the
beginning — there is much more work to be done"*). PLANNING §7 has the full account, including what
was real (the registry-path bug, the Caddyfile bug, both confirmed and fixed) and what turned out not
to matter once Fly itself was abandoned. This section now documents the **replacement decision**: a
plain VPS running `docker-compose.yml` unmodified.

#### AD-30. Host: Hetzner Cloud (CX22) — a real VPS, not a PaaS with its own deploy abstraction

| | Hetzner CX22 | DigitalOcean (1–2GB droplet) | Fly.io (tried, reverted) |
|---|---|---|---|
| Runs `docker-compose.yml` directly, no translation | **Yes** — it's a Linux box; `docker compose up -d --build` against the real file, unmodified | Yes, same reason | **No, in practice** — `[build.compose]` is documented but non-functional against the real `fly deploy` as of this project's own test (PLANNING §7) |
| Specs / price | 2 vCPU, 4 GB RAM, 40 GB disk — **~€3.79–4.35/mo (~$4–4.70/mo)** | 1 GB RAM/1 vCPU: $6/mo; 2 GB RAM/1 vCPU: $12/mo (both confirmed directly on digitalocean.com/pricing/droplets) | ~$6–7/mo (Machine + volumes) — moot, abandoned |
| Maturity of "just run compose here" | As mature as Docker itself — the single most common way Docker Compose is deployed anywhere | Same | Marketplace/experimental-adjacent per Fly's own forum, confirmed broken in this project's hands |

- **Why Hetzner over DigitalOcean**: more RAM per dollar for this footprint (Redpanda + TimescaleDB +
  Python backend + Node frontend running concurrently benefits from headroom beyond the 1 GB tier, and
  DigitalOcean's matching 2 GB tier costs ~2.5–3× as much for the same result). Both are otherwise
  equivalent for this use case (either has a ready-made "Docker on Ubuntu" marketplace image, so
  neither needs a manual Docker install step) — this is a price call, not a capability difference.
- **Why not retry Fly.io with the Machines API directly**: considered explicitly (see the chat
  comparison this section is based on) — it would still need every one of the old AD-31/34
  translations (`build:`→`image:`, `localhost` addressing, a Caddy proxy for one hostname), for zero
  fidelity gain over a VPS, while carrying real residual risk of hitting more of the same platform's
  documented immaturity on a scenario (8–9 interdependent containers with precise startup ordering)
  none of Fly's own multi-container examples actually cover.
- **Rejected**: Railway/Render (unchanged reasoning from the original comparison — both convert compose
  into N separately-billed services, the exact shape a single self-managed host was chosen to avoid);
  Fly.io Machines API (above).

#### AD-31. No compose translation needed at all — `docker-compose.yml` runs exactly as it does locally

This directly answers the question the Fly attempt forced onto the table: **on a real VPS, none of
the old AD-31 (Fly) translations exist as a category of problem**:

- **No `build:`-count limit** — a VPS runs `docker compose build` as normal; every service keeps its
  own `build:` entry, unchanged from `docker-compose.yml` today.
- **No service-name-DNS-to-`localhost` rewrite** — compose's own bridge network already gives every
  service DNS resolution by name (`redpanda:9092`, `timescaledb:5432`); this is exactly what local dev
  already relies on, so prod uses the identical values.
- **No image pre-build-and-push step, no registry** — the VPS builds the images itself, from the same
  Dockerfiles, the same way `docker compose up --build` already works on a laptop.
- **The only new file is a minimal `docker-compose.prod.yml` overlay** (`docker compose -f
  docker-compose.yml -f docker-compose.prod.yml up -d --build`), adding exactly two things
  `docker-compose.yml` doesn't have: the `caddy` service (AD-34) and the prod-only environment values
  (`CORS_ALLOWED_ORIGINS`, a real `POSTGRES_PASSWORD`) that shouldn't live in the dev file's committed
  defaults. Everything else — service definitions, networking, healthchecks, `depends_on` — is
  inherited from `docker-compose.yml` unchanged, via compose's own multi-file merge (a real, mature,
  extremely common docker-compose feature — not the same kind of "documented but unstable" territory
  the Fly compose deploy turned out to be).
- **Rejected**: keeping a full separate `docker-compose.prod.yml` copy (à la the abandoned
  `docker-compose.fly.yml`) — there is nothing left to override that justifies a whole second file
  once no image/networking translation is needed; an overlay merged on top of the real file is both
  less code and structurally guarantees prod never silently drifts from what local dev already proves
  works.

#### AD-32. Production Dockerfiles: unchanged from the Fly attempt — confirmed still valid on a VPS

Nothing here is host-specific; both `prod` stages stay exactly as built and verified before (backend:
no dev deps, no `--reload`, single worker; frontend: real Next.js standalone build). The build-time
NAB CSV fetch added to `backend/Dockerfile`'s `prod` stage during the earlier `/code-review high` pass
is *more* useful on a VPS than it would have been on Fly, not less: it means the producer never needs
`data/` bind-mounted or fetched separately on the server at all — the image already has it. The one
thing that changes is cosmetic: `--build-arg NEXT_PUBLIC_API_URL=https://<public-host>` now takes the
VPS's own domain/IP instead of a `*.fly.dev` one; the mechanism (a build `ARG`, not a runtime env var,
because Next.js bakes `NEXT_PUBLIC_*` in at `next build` time) is identical and already verified.

#### AD-33. Secrets: a `.env.prod` file on the server itself, never committed — the same shape as local dev, not a cloud provider's proprietary store

- No platform secret store exists on a plain VPS — the direct equivalent of local dev's `.env`
  (gitignored, AD-8) is a `.env.prod` created **directly on the server** (via `ssh` + a text editor, or
  `scp`'d over once and never stored anywhere else), referenced by `docker compose --env-file
  .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d`. Never generated or transmitted
  through this assistant — the project owner creates it on the box themselves, the same boundary
  already drawn for the Fly secrets step.
- **Prod Postgres credentials are freshly generated, not `app`/`app`** (the current dev default) —
  unchanged reasoning from the original AD-33: AD-2's "a dev DB and broker with default creds must not
  be reachable from the LAN" becomes non-negotiable, not a nice-to-have, once there's no `127.0.0.1`
  binding to fall back on for a service the whole point is to expose.
- **`cors_allowed_origins`** (`app/config.py`, already implemented and merged) is unchanged — still the
  fix for `main.py`'s one pre-existing exception to "config.py is the only place that reads env vars"
  (AD-5), still populated from `.env.prod` the same way any other setting is.

#### AD-34. Public exposure: Caddy still does path-based routing under one hostname — but now terminates its own TLS

- **The routing shape is unchanged and already built/validated** (`deploy/fly/Caddyfile`, moved and
  adjusted — see implementation section below): `/api/*`, `/ws/*`, `/health*` → `localhost:8000`
  (backend), everything else → `localhost:3000` (frontend), via a named `path` matcher (the exact
  syntax already confirmed correct with `caddy validate` and a real routing test during the Fly
  attempt's `/code-review high` fix).
- **What's different on a VPS: there is no edge proxy in front of Caddy anymore.** Fly's edge
  terminated TLS for free; on a bare VPS, **Caddy itself must get a real certificate**, which means it
  needs a real DNS name pointing at the server's IP — Let's Encrypt (Caddy's automatic-HTTPS default)
  cannot issue a certificate for a bare IP address. **This is an open decision for the project owner,
  not assumed here**: either (a) point a real domain/subdomain already owned at the VPS's IP, or (b) a
  free DNS-wildcard-to-IP service (e.g. `<ip>.sslip.io`) works too — Let's Encrypt only needs a
  resolvable public DNS name, not a purchased domain, and services like this exist exactly for
  IP-only servers. Caddy's `Caddyfile` needs whichever hostname is chosen in place of the bare `:8080`
  it used inside the Fly Machine (where Fly's own edge, not Caddy, held the certificate).
- **Rejected**: a self-signed certificate (browsers flag it, undermining the exact "clickable public
  link" impression this slice exists to create); plain HTTP with no TLS at all (mixed-content/`wss://`
  problems, and a portfolio link that trips a browser warning is worse than no link).

#### AD-35. The producer is still a one-shot job in prod — kept manual for this slice, not automated

Unchanged reasoning from the original AD-35 (none of it was Fly-specific):

- The producer (`restart: "no"`) replays the fixed 14-day NAB dataset once and exits (AD-14); after
  that, no new Kafka messages ever arrive until someone re-runs it.
- Looping it on a timer would reproduce the exact buffer-ordering bug PLANNING §7 (Slice 4) already
  measured (510 mismatches) — restarting the backend before each re-run doesn't fully fix it either,
  since `_seed_buffer` reseeds from data still chronologically *after* where a fresh replay starts.
  The only fully clean fix is a coordinated `TRUNCATE raw_metrics` + backend restart + producer re-run,
  which is real scheduled-automation work this slice's scope guard doesn't include.
- **Decision, updated for the new host**: run it by hand over `ssh <vps> 'docker compose -f
  docker-compose.yml -f docker-compose.prod.yml run --rm producer'` — the same manual, on-demand shape
  as before, `fly ssh console` replaced by plain `ssh` since there's no Fly Machine to shell into
  anymore.
- `REPLAY_SPEED=3600` (unchanged reasoning: ≈5.6 real minutes for the full series, long enough to
  actually watch it stream) set via `docker-compose.prod.yml`'s environment for `producer`, not a code
  change.
- **Not silently dropped**: a scheduled GitHub-Actions-driven reset-and-replay cycle remains an
  explicit, named, deferred option if wanted later, for the same reasons as before (this project has no
  scheduled automation yet, and it's not needed for a portfolio link someone browses to and manually
  replays for).

**AD-34's domain decision, resolved**: [sslip.io](https://sslip.io) (`<vps-ip>.sslip.io`), not a
purchased domain — confirmed by resolving it directly with `dig` before relying on it (both the
dotted-IP and dashed-IP forms resolve correctly; the dotted form is used). Zero cost, zero DNS setup,
and Let's Encrypt only needs a real resolvable hostname, which this is. `deploy/vps/README.md` names
the one line to change if a real domain replaces it later.

**Deferred (need Slice 6 implementation or explicit go-ahead):** the scheduled reset-and-replay
automation named in AD-35; horizontal scaling of the VPS (single-user MVP, PLANNING §2, unchanged by
deployment).

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

### Slice 3 — Model (done)

The bar here is not "training finished without an exception" — it's the real measured precision/recall/F1 against the known windows:

| Check | Kind | Real deps or mocks? | Why |
|---|---|---|---|
| `rolling_mean_1h`/`rolling_std_1h`/`rate_of_change` computed by hand on a small synthetic series, incl. a deliberate cadence gap, compared to `app.ml.features`'s output | unit | none | proves the time-based window (AD-15) handles the real gaps correctly, not just the happy path |
| Rolling state does not cross a `series_id` boundary (two series' feature rows interleaved as input) | unit | none | catches the one bug class that would silently blend unrelated series' statistics |
| Every training-set row's timestamp is strictly before that series' AD-16 cutoff (zero rows from inside or after any labeled window) | integration, against real stack | **real** | direct proof of the no-leakage claim, not just code review of the split logic |
| Real `precision_score`/`recall_score`/`f1_score` per series and combined, from `backend/scripts/train.py` run against the real TimescaleDB data | integration, against real stack | **real** | the actual deliverable the task asks to see — printed/logged, not asserted against a made-up floor before the real numbers are known |
| Loading `models/isolation_forest.joblib` back and re-predicting the same test set reproduces the same metrics | integration | **real** | proves the saved artifact is actually what was evaluated, not a save/load mismatch (feature order, missing metadata, etc.) |

### Slice 4 — API (done)

The bar here is explicitly not "a WebSocket connection was established" — it's real predictions
matching the model's own `predict()`, end to end through Kafka:

| Check | Kind | Real deps or mocks? | Why |
|---|---|---|---|
| `/api/series/{id}/history` unknown `series_id` → 404, `start >= end` → 400 | unit | none — these branches run before any DB I/O | fast, deterministic; the happy path needs real data so it's integration-tested instead |
| `/api/model`: 503 `{"status": "not_trained"}` when `app.state.model_metadata` is `None`, 200 with the metadata verbatim when set | unit | none — `app.state` set directly, no lifespan | pure state-branching logic, same style as `test_health.py`'s dependency-status tests |
| `ConnectionManager.broadcast` reaches only sockets registered for that `series_id`; a failed send disconnects that socket without affecting others | unit | fakes (`FakeWebSocket`) | the actual fan-out/failure-isolation logic AD-22 depends on, deterministic without a real socket |
| WebSocket rejects an unknown `series_id` with close code 1008 before ever registering it; a known `series_id` is registered on connect and deregistered on disconnect (no leaked reference) | unit | none / real `ConnectionManager` | direct proof of AD-23's connect/disconnect contract |
| `/api/series/{id}/history` row-for-row against `raw_metrics`, `labeled_windows` matches `nab_anomaly_windows` for the range, `is_anomaly` matches `model.predict()` computed independently on the same rows, `/api/model` matches the real joblib metadata verbatim | integration, against real stack | **real** | direct proof of AD-20/AD-21's claims, not code review |
| **Empirical, end-to-end**: a real `websockets` client connects to `/ws/series/{id}/live`, a real producer replay is triggered, and every received `is_anomaly` is diffed against `model.predict()` run independently on the same feature rows | manual verification script against the real stack (not a permanent pytest test — needs a running WebSocket server + live replay) | **real** | the task's own bar: prove the client sees real model predictions, not just that JSON arrives — see the AD-22 bug this caught, below |

## 6. Measurements Log

_(real measured numbers only, no unverified estimates — latency, throughput, model
precision/recall, etc., filled in as the project progresses)_

### Slice 3 — Model

One `IsolationForest` (`n_estimators=100`, `contamination="auto"`, `random_state=42`), evaluated
point-wise against `nab_anomaly_windows`. Measured with `backend/scripts/train.py` against the
real TimescaleDB data (2026-09-23), reproduced by reloading the saved
`models/isolation_forest.joblib` and re-predicting independently (see AD-19 verification, §5):

| Series | Train rows | Test rows | Test positives (in-window) | Precision | Recall | F1 |
|---|---|---|---|---|---|---|
| `ec2_cpu_utilization_825cc2` | 1,514 | 2,506 | 343 | 0.126 | 0.825 | 0.218 |
| `rds_cpu_utilization_cc0c53` | 2,968 | 1,052 | 402 | 0.322 | 0.769 | 0.454 |
| **Combined** | 4,482 | 3,558 | 745 | **0.184** | **0.795** | **0.299** |

Reading these: recall is the strong side (catches ~77–83% of truly-anomalous points per series) at
the cost of precision (a lot of false positives — expected from an untuned, unsupervised model
whose only calibration lever is `contamination="auto"`). As AD-17 states explicitly, this is
computed over **3 labeled windows total** — informative for this portfolio project, not a
statistically significant validation. Not tuned further in this slice (no FastAPI/dashboard yet to
consume a tuned threshold meaningfully) — a natural first target if Slice 4/5 surfaces false
positives as a UX problem.

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
- **A malformed message crashed the consumer outright, and `restart: unless-stopped` turned that
  into a permanent crash-restart loop.** Flagged by `/code-review`, reproduced directly: published
  a non-JSON garbage message to `raw-metrics` and watched the consumer die on an uncaught
  `json.JSONDecodeError`, restart, hit the same uncommitted message again, and die again —
  confirmed via `docker inspect --format '{{.RestartCount}}'` climbing (4 and counting) rather than
  stabilizing. A second, related case: a well-formed JSON message with a DB-unwritable value (e.g.
  a non-numeric `value`, or `series_id: null` against the `NOT NULL` constraint) hit
  `write_with_retry`'s blanket `except psycopg.Error` and was retried forever with exponential
  backoff — a permanent data problem misclassified as a transient connectivity one, silently
  stalling that partition forever with no crash and no error surfaced anywhere. Fixed both with the
  same principle: a message that can never succeed is logged and skipped (offset committed), not
  retried forever or left to crash the process. Concretely — `parse_message()` in
  `backend/app/ingestion/consumer.py` extracts and validates the 3 fields up front, and
  `write_with_retry` now only retries `psycopg.OperationalError`/`OSError` (genuine connectivity
  failures); any other `psycopg.Error` propagates so the caller can skip it instead of looping.
  Re-ran the identical garbage-message and bad-value/null-value reproductions after the fix,
  interleaved with valid messages: all bad messages were logged and skipped, all valid messages on
  either side were written correctly, and `RestartCount` stayed at `0` throughout. Added
  `backend/tests/unit/test_consumer.py` as a permanent regression test for both cases (no real
  Kafka/DB needed — a fake connection object simulates the operational-vs-permanent-error split).
- **Considered and rejected: batching Kafka sends/commits for throughput.** `/code-review` also
  flagged that the producer awaits each `send_and_wait` individually (no pipelining) and the
  consumer commits its Kafka offset after every single row (no batching). Measured both before
  deciding: a full producer replay of both series (8,064 messages, `REPLAY_SPEED=0`) completes in
  ~4s wall time including container startup, and the consumer's full catch-up from empty takes
  ~8.5s including consumer-group-join overhead — both single-digit seconds for this project's
  actual dataset size. AD-13 already chose row-at-a-time deliberately, to keep commit granularity,
  DB-write granularity, and idempotence granularity all equal (§4, AD-13); batching would reintroduce
  partial-batch-failure handling for a throughput problem that doesn't exist at this scale. Not
  applied — revisit only if the dataset size changes materially (e.g. ingesting the full NAB
  corpus, which AD-10 explicitly deferred).

### Slice 3 — Model

- **`pandas==2.3.3` + `numpy==2.5.3` (the versions `pandas>=2.2,<3` first resolved to) raise a
  `DeprecationWarning` on every single `pd.Timedelta(...)` construction** — confirmed directly,
  not assumed: `pd.Timedelta(minutes=60)` and even the plain string form `pd.Timedelta('60min')`
  both trigger `"The 'generic' unit for NumPy timedelta is deprecated"` under `-W
  error::DeprecationWarning` on this pandas/numpy pairing, i.e. `app/ml/features.py`'s time-based
  rolling window (AD-15) would have logged a deprecation warning on every training run and is one
  numpy release away from an outright break. Fixed by widening the dependency range to
  `pandas>=2.2,<4` and locking to the resolved `pandas==3.0.6`, which does not exhibit this warning
  (checked the same `pd.Timedelta` calls under `-W error::DeprecationWarning` again, clean). No
  other code changes needed — `compute_features`'s pandas API surface (`.rolling()`, `.diff()`,
  groupby) is unchanged between 2.3 and 3.0 for what this project uses.
- **A mid-series gap >= the 60-minute window produced a NaN-shaped feature row instead of being
  dropped.** Flagged by `/code-review`, reproduced directly: a series with a 90-minute gap
  partway through (not at the start — AD-15's existing leading-rows drop only guards the start)
  produces `rolling_std_1h = NaN` on the row right after the gap, because that row's own trailing
  window contains only itself, even though `has_full_window` (which only checks distance from the
  *series'* start, not from the row's own preceding sample) passes. Checked whether this actually
  crashes training as the report suggested: it doesn't — `IsolationForest.fit()`/`.predict()` on
  `sklearn==1.9.1` tolerate `NaN` natively (`allow_nan=True` in its sklearn tags), so this
  wouldn't have thrown. Fixed anyway: a NaN-shaped row isn't a real "normal" or "anomalous"
  feature vector, it's a data-quality artifact from missing monitoring data, and `app/ml/features.py`
  is explicitly the same code Slice 4's live Kafka-fed inference will use, where real gaps
  (broker downtime, consumer lag) are plausible in a way they aren't in the closed 2-series NAB
  replay. `compute_features` now drops any row with a NaN in `FEATURE_NAMES` as a final step.
  Confirmed against the real ingested data that this changes nothing today (max real gap is 10
  minutes, per AD-15 — `compute_features` output and `scripts/train.py`'s metrics are
  bit-for-bit identical before and after the fix) and added a permanent unit test reproducing the
  90-minute-gap case.
- **`test_rolling_state_never_crosses_a_series_boundary` didn't actually test what it claimed —
  its two synthetic series were 5 calendar months apart, so a 60-minute window could never reach
  across them regardless of whether the code grouped by `series_id` correctly.** Proved this
  concretely rather than taking the report's word for it: wrote a deliberately un-grouped
  "buggy" version of `compute_features` (a single global time-sorted rolling window, ignoring
  `series_id` entirely) and ran the *existing* test against it — it passed, confirming a real
  cross-series-leakage regression would have shipped silently. Fixed by giving both series the
  *same* timestamps instead of ones 5 months apart (so a real leak, if introduced, would visibly
  blend their very different values); re-ran the same "buggy" version against the *new* test and
  confirmed it now correctly fails.
- **Minor: `compute_features` re-sorted each per-series group by time after already sorting the
  whole frame by `(series_id, time)` up front** — confirmed the second sort was a genuine no-op
  (`group['time'].is_monotonic_increasing` was already `True` for every group) and removed it.

### Slice 4 — API

- **The live-feed task's groupless partition assignment (AD-22) was a one-shot lookup — on a fresh
  stack it permanently locked onto zero partitions and silently never received anything, ever.**
  Caught only by running the actual empirical verification the task calls for (a real `websockets`
  client + a real producer replay against a stack reset with `docker compose down -v`), not by
  reading the code: `backend` only depends on `redpanda`/`timescaledb` being healthy
  (`docker-compose.yml`), not on the producer having run, so on a genuinely fresh stack the
  `raw-metrics` topic doesn't exist yet when the live-feed task starts. The original
  `_assign_at_tip()` called `partitions_for_topic()` exactly once, got an empty result, called
  `consumer.assign([])`, and never looked again — even after the producer created the topic and
  published thousands of messages seconds later, the task stayed assigned to nothing and the
  WebSocket client received **zero** messages, with no error anywhere (the task just idled cleanly
  inside `async for msg in consumer`, which yields nothing forever on an empty assignment). Fixed
  by retrying `partitions_for_topic()` with exponential backoff until it returns a non-empty result,
  mirroring AD-13's "block and retry, don't give up" stance. Re-ran the identical reset-and-verify
  sequence after the fix: the live-feed task correctly received all 4,032 rows for `ec2` and all
  isolation-forest `is_anomaly` flags matched `model.predict()` run independently on the same rows
  — 0 mismatches (see the Measurements/Testing entry above).
- **A second empirical run (immediately after the first, without resetting the stack) produced 510
  mismatches between the live feed and independently-recomputed predictions — investigated before
  concluding anything was actually broken.** The second run re-invoked the producer a second time
  while the API's per-series rolling buffer (AD-22) was already warm with data from near the *end*
  of the first replay (the last ~2 hours of the 14-day series); the second replay's messages start
  again from the *beginning* of the series (2014-04-10), i.e. chronologically *before* what was
  already sitting in the buffer. The buffer's eviction logic assumes each newly-appended point is
  the newest, which is false in that specific sequence, corrupting the buffer's effective time
  window for those points. **This is not a bug in the intended usage** (the producer is a one-shot
  replay job, `restart: "no"`, meant to run exactly once per demo) — it's an artifact of re-running
  it a second time mid-session purely to generate fresh Kafka messages for testing. Confirmed by
  repeating the *first* scenario (fresh stack, single producer run) cleanly: 0 mismatches. Not
  fixed — handling arbitrary backward time jumps in the live buffer is out of this slice's scope
  (AD-12 already treats the producer as a single chronological replay); documented here instead so
  a future slice doesn't rediscover this by surprise if the demo/deployment strategy (Slice 6) ever
  involves re-running the producer against a live API.
- **Path routing: `series_id` values contain a literal `/`** (e.g.
  `realAWSCloudwatch/ec2_cpu_utilization_825cc2`, per AD-10's naming) **— FastAPI's default
  `{series_id}` path parameter stops at the first `/`,** so `/api/series/{series_id}/history` and
  `/ws/series/{series_id}/live` both 404'd immediately when first tested against a real series ID
  (caught by the very first `curl` against the running stack, before writing any permanent test).
  Fixed by using the `:path` path converter (`{series_id:path}`) on both routes, which greedily
  matches everything up to the required literal suffix (`/history` or `/live`). Confirmed against
  the real stack afterward: both routes resolve correctly for both series IDs.
- **`/code-review high` on PR #4 found a real `docker-compose.yml` startup race this slice
  introduced: `backend` depended on `redpanda`/`timescaledb` being healthy, but not on `migrate`
  having *finished* creating `raw_metrics`/`nab_anomaly_windows`** — a gap that didn't matter before
  Slice 4 (nothing at backend startup touched the DB) but does now that the live-feed task's buffer
  seeding queries `raw_metrics` at startup. Reproduced directly, twice: (1) a normal
  `docker compose up` from empty volumes showed `migrate` finishing only ~1.1s before backend's
  first DB query — close enough to be a real, not theoretical, race; (2) the worst case, starting
  `backend` with `migrate` never having run at all, made the live-feed task die with an uncaught
  `UndefinedTable` from `_seed_buffer` — silently: no log line, `/health` stayed `200`, and a
  WebSocket client kept getting accepted connections that received **zero** messages for the rest of
  the container's life, confirmed to never recover even after running `migrate` and a full producer
  replay afterward against that same already-started backend. Fixed two ways: (a) added
  `migrate: condition: service_completed_successfully` to `backend`'s `depends_on`
  (`docker-compose.yml`), matching `producer`/`consumer`'s existing convention, which removes the
  race in normal usage; (b) defense in depth regardless — buffer seeding
  (`feed_consumer._seed_all_buffers`) now retries with exponential backoff on `psycopg.Error`
  instead of raising once, mirroring AD-13's ingestion-consumer stance and AD-22's own
  topic-not-ready retry. Re-ran the exact worst-case reproduction (`backend` started with `migrate`
  never run) after the fix: the task now logs visible retry lines instead of dying, and once
  `migrate` + a producer replay ran afterward, that same long-running backend process picked up and
  started streaming real scored predictions on its own — confirmed with a real WebSocket client.
- **Same review pass: `ConnectionManager.broadcast()`'s bare `except Exception` around
  `send_json()` would silently misclassify a real bug as a dead client.** `send_json()` calls
  `json.dumps()` *before* touching the socket (confirmed by reading Starlette's source directly,
  not assumed) — a non-serializable value in the broadcast message raises `TypeError` there, before
  any connection-related failure is even possible, while an actually-broken connection surfaces as
  `WebSocketDisconnect` or `RuntimeError` (also confirmed from Starlette's `WebSocket.send()`
  source). The bare `except Exception` treated both identically: silently disconnect the client and
  drop the message, with no way to tell "client went away" from "we sent it garbage." Not
  triggered by any data this slice currently sends, so this was a latent gap rather than a live
  failure — fixed anyway by narrowing the `except` to `(WebSocketDisconnect, RuntimeError)`, so a
  future serialization bug propagates loudly instead of vanishing. Added
  `backend/tests/unit/test_broadcaster.py` cases proving a `TypeError` now propagates (socket stays
  registered) while a `WebSocketDisconnect`/`RuntimeError` is still treated as gone. Also added a
  broad `try/except` around each message's scoring+broadcast in `run_live_feed`'s main loop, so one
  bad message still can't take the whole live-feed task down for every connected client.
- **Same review pass: a `start`/`end` query param on `/api/series/{id}/history` with no UTC offset
  crashed with a `500`.** FastAPI/pydantic parses `"2014-04-15T00:00:00"` (no trailing `Z`/offset)
  as a *naive* `datetime`, while every `time` value read back from TimescaleDB (`TIMESTAMPTZ`) is
  tz-aware — reproduced directly against the running server (a plain `500 Internal Server Error`)
  before touching any code. Fixed by normalizing a naive `start`/`end` to UTC right after parsing
  (AD-12's own convention: this project's timestamps are UTC when no zone is given), rather than
  erroring or guessing the caller's local zone. Added a regression test
  (`test_history_accepts_naive_start_end_as_utc`) to `tests/integration/test_api.py`.
- **Same review pass, acknowledged but not fixed: `/api/series/{id}/history` opens a new
  `psycopg` connection per request instead of using a pool.** Real observation, not disputed — but
  at this project's actual scale (single-user MVP, PLANNING §2; 2 series; no measured concurrent
  load) it isn't a live problem, and adding pooling (a new dependency, `psycopg_pool`, plus lifespan
  wiring) for a cost that hasn't been measured would be exactly the premature optimization this
  project's own conventions argue against elsewhere (e.g. Slice 2 AD-13's rejected batching, decided
  the same way: measure first, don't add complexity for a bottleneck that isn't there yet). Deferred
  to Slice 5+ if a real dashboard's polling pattern ever measurably needs it — same treatment as
  `_SERIES_IDS`'s dedup below, both filed rather than silently dropped.
- **Same review pass: `_SERIES_IDS` (the "is this a known series_id" set) was defined identically
  in both `app/api/metrics.py` and `app/api/ws.py`.** No behavioral bug today, but the two copies
  could silently diverge under a future edit. Moved to a single `SERIES_IDS` constant in
  `app/ingestion/series_registry.py` (the module that already owns `SERIES`) and imported by both.

**Second `/code-review high` pass, after the fixes above, per the task ("relance le code-review pour
vérifier que les fixes tiennent") — found 3 more real issues in the same file, plus a repeat of the
already-deferred connection-pooling note:**

- **The live-feed loop accepted *any* `series_id` from the Kafka topic — unlike `/history` and the
  WebSocket endpoint, which both validate against `SERIES_IDS` before doing anything.** Confirmed
  directly: published a message with a bogus `series_id` straight to the topic (bypassing the
  producer entirely) and it was silently accepted into `buffers` with zero trace anywhere. A stray
  or malformed producer message would have grown `buffers` with a junk entry for the rest of the
  container's life. Fixed by validating `series_id` against the same `SERIES_IDS` used by the
  REST/WebSocket layer (§7's dedup above) and skip-and-log otherwise — re-ran the identical
  reproduction after the fix and confirmed the message is now rejected with a visible log line.
- **`_score_input_row` (rebuilding a small pandas DataFrame and recomputing the rolling features
  from scratch on every incoming message) ran directly on the event loop, unlike `model.predict()`
  right next to it, which AD-21 already measured and moved to `asyncio.to_thread()`.** Measured
  directly on this project's real buffer size (~25 rows at NAB's cadence over `BUFFER_MINUTES`):
  **~1.1ms** — the same order of magnitude as `model.predict()`'s own measured ~2.2ms, i.e. the exact
  "non-negligible, don't block the loop" threshold AD-21 already established, just left unapplied to
  this one call site. Fixed by running it through `asyncio.to_thread()` too, for the same reason.
- **The main `async for msg in consumer:` loop had no exception handling of its own — only
  per-message processing did.** If the underlying Kafka fetch ever raised instead of retrying
  internally, the whole live-feed task would die silently, exactly like the buffer-seeding and
  topic-assignment races already fixed earlier in this same file. **Tried hard to reproduce and
  could not**: a `docker compose restart redpanda` and a full 40-second `docker compose stop
  redpanda` (well beyond a normal healthcheck cycle) both recovered on their own via aiokafka's
  internal reconnect logic — confirmed with a real WebSocket client receiving live, correctly-scored
  messages again after each outage, with no task restart needed. Fixed anyway, as defense in depth:
  extracted the consumer lifecycle into `_consume_forever()` and wrapped it in an outer
  retry-with-backoff loop in `run_live_feed()`, consistent with the retry-don't-die pattern already
  applied twice in this file. Documented as *not empirically confirmed dead*, unlike the other
  findings in this section — applied for consistency and because aiokafka's internal retry coverage
  isn't a documented guarantee, not because a live failure was observed.
- **The same per-request-connection-pooling observation from the first review pass was raised again,
  now also naming `feed_consumer.py`'s per-seed-cycle connection as a third instance of the same
  pattern.** No new information changes the earlier call: still deferred, still not a measured
  problem at this project's single-user MVP scale (PLANNING §2).

Added `backend/tests/unit/test_feed_consumer.py` (previously nonexistent) covering the unknown-
`series_id` skip and the outer loop's retry-after-failure behavior. Re-ran the full empirical bar
once more after all of the above, on a stack rebuilt from these fixes: 1,518/1,518 live messages
matched `model.predict()` independently — 0 mismatches. 55/55 backend tests pass.

### Slice 5 — Frontend

No CI is configured for this project (verification has always run against the real `docker compose`
stack, not a pipeline) — `/code-review high` was run manually against PR #5 before merging, matching
the bar every prior slice's PR was held to:

- **A failed initial `GET /history` silently opened the WebSocket anyway.** `connect()` caught the
  fetch error, set `error`, and fell through unconditionally into `ws = new WebSocket(...)` — if
  that socket then connected, `connectionState` became `"open"` ("Live") while `points` stayed
  empty forever, since a later reconnect's gap-fill also had nothing to work from
  (`lastKnownTime()` returns `null` with zero points ever recorded). A backend blip at the exact
  moment a series is selected would have shown a healthy-looking, permanently blank chart. Fixed:
  a fetch failure (initial or reconnect gap-fill) now retries the *whole* `connect()` — history
  fetch included — on the same backoff as a dropped socket, and never opens a socket over data
  known to be incomplete.
- **`ws.onclose` never inspected the close code**, so a permanent rejection (AD-23: code `1008` for
  an unknown `series_id`) would have been retried with exponential backoff forever instead of
  settling into the `"closed"` state `ConnectionState` already declared but nothing ever set. Fixed:
  a `1008` close now sets `connectionState: "closed"` and surfaces the close reason as an error,
  without scheduling another attempt.
- **`useLiveSeries`'s `error` field was computed but never rendered anywhere** — every fetch/socket
  failure above was already tracked in state and completely invisible to the user. Added
  `components/ErrorBanner.tsx` (rendered in `page.tsx`'s `Dashboard`, plus for a failed
  `GET /api/series` at the top level) so a connection problem is always visible, not just logged to
  state no component reads.
- **`getSeries().then(...)` had no `.catch()`** in `page.tsx` — a failed `/api/series` call was an
  unhandled promise rejection and left the page on "Loading…" forever with zero indication anything
  was wrong. Fixed with a `.catch()` setting a `seriesError` state, rendered via the same
  `ErrorBanner`. Reproduced directly: stopped `backend`, loaded the page fresh, confirmed the banner
  ("Connection problem: Failed to load the series list: Failed to fetch") replaces the old
  infinite-spinner behavior; restarted `backend` and confirmed a fresh load recovers normally.
- **Every incoming live WebSocket message cloned the entire `points` `Map` and re-sorted it** — fine
  at the message rates a real demo replay produces, but at `REPLAY_SPEED=0` (the project's own
  "as fast as Kafka accepts" mode, PLANNING's Slice 2 measurements log) messages arrive far faster
  than a browser tab should re-render. Fixed by buffering incoming messages in a plain object
  (not React state) and flushing at most once per animation frame via `requestAnimationFrame`,
  bounding the clone+sort cost to the browser's own paint rate regardless of message arrival rate —
  the same "don't do avoidable work per message" instinct as the backend's own AD-21
  `asyncio.to_thread` fix, applied on the client side instead.
- **Minor — date/time formatting was duplicated** across `AlertsPanel` and `MetricChart`'s tooltip
  and axis-tick formatter. Extracted to `lib/format.ts` (`formatDateTime`, `formatAxisTick`) so a
  future display-format change (e.g. forcing UTC display, given this same review's own empirical
  verification note about local-timezone tooltip confusion) has one place to change, not three.
- **Minor — dead CSS**: `globals.css` declared a `:root[data-theme="dark"]` override block, but no
  component in this slice ever sets a `data-theme` attribute — no toggle UI exists yet, so the rule
  could never match. Removed rather than left as an apparent (but non-functional) theme-toggle hook;
  dark mode is OS-`prefers-color-scheme`-only until an actual toggle is built.

All fixes re-verified against the real running stack via a live Chrome session (not just `tsc`/lint/
build, though all three stayed clean): the fetch-failure retry and the new error banner were both
reproduced by stopping `backend` mid-session and on a fresh page load, confirming the fix in each
case before restarting `backend` and confirming recovery. Squash-merged to `main` after this pass.

### Slice 6 — Deployment

`/code-review high` on PR #6 found two severe gaps and several real correctness/simplification
issues before any of this had a chance to fail silently on a real deploy — exactly the value of
running it before merging, since none of these would have thrown an error or failed a healthcheck:

- **The trained model artifact had no path to the deployed Machine at all.** `models/` is gitignored
  (AD-19) — local dev gets it via a bind mount (`./models:/app/models:ro`,
  `docker-compose.yml`), but `docker-compose.fly.yml` had no volume, no Fly Volume, and no Dockerfile
  `COPY` putting a model file anywhere on the Machine. `load_model()` would have found nothing,
  `app.state.model` stayed `None` forever (AD-24's own "handled state," which is exactly why this
  wouldn't have crashed or failed a healthcheck), and the deployed demo would silently never score a
  single anomaly. Fixed by adding a `models_data` Fly Volume (`fly.toml`) shared between two new
  processes: `backend` (reads) and a new one-shot `train` compose service (writes, running
  `scripts/train.py` against the real deployed TimescaleDB once ingestion has real rows) —
  `deploy/fly/README.md` now sequences producer -> train -> restart backend. Verified for real:
  built the `prod` backend image, ran `scripts/train.py` inside it against this project's actual
  local TimescaleDB (mounting a throwaway directory, not the real `models/`), and got the exact same
  precision/recall/F1 numbers already on record in this document's Slice 3 measurements log
  (0.126/0.825/0.218 for `ec2`, 0.322/0.769/0.454 for `rds`) — bit-for-bit consistent, not just "it
  ran without an exception."
- **The producer's NAB CSVs had no path to the deployed Machine either.** `docker-compose.fly.yml`
  bind-mounted `./data:/data:ro` — a path that only exists on a developer's own checkout (`data/` is
  gitignored, AD-1), never on a remote Fly Machine. Following `deploy/fly/README.md`'s own documented
  producer step as originally written would have crashed with `FileNotFoundError`. Fixed by baking
  the 2 NAB CSVs into the backend `prod` image at *build* time (`backend/Dockerfile`, fetched from
  the same NAB GitHub URLs `scripts/fetch_nab_data.sh` already uses, at the exact `/data` path the
  producer already defaults to — no env var or compose change needed) — AD-14's "the producer
  container itself needs no internet access" still holds at *runtime*, unchanged; only the build step
  gained a network dependency, which is normal. Verified by building the image for real and
  confirming both CSVs land with the correct row counts (4,032 data rows each, matching AD-10).
- **The Caddyfile routed `/health`/`/health/ready` to the frontend, not the backend — caught by
  actually validating the config, not by reading it.** `backend/app/api/health.py` registers both
  routes with no `/api` prefix, so they fell through the original `/api/*`/`/ws/*`-only rules into
  the catch-all block, which forwards to the frontend (port 3000) — meaning `deploy/fly/README.md`'s
  own post-deploy `curl .../health` check would have hit a Next.js 404, not the backend. Fixed by
  adding a named `path` matcher covering `/api/*`, `/ws/*`, and `/health*` together. **A second,
  real bug was caught fixing the first one**: the initial fix used `handle /api/* /ws/* /health* {
  ... }`, assuming (wrongly) that `handle` takes multiple path arguments the way some other Caddy
  directives do — running `caddy validate` against the actual Caddyfile (not just reading Caddy's
  docs) failed immediately with a parse error, before this ever reached a real deploy. `handle` takes
  exactly one argument; a named `path` matcher (which *does* accept several patterns) referenced by
  `handle @name` is the correct shape, confirmed valid by the same `caddy validate` run. Further
  confirmed with a real routing test: ran the built backend `prod` image and a Caddy container
  sharing its network namespace (`docker run --network container:<backend>`, deliberately mirroring
  how Fly's compose Machine shares one namespace across containers, AD-31) and hit Caddy's `:8080`
  from outside — `/health` and `/api/series` both correctly reached the backend.
- **`caddy`'s `depends_on` used plain service names instead of `condition: service_healthy`**, unlike
  every other service in the same file. Since backend's own startup chain and frontend's healthcheck
  both take real time, Caddy (the one public entry point) could start accepting traffic before either
  was actually ready, turning the README's own immediate post-deploy verification into a race against
  502s. Fixed to match the file's own established pattern.
- **The `${POSTGRES_PASSWORD}`/`${FLY_APP_NAME}`/`${IMAGE_REGISTRY}` substitutions were documented as
  coming from `fly secrets set` — which is not how Compose variable substitution works.** `${VAR}` in
  a compose file is resolved by whatever process *parses that file* (locally, when `fly deploy` reads
  it), not by anything injected into a container's runtime environment — `fly secrets` and compose
  substitution are two unrelated mechanisms that happened to look interchangeable in the first draft.
  This file's own header comment already flagged the substitution path as "least-verified," which
  this confirmed was a concrete gap, not just a hedge: without exporting these in the *deploying*
  shell, `DATABASE_URL` would silently resolve to a passwordless connection string and
  `CORS_ALLOWED_ORIGINS` to `https://.fly.dev` (matching no real `Origin` header). Fixed by rewriting
  `deploy/fly/README.md`'s secrets section to `export` all three in the same shell `fly deploy` runs
  from, and correcting the compose file's own header comment to stop claiming otherwise.
- **Minor — simplification**: `docker-compose.fly.yml` retyped `DATABASE_URL`/`KAFKA_BOOTSTRAP_SERVERS`
  across four services instead of reusing an `x-backend-env` YAML anchor, unlike the sibling dev file
  right next to it doing exactly that. Added the same anchor. Also hardcoded `app`/`monitoring`
  instead of parameterizing `${POSTGRES_USER:-app}`/`${POSTGRES_DB:-monitoring}` the way the dev file
  already does — fixed to match.
- **Minor — the backend `prod` image shipped `tests/`** via an unqualified `COPY . .` with no
  matching `.dockerignore` entry (only `.venv`/`__pycache__`/`.pytest_cache` were excluded). Added
  `tests` to `backend/.dockerignore`; `scripts/` is kept, since the new `train` service (above) needs
  `scripts/train.py` at runtime.

Re-ran the full local verification pass after all of the above: both `prod` images still build
clean, 55/55 backend tests still pass, local `docker compose up` dev workflow still fully unaffected.
The real `fly deploy` itself remains not-yet-run (no Fly account in this environment, unchanged from
before this review pass) — `deploy/fly/README.md` has the up-to-date command sequence.

**Fly.io was subsequently tried against a real account, found to be genuinely broken, and abandoned —
the full story, not glossed over.** The project owner created a real Fly account, added a payment
method, and worked through the deploy sequence live:

- **A real registry-naming bug, found and fixed before the bigger problem surfaced.** The original
  `docker-compose.fly.yml` referenced images as `${IMAGE_REGISTRY}/mfp-backend:prod` /
  `.../mfp-frontend:prod` (one sub-path per image under the app's registry namespace) — `docker push`
  failed with `404 Not Found` on the blob-upload endpoint. Fly's actual registry convention (confirmed
  by testing the alternative directly, not by reading docs) is **one repository per app, distinguished
  by tag** — `registry.fly.io/<app>:<tag>`, not `registry.fly.io/<app>/<image-name>:<tag>`. Fixed by
  retagging both images as `${IMAGE_REGISTRY}:backend-prod` / `:frontend-prod`; both pushed
  successfully once corrected. A second, unrelated push failure (also `404`) was traced to `docker
  buildx`'s default attestation/provenance manifest, which Fly's registry doesn't accept — fixed with
  `--provenance=false --sbom=false` on both builds.
- **The core premise of AD-30 — that `fly deploy` runs a compose file as one Machine — did not hold up
  against the real, current stable `flyctl` (v0.4.106).** `fly deploy` failed with `app does not have a
  Dockerfile or buildpacks configured` despite a `[build.compose]` block that `fly config show`
  confirmed was parsed correctly. The original `fly.toml` used a top-level `compose = "..."` key (a
  transcription error, confirmed wrong against Fly's own docs, which specify a nested `[build.compose]`
  table) — fixing that syntax error did not fix the underlying problem: `fly deploy` still refused to
  recognize the compose config after the fix, tried both the default `compose.yml` filename and an
  explicit `file =` override, with identical failures either way.
- **Directly asking Fly's own documentation confirmed this is a known, acknowledged rough edge, not a
  local mistake.** Fly's community forum ("Docker Compose Compatibility: The Journey Begins",
  community.fly.io) has the Fly maintainer's own words: *"This is just the beginning — there is much
  more work to be done,"* actively soliciting real-world use cases to prioritize further work, and
  documents exactly the kind of failure mode hit here (`app does not have a Dockerfile or buildpacks
  configured` appearing even with a seemingly-correct compose config) as a known, unresolved issue for
  some users. This wasn't findable by reading Fly's marketing/guide-style documentation alone (which
  presents `[build.compose]` as a shipped, working feature, `flyctl v0.3.152+`) — only by testing it
  for real against a live account, and then specifically searching for others hitting the identical
  error string, did the real (unstable, early-stage) status surface.
- **Decision: abandon Fly.io for this project, before sinking more effort into working around an
  admittedly-unstable feature.** The next-cheapest workaround (Fly's lower-level Machines API
  `containers` array, bypassing the broken compose-translation layer) was evaluated and explicitly
  *not* chosen: it would still require every one of AD-31/34's translations (`build:`→`image:`,
  `localhost` addressing, a Caddy proxy for one hostname) for zero fidelity gain over a plain VPS,
  while risking further undocumented rough edges on a use case (8–9 interdependent containers with
  precise startup ordering) none of Fly's own multi-container examples actually resemble (they're all
  1–2-container sidecar patterns — metrics collectors, secrets agents — not a full application stack).
  Replaced with a plain VPS (Hetzner Cloud, current §4 AD-30..AD-35) that runs `docker-compose.yml`
  completely unmodified — the single most mature, boring, well-trodden way to deploy Docker Compose
  that exists, with none of AD-30's original comparison advantages ("one host, not N billed services")
  lost, since a VPS *is* one host, natively, with zero translation layer to be unstable in the first
  place.
- **Real cloud resources were created and then fully torn down**: a Fly app, 3 Fly Volumes
  (`redpanda_data`, `timescaledb_data`, `models_data`), and 2 pushed images. `fly apps destroy` was run
  and confirmed via `fly apps list` (empty) and a volumes query against the now-nonexistent app
  (`app not found`) — no residual Fly.io billing from this attempt. `fly.toml`, `docker-compose.fly.yml`,
  and `deploy/fly/` are removed from the repo in the same change that introduces the VPS-based files
  (§4's current AD-30..AD-35) — kept only in this section's history, not as dead config nobody should
  run.
- **What was still real and worth keeping, despite the platform being abandoned**: the registry-naming
  fix and the earlier `/code-review high` fixes (Caddyfile routing, the model/dataset build-time
  baking, the `x-backend-env` anchor, etc., all documented above) were genuine, transferable findings —
  most carry over directly to the VPS-based Caddyfile and Dockerfiles unchanged, which is why AD-32
  (Dockerfiles) in particular needed no rework at all for the new host.

**`/code-review high` on PR #7 (the VPS replacement) found real issues before the first real deploy,
same discipline as every prior slice:**

- **`.env.prod` was never added to `.gitignore`**, only `.env` and the now-obsolete `.env.fly` — a real
  gap, since `deploy/vps/README.md` names `.env.prod` specifically and it holds a freshly-generated
  Postgres password. Fixed by adding the literal entry.
- **The Verify section's `curl -sf https://$PUBLIC_HOSTNAME/...` relied on a shell variable that was
  only ever written into `.env.prod`, never exported** — copy-pasting the README's own commands in
  order would have expanded to `https:///health` (empty host) and failed confusingly. Fixed by adding
  an explicit `set -a; source .env.prod; set +a` step before it's needed.
- **The prod overlay's `volumes: !override` for `backend` dropped `:ro`**, giving the permanently
  running, internet-facing API process unnecessary write access to `/app/models` — the only reason it
  was writable at all was so a `docker compose run --rm backend python scripts/train.py` invocation
  could write to it, reusing `backend`'s own service definition. Fixed by restoring `:ro` on `backend`
  and adding a small dedicated `train` one-shot service instead (tagged with a Compose `profile` so it
  never starts on a plain `up -d`, confirmed with `docker compose config --services`) — least privilege
  for the always-on service, without losing the ability to retrain on demand.
- **The `!override` merge tag's version requirement was asserted, not checked, against the actual
  target server.** Verified for real over SSH against the live Hetzner box (a throwaway two-file
  compose test): Compose v2.32.4 (bundled with the Docker CE marketplace image) supports it correctly.
- **Minor — simplification**: `build: { target: prod }` was repeated identically across four services.
  Extracted to an `x-prod-build` YAML anchor, matching the dev file's own `x-backend-env` convention
  right next to it. The new `train` service needed its own explicit `context: ./backend` alongside the
  anchor, since (unlike the four services above) it has no base-file entry to inherit `context` from —
  caught by actually running `docker compose run --rm train` locally (`open Dockerfile: no such file or
  directory` before the fix), not assumed to work from the anchor alone.

Re-verified after all of the above: `docker compose config` against the real merged files resolves
exactly as intended (backend `:ro` restored, `train` excluded from `--services`/`up -d` but runnable
via `run --rm train`, every prod-target service correctly using the shared anchor); `!override`
re-confirmed on the real server.
