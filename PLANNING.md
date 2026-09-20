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

## 5. Testing Strategy

_(to define per slice — QE-senior posture: not just happy-path unit tests; realistic edge cases
such as missing Kafka messages, model drift, outlier bursts, TimescaleDB connection loss; explicit
call on which tests need real dependencies (e.g. real Redpanda in CI via testcontainers) vs mocks,
and why)_

## 6. Measurements Log

_(real measured numbers only, no unverified estimates — latency, throughput, model
precision/recall, etc., filled in as the project progresses)_

## 7. Bugs & False Starts

_(transparency on what didn't work is part of the discipline — filled in as they occur)_
