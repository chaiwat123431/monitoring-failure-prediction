# Deploying to Fly.io (Slice 6)

See `PLANNING.md` §4 AD-30..AD-35 for why Fly.io, and for every trade-off/caveat behind the files
here (`fly.toml`, `docker-compose.fly.yml`, `Caddyfile`). This file is just the command sequence.

Everything below needs your own Fly.io account and (per Fly's own free-trial limits) a payment
method on file — that step, and every command that touches your Fly account or bills you, has to be
run by you, not an agent. `!<command>` in a Claude Code session runs a command directly in your own
shell if you'd like help running these interactively.

## One-time setup

```bash
# Install flyctl if you haven't: https://fly.io/docs/flyctl/install/
fly auth login

# Pick a globally-unique app name and edit fly.toml's `app =` line to match before this step.
fly apps create <your-app-name>

# Three persistent volumes (AD-31, plus models_data added after /code-review high on PR #6) —
# region must match fly.toml's primary_region.
fly volumes create redpanda_data --region iad --size 1
fly volumes create timescaledb_data --region iad --size 1
fly volumes create models_data --region iad --size 1
```

## Set the deploy-time variables — in the *same shell* you'll run `fly deploy` from

`docker-compose.fly.yml`'s `${POSTGRES_PASSWORD}` / `${FLY_APP_NAME}` / `${IMAGE_REGISTRY}` are
resolved when *this file is parsed* (i.e. locally, by `fly deploy` reading the compose file) — a
separate mechanism from `fly secrets`, which only injects values into an already-running container's
environment. Setting these as `fly secrets` alone is not enough; export them in the shell you deploy
from too (a generated password kept in both places so `fly ssh console` + `psql` also works later):

```bash
export POSTGRES_PASSWORD="$(openssl rand -base64 24)"
export FLY_APP_NAME="<your-app-name>"
export IMAGE_REGISTRY="registry.fly.io/<your-app-name>"

fly secrets set POSTGRES_PASSWORD="$POSTGRES_PASSWORD"   # for anything that reads it at runtime too
```

(Or put all three in a local, gitignored `.env` file next to `fly.toml` and `source` it before every
`fly deploy` — same effect, easier to not forget one.)

## Build and push the two app images

Fly's compose deploy allows only one `build:` entry per file (AD-31) — build and push both images
yourself first, then `docker-compose.fly.yml` references them by tag:

```bash
fly auth docker  # authenticates `docker push` against Fly's registry

docker build --target prod -t "$IMAGE_REGISTRY/mfp-backend:prod" ./backend
docker push "$IMAGE_REGISTRY/mfp-backend:prod"

docker build --target prod \
  --build-arg NEXT_PUBLIC_API_URL="https://$FLY_APP_NAME.fly.dev" \
  -t "$IMAGE_REGISTRY/mfp-frontend:prod" ./frontend
docker push "$IMAGE_REGISTRY/mfp-frontend:prod"
```

## Deploy

```bash
fly deploy
```

## Verify (do this before treating the deploy as done)

```bash
fly status
curl -sf https://$FLY_APP_NAME.fly.dev/health
curl -sf https://$FLY_APP_NAME.fly.dev/api/series
```

Open `https://$FLY_APP_NAME.fly.dev` in a browser and confirm the dashboard loads. At this point
`raw_metrics` is still empty (only `migrate` has run) and no model exists yet — the chart will show
"Waiting for data…" and the "no model loaded" banner until you run the next two sections.

**This first deploy is also when the least-verified assumptions behind these files get checked for
real**: whether `fly.toml`'s `[[mounts]]` `processes` scoping lands each volume on the right
container (including `models_data`, shared by two processes — the newest and least-tested of the
three), and whether an `external: true` named volume in `docker-compose.fly.yml` actually maps to a
pre-created Fly Volume of the same name. If either doesn't work as written, `fly logs` will show
which service failed to start and why — adjust `fly.toml`/`docker-compose.fly.yml` accordingly and
note the correction in `PLANNING.md`'s Slice 6 section, the same way every prior slice's
empirical-verification pass corrected anything that didn't match what was proposed.

## Seed data and train a model

```bash
# Replays the 2 NAB CSVs (baked into the backend image at build time, backend/Dockerfile) into the
# deployed Kafka/TimescaleDB. AD-35: manual, on demand, not scheduled — see that section for why.
fly ssh console -C "docker compose -f docker-compose.fly.yml run --rm producer"

# Once that finishes (REPLAY_SPEED=3600 means ~5.6 real minutes for the full series, AD-35), train:
fly ssh console -C "docker compose -f docker-compose.fly.yml run --rm train"

# backend loaded with no model at startup (AD-24) — restart it to pick the freshly-trained one up.
fly ssh console -C "docker compose -f docker-compose.fly.yml restart backend"
```

Re-running the producer later replays the same fixed dataset again — see PLANNING.md AD-35 for why
that's safe as a single clean run but *not* safe to put on an unattended schedule without first also
resetting `raw_metrics` and restarting `backend` (the exact Slice 4 buffer-ordering bug this would
otherwise reproduce is documented there).
