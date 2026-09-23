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

# Two persistent volumes (AD-31) — region must match fly.toml's primary_region.
fly volumes create redpanda_data --region iad --size 1
fly volumes create timescaledb_data --region iad --size 1

# Secrets (AD-33) — generate a real password, don't reuse the dev default ("app").
fly secrets set POSTGRES_PASSWORD="$(openssl rand -base64 24)"
fly secrets set FLY_APP_NAME="<your-app-name>"
```

## Build and push the two app images

Fly's compose deploy allows only one `build:` entry per file (AD-31) — build and push both images
yourself first, then `docker-compose.fly.yml` references them by tag:

```bash
export IMAGE_REGISTRY=registry.fly.io/<your-app-name>
fly auth docker  # authenticates `docker push` against Fly's registry

docker build --target prod -t "$IMAGE_REGISTRY/mfp-backend:prod" ./backend
docker push "$IMAGE_REGISTRY/mfp-backend:prod"

docker build --target prod \
  --build-arg NEXT_PUBLIC_API_URL="https://<your-app-name>.fly.dev" \
  -t "$IMAGE_REGISTRY/mfp-frontend:prod" ./frontend
docker push "$IMAGE_REGISTRY/mfp-frontend:prod"
```

`IMAGE_REGISTRY` is read by `docker-compose.fly.yml`'s `${IMAGE_REGISTRY}` substitution — export it
in the same shell you run `fly deploy` from (or add it to a `.env.fly` file next to `fly.toml`;
`flyctl` reads a `.env` file in the app root the same way `docker compose` does).

## Deploy

```bash
fly deploy
```

## Verify (do this before treating the deploy as done)

```bash
fly status
curl -sf https://<your-app-name>.fly.dev/health
curl -sf https://<your-app-name>.fly.dev/api/series
```

Open `https://<your-app-name>.fly.dev` in a browser and confirm the dashboard loads. At this point
`raw_metrics` is still empty (only `migrate` has run) — the chart will show "Waiting for data…"
until you run the producer (next section).

**This first deploy is also when AD-31/AD-34's least-verified assumptions get checked for real**:
whether the `[[mounts]]` `processes` scoping above actually lands each volume on the right
container, and whether Fly's compose engine passes `fly secrets` through as `${VAR}` substitutions
the way local `.env` does for `docker-compose.yml`. If either doesn't work as written, `fly logs`
will show which service failed to start and why — adjust `fly.toml`/`docker-compose.fly.yml`
accordingly and note the correction in `PLANNING.md`'s Slice 6 section, the same way every prior
slice's empirical-verification pass corrected anything that didn't match what was proposed.

## Run the producer (AD-35 — manual, on demand, not scheduled)

```bash
fly ssh console -C "docker compose -f docker-compose.fly.yml run --rm producer"
```

Re-running this later replays the same fixed dataset again — see PLANNING.md AD-35 for why that's
safe as a single clean run but *not* safe to put on an unattended schedule without first also
resetting `raw_metrics` and restarting `backend` (the exact Slice 4 buffer-ordering bug this would
otherwise reproduce is documented there).
