# Deploying to a VPS (Slice 6, revised)

See `PLANNING.md` §4 AD-30..AD-35 for the architecture and why (a plain VPS after an earlier Fly.io
attempt was found unstable and reverted — §7 has the full story). This file is the command sequence.

## One-time server setup

Provisioned: Hetzner CX22 (2 vCPU/4GB), Docker CE marketplace image, SSH key already on the box.

```bash
ssh root@<vps-ip>          # confirm Docker is present (marketplace image ships it)
docker version
git clone https://github.com/chaiwat123431/monitoring-failure-prediction.git
cd monitoring-failure-prediction
```

## `.env.prod` — created on the server itself, never transmitted elsewhere

```bash
cat > .env.prod <<EOF
POSTGRES_PASSWORD=$(openssl rand -base64 24)
PUBLIC_HOSTNAME=<vps-ip-with-dots>.sslip.io
EOF
```

`PUBLIC_HOSTNAME` uses [sslip.io](https://sslip.io) — a public DNS service that resolves
`<ip>.sslip.io` to `<ip>` with no domain purchase or DNS setup needed (confirmed by resolving it
directly with `dig` before relying on it here). Let's Encrypt (Caddy's automatic HTTPS) only needs a
real, resolvable hostname — it doesn't care that this one is a third party's wildcard rather than a
domain the project owns. Point a real domain here instead later by changing this one value.

## Deploy

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod up -d --build
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod ps
```

## Verify

```bash
curl -sf https://$PUBLIC_HOSTNAME/health
curl -sf https://$PUBLIC_HOSTNAME/api/series
```

Open `https://<vps-ip>.sslip.io` in a browser. `raw_metrics` is empty and no model exists yet (only
`migrate` has run) — the chart shows "Waiting for data…" and the "no model loaded" banner until the
next section.

## Seed data and train a model

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  run --rm producer

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  run --rm backend python scripts/train.py

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  restart backend   # picks up the freshly-trained model (AD-24 — no model at startup is handled,
                     # not hot-reloaded, so a restart is how it notices a new file)
```

`scripts/train.py` and `docker compose run --rm backend ...` both reuse the `backend` service's own
image/env/volumes (including the `./models:/app/models` mount, AD-31) — no separate training service
needed, unlike the abandoned Fly version (which needed one only because of Fly's per-process volume
scoping, not a general requirement).

Re-running the producer later replays the same fixed dataset again — see `PLANNING.md` AD-35 for why
that's safe as a single clean run but *not* safe to put on an unattended schedule without first also
resetting `raw_metrics` and restarting `backend` (the Slice 4 buffer-ordering bug this would otherwise
reproduce is documented there).
