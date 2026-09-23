# Deploying to a VPS (Slice 6, revised)

See `PLANNING.md` §4 AD-30..AD-35 for the architecture and why (a plain VPS after an earlier Fly.io
attempt was found unstable and reverted — §7 has the full story). This file is the command sequence.

## One-time server setup

Provisioned: Hetzner CX22 (2 vCPU/4GB), Docker CE marketplace image, SSH key already on the box.
Confirmed directly against this project's own real server before this file was written: Docker CE
27.5.1 / Compose v2.32.4 (which does support the `!override` merge tag `docker-compose.prod.yml`
relies on — checked with a throwaway compose file, not assumed from the version number alone), Ubuntu
24.04.

```bash
ssh root@<vps-ip>
docker version
```

The repo is private, so a plain `git clone https://...` on the server has no credentials to use it
with. Rather than provision a GitHub deploy key on a demo box for a one-time copy, push the working
tree directly from a machine that already has `git`/`gh` access:

```bash
# from your own machine, inside the repo, on the commit you want deployed
git archive --format=tar HEAD | ssh root@<vps-ip> 'mkdir -p monitoring-failure-prediction && tar -x -C monitoring-failure-prediction'
ssh root@<vps-ip> 'cd monitoring-failure-prediction && ls'
```

Re-run the `git archive | ssh ... tar -x` line to update the server to a newer commit later.

## `.env.prod` — created on the server itself, never transmitted elsewhere

```bash
cd monitoring-failure-prediction
cat > .env.prod <<EOF
POSTGRES_PASSWORD=$(openssl rand -base64 24)
PUBLIC_HOSTNAME=<vps-ip-with-dots>.sslip.io
EOF
```

`.env.prod` is gitignored (confirmed: it's a literal entry, not just the broader `.env` pattern) —
`git status` inside this checkout should never show it as trackable.

`models/` is also gitignored (AD-19) and so doesn't exist yet in a fresh checkout — Docker
auto-creates it, **root-owned**, the first time it's bind-mounted, but the backend/train containers
run as uid 1000 (`backend/Dockerfile`), so `scripts/train.py` fails with `PermissionError` until this
is fixed once:

```bash
mkdir -p models && chown 1000:1000 models
```

`PUBLIC_HOSTNAME` uses [sslip.io](https://sslip.io) — a public DNS service that resolves
`<ip>.sslip.io` to `<ip>` with no domain purchase or DNS setup needed (confirmed by resolving it
directly with `dig` before relying on it here). Let's Encrypt (Caddy's automatic HTTPS) only needs a
real, resolvable hostname — it doesn't care that this one is a third party's wildcard rather than a
domain the project owns. Point a real domain here instead later by changing this one value.

**Export the same values into the shell for every command below** (compose's `--env-file` only feeds
the *containers*; commands like the `curl` checks further down need the value in your own shell too):

```bash
set -a; source .env.prod; set +a
```

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

Open `https://$PUBLIC_HOSTNAME` in a browser. `raw_metrics` is empty and no model exists yet (only
`migrate` has run) — the chart shows "Waiting for data…" and the "no model loaded" banner until the
next section.

## Seed data and train a model

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  run --rm producer

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  run --rm train

docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod \
  restart backend   # picks up the freshly-trained model (AD-24 — no model at startup is handled,
                     # not hot-reloaded, so a restart is how it notices a new file)
```

`train` is a small dedicated one-shot service (`docker-compose.prod.yml`, tagged with a Compose
`profile` so it never starts on a plain `up -d`) rather than reusing `backend`'s own service
definition for this — `backend` only ever needs *read-only* access to `/app/models` (it just loads
the file at startup, AD-24), and giving the persistent, internet-facing `backend` container
unnecessary write access to it for the sake of one occasional training command would widen its blast
radius for no benefit (`/code-review high` on PR #7).

Re-running the producer later replays the same fixed dataset again — see `PLANNING.md` AD-35 for why
that's safe as a single clean run but *not* safe to put on an unattended schedule without first also
resetting `raw_metrics` and restarting `backend` (the Slice 4 buffer-ordering bug this would otherwise
reproduce is documented there).
