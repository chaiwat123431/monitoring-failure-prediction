FROM node:22-slim AS dev

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN chown -R node:node /app
USER node

EXPOSE 3000
CMD ["npm", "run", "dev"]

# PLANNING.md Slice 6 AD-32: real production build, not `next dev` — deps/build/runner so the final
# image ships only the standalone server output (next.config.ts's `output: "standalone"`), not the
# full source tree + devDependencies.
FROM node:22-slim AS build

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

# NEXT_PUBLIC_API_URL must be present at `next build` time, not container-start time: Next.js bakes
# every NEXT_PUBLIC_* value into the client JS bundle during the build — setting it only as a
# runtime `environment:` value (as the dev compose service does, AD-7) would ship a client bundle
# with an empty/stale API URL. Pass it via `docker build --build-arg NEXT_PUBLIC_API_URL=...`.
ARG NEXT_PUBLIC_API_URL
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL

COPY . .
RUN npm run build

FROM node:22-slim AS prod

WORKDIR /app
# HOSTNAME=0.0.0.0 is required, not cosmetic: Docker sets a HOSTNAME env var in every container to
# the container's own id/hostname, and Next.js's standalone server.js binds to `process.env.HOSTNAME`
# when present — without this override it silently listens only on the container's bridge IP, not on
# 127.0.0.1, which made this healthcheck (and any `localhost`-based check run *inside* the container)
# fail with ECONNREFUSED despite the server logging "Ready" (found deploying to a real VPS, where the
# healthcheck runs inside the container's own network namespace — a plain `docker run -p` from the
# host doesn't expose this, since Docker's port-forwarding reaches the container's bridge IP anyway).
ENV NODE_ENV=production PORT=3000 HOSTNAME=0.0.0.0

# .next/standalone already includes a pruned node_modules for exactly what the server needs;
# .next/static is not part of it and must be copied alongside it (Next.js's own documented
# standalone-output layout). No public/ directory exists in this project yet — add a COPY line for
# it here if one is introduced later.
COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static

USER node
EXPOSE 3000
CMD ["node", "server.js"]
