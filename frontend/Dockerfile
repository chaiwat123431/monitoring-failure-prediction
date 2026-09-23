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
ENV NODE_ENV=production PORT=3000

# .next/standalone already includes a pruned node_modules for exactly what the server needs;
# .next/static is not part of it and must be copied alongside it (Next.js's own documented
# standalone-output layout). No public/ directory exists in this project yet — add a COPY line for
# it here if one is introduced later.
COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static

USER node
EXPOSE 3000
CMD ["node", "server.js"]
