FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /usr/local/bin/

RUN groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

FROM base AS dev

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --group dev

COPY . .
RUN chown -R app:app /app
USER app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# PLANNING.md Slice 6 AD-32: sibling to `dev`, not a shared stage with flags — no dev dependency
# group (pytest/httpx have no business in a runtime image), no --reload (no source bind-mount in
# prod, nothing to reload), and deliberately no --workers: a second worker would run its own
# independent live-feed Kafka consumer + buffer-seeding queries (AD-22) for zero benefit at this
# project's single-user scale (PLANNING §2).
FROM base AS prod

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen

COPY . .

# /code-review high fix (Slice 6 PR #6): the deployed Fly Machine has no host-side `./data` bind
# mount (data/ is gitignored, AD-14) — bake the same 2 NAB CSVs scripts/fetch_nab_data.sh fetches
# locally directly into the image, at the exact path (`/data`) the producer already defaults to, so
# no env var or compose change is needed on top of this. This is a *build-time* fetch only — AD-14's
# "the producer container itself needs no internet access" still holds at runtime, unchanged.
RUN mkdir -p /data/realAWSCloudwatch && \
    python -c "import urllib.request; urllib.request.urlretrieve('https://raw.githubusercontent.com/numenta/NAB/master/data/realAWSCloudwatch/ec2_cpu_utilization_825cc2.csv', '/data/realAWSCloudwatch/ec2_cpu_utilization_825cc2.csv')" && \
    python -c "import urllib.request; urllib.request.urlretrieve('https://raw.githubusercontent.com/numenta/NAB/master/data/realAWSCloudwatch/rds_cpu_utilization_cc0c53.csv', '/data/realAWSCloudwatch/rds_cpu_utilization_cc0c53.csv')"

RUN chown -R app:app /app /data
USER app

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
