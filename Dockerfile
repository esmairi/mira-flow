# syntax=docker/dockerfile:1

# ---- dev: full toolchain, bind-mounted repo via compose.yaml (target: dev) ----
# Not a production build. Meant to be run as the `app` service in compose.yaml
# with the repo bind-mounted, so edits on the host show up without a rebuild.
# Also sidesteps building confluent-kafka on musl/Alpine hosts: this image is
# glibc (Debian), where prebuilt wheels apply.
FROM python:3.13-slim AS dev

# librdkafka + a compiler, in case a platform ever lacks a confluent-kafka wheel.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /usr/local/bin/

# Keep the venv outside /app so the dev bind mount (`.:/app`) can't shadow it.
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# No deps-only caching layer here: this is now a uv workspace (root +
# packages/core), so resolution needs every member's pyproject.toml present
# regardless — copying everything up front is simpler and correct, at the
# cost of that cache layer.
COPY . .
RUN uv sync --group dev

CMD ["bash"]

# ---- builder: production deps only, into /opt/venv ----
FROM python:3.13-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /uvx /usr/local/bin/

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

# Which packages/ledger DB driver extra(s) to install, e.g. "postgres" —
# see packages/ledger/pyproject.toml's `optional-dependencies`. Empty by
# default (sqlite only, stdlib driver, nothing extra to install); compose.yaml
# passes `--build-arg DB_EXTRA=postgres` for the `*-postgres` services, so
# only images that actually need a given driver carry its dependencies —
# unconditionally installing every driver into every image stops scaling the
# moment a third database is added.
ARG DB_EXTRA=""

WORKDIR /app
COPY . .
# --frozen: fail loud if uv.lock is stale rather than silently re-resolving.
# --no-dev: uv includes the "dev" dependency-group by default, so this must
# be explicit — otherwise pytest/ruff/testcontainers end up in the prod image.
# ${DB_EXTRA:+--extra "$DB_EXTRA"}: shell parameter expansion — adds
# `--extra postgres` only when DB_EXTRA is set to something, so the plain
# sqlite build stays driver-free.
RUN uv sync --frozen --no-dev ${DB_EXTRA:+--extra "$DB_EXTRA"}

# ---- prod: slim runtime, no compiler, non-root, execs mira-flow as PID 1 ----
# `mira-flow run` must be the container's actual entrypoint (exec form, no
# shell wrapper) so a Kubernetes/`docker stop` SIGTERM reaches it directly —
# see cli.py's SIGTERM handler, which this depends on to shut down gracefully
# instead of waiting out the full grace period for a SIGKILL.
FROM python:3.13-slim AS prod

RUN useradd --create-home --uid 1000 mira

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --chown=mira:mira . .

USER mira

# ENTRYPOINT/CMD split (not one CMD): lets a deployment override just the
# subcommand/flags — e.g. `command: ["run", "--workflows-dir", "/workflows"]`
# in compose.yaml, or a Kubernetes `args:` override — without repeating
# "mira-flow" or losing the exec-form PID 1 guarantee above.
ENTRYPOINT ["mira-flow"]
CMD ["run"]
