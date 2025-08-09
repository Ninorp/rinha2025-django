# Use Python 3.13 slim image as base
FROM ghcr.io/astral-sh/uv:python3.13-slim as builder

# Prevent Python from writing .pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

# Install system dependencies for psycopg (PostgreSQL client libs) and build tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       curl \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/uv \
--mount=type=bind,source=uv.lock,target=uv.lock \
--mount=type=bind,source=pyproject.toml,target=pyproject.toml \
uv sync --locked --no-install-project --no-dev
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Then, use a final image without uv
FROM python:3.13-slim

COPY --from=builder /app /app

# Set workdir
WORKDIR /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Run Django with Uvicorn over Unix Domain Socket for Nginx to proxy
# APP_SOCKET_NAME can be set per-service (e.g., web1.sock, web2.sock)
CMD bash -lc "mkdir -p /sockets && chmod 777 /sockets && uvicorn rinha2025.asgi:application --uds /sockets/${APP_SOCKET_NAME:-web.sock} --workers 1"
