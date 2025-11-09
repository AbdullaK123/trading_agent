FROM python:3.11-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency files first (for Docker cache)
COPY pyproject.toml uv.lock ./

# Ensure uv uses a CPython version that matches the lockfile
ENV UV_PYTHON_VERSION=3.11

# Install dependencies
RUN uv sync

# Copy the rest of your code
COPY . .

CMD ["uv", "run", "src/main.py"]