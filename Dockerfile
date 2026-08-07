ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}  AS builder

# Set build labels
LABEL stage=builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Configure uv-managed virtual environment
ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /tmp/app

# Copy project metadata and dependencies
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --extra server --no-install-project

# Copy and install runtime source code to the builder image
COPY async_titiler/ async_titiler/
RUN uv pip install --no-deps .

# Runtime stage
FROM python:${PYTHON_VERSION}-slim

# Set runtime labels
LABEL org.opencontainers.image.source="https://github.com/developmentseed/async-titiler"
LABEL org.opencontainers.image.description="Async TiTiler"
LABEL org.opencontainers.image.licenses="MIT"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/opt/venv/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libexpat1 fonts-dejavu \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv

RUN groupadd -g 1000 user && \
    useradd -u 1000 -g user -s /bin/bash -m user

USER user