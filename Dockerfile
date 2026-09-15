FROM cgr.dev/chainguard/wolfi-base:latest@sha256:3754b6da0e1ccdab0fe46abfdd7bbbba994b593c28149f6659fa6597f2261aeb AS base

ARG PYTHON_VERSION=3.14

ENV PYTHONUNBUFFERED=1

# Set build labels
LABEL stage=builder

RUN apk add --no-cache \
    python-${PYTHON_VERSION} \
    libstdc++ bash tzdata curl

# uv as a pass-through stage rather than a direct COPY --from=<image>:
# Dependabot only parses FROM lines (dependabot/dependabot-core#5103), so this
# keeps the version pinned *and* auto-updated by the docker ecosystem.
FROM ghcr.io/astral-sh/uv:0.12.13@sha256:b485bd65cc2cf1c9a93b3554012c9c3778cf7b1b5fd3d3096ce9e1226c97e1e6 AS uv

# Build stage
FROM base AS builder

ARG PYTHON_VERSION

# Install uv
COPY --from=uv /uv /uvx /usr/local/bin/

# Configure uv-managed virtual environment
#   UV_PYTHON: the apk python's version, but resolved by uv (not the apk
#     binary itself: Wolfi's rolling python-3.14 build has repeatedly required
#     a newer glibc than the glibc package available in the same repo snapshot,
#     making /usr/bin/python3.14 unable to import stdlib extension modules).
#     uv instead downloads its own self-contained, statically-linked build.
#   UV_PYTHON_INSTALL_DIR: put that managed interpreter next to /opt/venv (both
#     get copied into the runtime stage below) — otherwise it lands under
#     /root/.local/share/uv, which never reaches the runtime stage, leaving
#     /opt/venv/bin/python (and every venv script's shebang, eg uvicorn) a
#     dangling symlink: `exec .../uvicorn: no such file or directory`.
#   UV_COMPILE_BYTECODE: .pyc at install time, as pip did, so cold starts
#     don't pay the compile on first import
ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON=${PYTHON_VERSION} \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    UV_COMPILE_BYTECODE=1 \
    PATH="/opt/venv/bin:${PATH}"

WORKDIR /tmp/app

# Copy project metadata and dependencies
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --frozen --no-dev --extra server --no-install-project

# Copy and install runtime source code to the builder image
COPY async_titiler/ async_titiler/
RUN uv pip install --no-deps --no-editable .

# uv skips downloading its own managed interpreter when the apk python already
# satisfies UV_PYTHON, leaving UV_PYTHON_INSTALL_DIR absent. Make sure it
# exists (even empty) so the unconditional COPY below never fails.
RUN mkdir -p /opt/uv-python

# Runtime stage
FROM base

# Set runtime labels
LABEL org.opencontainers.image.source="https://github.com/developmentseed/async-titiler"
LABEL org.opencontainers.image.description="Async TiTiler"
LABEL org.opencontainers.image.licenses="MIT"

# The chart execs `command: ["uvicorn"]` — a bare binary, resolved against the
# image's PATH.
ENV PATH="/opt/venv/bin:${PATH}"

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/uv-python /opt/uv-python

WORKDIR /tmp

# Run as the wolfi-base nonroot user (uid 65532). On Kubernetes the chart sets 
# the net.ipv4.ip_unprivileged_port_start sysctl per pod. 
# Other deployments must allow unprivileged low ports the same way,
# or override PORT to something >=1024.
USER nonroot