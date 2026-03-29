# ---------- build stage ----------
FROM python:3.12.10-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.8.4 /uv /usr/local/bin/uv

WORKDIR /app

# Copy only dependency manifests first so Docker can cache the install layer
COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-editable

COPY src/ src/
COPY README.md ./

RUN uv sync --frozen --no-dev

# ---------- runtime stage ----------
FROM python:3.12.10-slim AS runtime

# Create a non-root user for runtime security
RUN useradd --create-home appuser

WORKDIR /app

# Copy the virtual-env and source from the builder
COPY --from=builder --chown=appuser:appuser /app/.venv .venv
COPY --from=builder --chown=appuser:appuser /app/src src
COPY --from=builder --chown=appuser:appuser /app/pyproject.toml .
COPY --from=builder --chown=appuser:appuser /app/README.md .

# Put the virtual-env on the PATH so `wkmigrate` and `python` resolve there
ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV="/app/.venv"

# Fail the build early if the package wasn't installed correctly
RUN python -c "import wkmigrate"

# Ensure the output directory exists and is writable by the non-root user
RUN mkdir -p /app/output && chown appuser:appuser /app/output

USER appuser

ENTRYPOINT ["wkmigrate"]
CMD []
