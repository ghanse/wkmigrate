# ---------- build stage ----------
FROM python:3.12-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN pip install --no-cache-dir poetry==2.2.1

WORKDIR /app

# Copy only dependency manifests first so Docker can cache the install layer
COPY pyproject.toml poetry.lock poetry.toml ./

# Install runtime dependencies only (no dev group)
RUN poetry config virtualenvs.create true --local \
    && poetry config virtualenvs.in-project true --local \
    && poetry install --only main --no-root --no-interaction

COPY src/ src/
COPY README.md ./
RUN poetry install --only main --no-interaction

# ---------- runtime stage ----------
FROM python:3.12-slim AS runtime

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
