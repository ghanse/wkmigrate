# ---------- build stage ----------
FROM python:3.12-slim AS builder

# Avoid interactive prompts from apt
ENV DEBIAN_FRONTEND=noninteractive

RUN pip install --no-cache-dir poetry==2.2.1

WORKDIR /app

# Copy only dependency manifests first so Docker can cache the install layer
COPY pyproject.toml poetry.lock poetry.toml ./

# Install runtime dependencies only (no dev group)
RUN poetry config virtualenvs.create true --local \
    && poetry config virtualenvs.in-project true --local \
    && poetry install --only main --no-root --no-interaction

# Copy the rest of the source code and install the project itself
COPY src/ src/
COPY README.md ./
RUN poetry install --only main --no-interaction

# ---------- runtime stage ----------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy the virtual-env and source from the builder
COPY --from=builder /app/.venv .venv
COPY --from=builder /app/src src
COPY --from=builder /app/pyproject.toml .
COPY --from=builder /app/README.md .

# Put the virtual-env on the PATH so `wkmigrate` and `python` resolve there
ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV="/app/.venv"

# Smoke-test: make sure the CLI entry-point is importable
RUN python -c "import wkmigrate"

# Default command – users can override with their own script / arguments
ENTRYPOINT ["python"]
CMD ["-m", "wkmigrate"]
