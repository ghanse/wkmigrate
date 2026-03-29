.PHONY: dev test integration fmt docs docs-serve docs-clean docker

clean:
	rm -fr .venv clean .pytest_cache .ruff_cache .coverage coverage.xml
	rm -fr **/*.pyc

dev:
	uv sync

ci:
	uv sync --frozen

test:
	uv run pytest tests/unit

integration:
	uv run pytest -m integration --tb=short -v

fmt:
	uv run black .
	uv run ruff check . --fix
	uv run mypy .
	uv run pylint --output-format=colorized -j 0 src tests

docs:
	uv run pydoc-markdown
	yarn --cwd docs/wkmigrate install
	yarn --cwd docs/wkmigrate build

docs-serve:
	uv run pydoc-markdown
	yarn --cwd docs/wkmigrate build
	yarn --cwd docs/wkmigrate serve

docs-clean:
	rm -rf docs/wkmigrate/build
	rm -rf docs/wkmigrate/.docusaurus docs/wkmigrate/.cache
	find docs/wkmigrate/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +

docker:
	docker build -t wkmigrate:latest .
