.PHONY: dev test fmt docs docs-serve docs-clean

dev:
	poetry install

test:
	poetry run pytest

fmt:
	poetry run black .
	poetry run ruff check . --fix
	poetry run mypy .
	poetry run pylint --output-format=colorized -j 0 src tests

docs:
	# Regenerate API reference under docs/wkmigrate/docs using pydoc-markdown
	poetry run pydoc-markdown
	# Install yarn dependencies
	yarn --cwd docs/wkmigrate install
	# Build the static Docusaurus site
	yarn --cwd docs/wkmigrate build

docs-serve:
	# Always rebuild API docs and the static site before serving
	poetry run pydoc-markdown
	yarn --cwd docs/wkmigrate build
	yarn --cwd docs/wkmigrate serve

docs-clean:
	rm -rf docs/wkmigrate/build
	rm -rf docs/wkmigrate/.docusaurus docs/wkmigrate/.cache
	# Remove generated API docs but keep the hand-authored index.mdx
	find docs/wkmigrate/docs/reference/api -mindepth 1 -not -name 'index.mdx' -exec rm -rf {} +