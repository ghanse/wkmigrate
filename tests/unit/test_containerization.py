"""Tests for containerization artifacts (Dockerfile, .dockerignore, docker-compose.yml)."""

from __future__ import annotations

import os
import re

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def _read(filename: str) -> str:
    """Read a file from the project root."""
    with open(os.path.join(PROJECT_ROOT, filename), encoding="utf-8") as fh:
        return fh.read()


class TestDockerfile:
    """Validate that the Dockerfile follows expected conventions."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.content = _read("Dockerfile")

    def test_dockerfile_exists(self) -> None:
        assert os.path.isfile(os.path.join(PROJECT_ROOT, "Dockerfile"))

    def test_uses_python_312_base(self) -> None:
        assert "python:3.12" in self.content

    def test_multi_stage_build(self) -> None:
        assert self.content.count("FROM ") >= 2, "Expected a multi-stage build"

    def test_installs_poetry(self) -> None:
        assert "poetry" in self.content

    def test_copies_dependency_manifests_before_source(self) -> None:
        manifest_pos = self.content.index("COPY pyproject.toml")
        source_pos = self.content.index("COPY src/")
        assert manifest_pos < source_pos, "Dependency manifests should be copied before source for better layer caching"

    def test_sets_virtualenv_on_path(self) -> None:
        assert ".venv" in self.content
        assert "PATH" in self.content

    def test_no_dev_dependencies_in_runtime(self) -> None:
        assert "--only main" in self.content

    def test_runs_as_non_root_user(self) -> None:
        assert "useradd" in self.content, "Expected a non-root user to be created"
        assert "USER appuser" in self.content, "Expected USER directive for non-root execution"

    def test_entrypoint_uses_cli_script(self) -> None:
        assert 'ENTRYPOINT ["wkmigrate"]' in self.content, "Entrypoint should use the CLI script entry point"

    def test_output_directory_created_with_correct_ownership(self) -> None:
        assert "mkdir -p /app/output" in self.content, "Output directory should be created before USER directive"
        assert "chown appuser:appuser /app/output" in self.content, "Output directory should be owned by appuser"


class TestDockerignore:
    """Validate that the .dockerignore excludes non-essential paths."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.content = _read(".dockerignore")

    def test_dockerignore_exists(self) -> None:
        assert os.path.isfile(os.path.join(PROJECT_ROOT, ".dockerignore"))

    def test_excludes_git(self) -> None:
        assert ".git" in self.content

    def test_excludes_venv(self) -> None:
        assert ".venv" in self.content

    def test_excludes_tests(self) -> None:
        assert "tests" in self.content

    def test_excludes_docs(self) -> None:
        assert "docs" in self.content


class TestDockerCompose:
    """Validate the docker-compose.yml structure."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.content = _read("docker-compose.yml")

    def test_compose_file_exists(self) -> None:
        assert os.path.isfile(os.path.join(PROJECT_ROOT, "docker-compose.yml"))

    def test_defines_wkmigrate_service(self) -> None:
        assert "wkmigrate" in self.content

    def test_references_dockerfile(self) -> None:
        assert "Dockerfile" in self.content

    def test_uses_env_file(self) -> None:
        assert "env_file" in self.content or ".env" in self.content

    def test_mounts_output_volume(self) -> None:
        assert "output" in self.content

    def test_no_fixed_container_name(self) -> None:
        assert (
            "container_name" not in self.content
        ), "Avoid fixed container_name to prevent collisions and allow scaling"

    def test_env_file_not_required(self) -> None:
        assert "required: false" in self.content, "env_file should use required: false so compose works without .env"

    def test_has_restart_policy(self) -> None:
        assert "restart:" in self.content, "Expected an explicit restart policy"


class TestEnvExample:
    """Validate that .env.example exists to document required environment variables."""

    def test_env_example_exists(self) -> None:
        assert os.path.isfile(os.path.join(PROJECT_ROOT, ".env.example"))

    def test_env_example_has_content(self) -> None:
        content = _read(".env.example")
        assert content.strip(), ".env.example should not be empty"


class TestMakefile:
    """Validate that the Makefile includes a docker target."""

    @pytest.fixture(autouse=True)
    def _load(self) -> None:
        self.content = _read("Makefile")

    def test_has_docker_target(self) -> None:
        assert re.search(r"^docker:", self.content, re.MULTILINE)

    def test_docker_target_builds_image(self) -> None:
        assert "docker build" in self.content
