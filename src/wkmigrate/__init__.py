"""Shared paths and constants used throughout the wkmigrate package."""

import os

_project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

JSON_PATH = os.path.join(_project_root, "tests", "resources", "json")

YAML_PATH = os.path.join(_project_root, "tests", "resources", "yaml")
