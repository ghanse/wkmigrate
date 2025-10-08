"""This module defines the ``YAMLDefinitionStore`` class."""

import os
from dataclasses import dataclass
from typing import Any

import yaml
from wkmigrate.definition_stores.definition_store import DefinitionStore


@dataclass
class YAMLDefinitionStore(DefinitionStore):
    """This class is used to create a target YAML file."""

    yaml_file_path: str

    def __post_init__(self):
        """Validates the provided file path."""
        if not os.path.isfile(self.yaml_file_path):
            raise ValueError(f"No file found at path {self.yaml_file_path}")

    def load(self, workflow_name: str) -> dict:
        """Gets a dictionary representation of a Databricks workflow from the YAML file.
        :parameter workflow_name: Databricks workflow name as a ``str``
        :return: ``dict`` representation of the Databricks workflow
        """
        return self._get_workflow(workflow_name)

    def dump(self, job_definition: dict) -> None:
        """Writes a workflow definition to the YAML file following Databricks Asset Bundles' YAML specification.
        :parameter job_definition: Databricks workflow as a ``dict``
        :return: ``None``
        """
        self._write_file(job_definition)

    def _read_file(self) -> Any:
        """Reads the YAMLDefinitionStore file into an object.
        :return: Parsed YAML object.
        """
        with open(self.yaml_file_path, "rb") as yaml_file:
            return yaml.safe_load(yaml_file)

    def _write_file(self, data: dict) -> None:
        """Writes an object to the YAMLDefinitionStore file.
        :parameter data: Object to write to YAML as a ``dict``
        """
        with open(self.yaml_file_path, "wb") as yaml_file:
            yaml.dump(data, yaml_file, sort_keys=False)

    def _get_workflow(self, workflow_name: str) -> dict:
        """Gets a workflow definition with the provided workflow name from the YAMLDefinitionStore file.
        :parameter workflow_name: Workflow name as a ``str``
        :return: Workflow definition as a ``dict``
        """
        source = self._read_file()
        if "resources" not in source:
            raise ValueError('No "resources" defined in source YAML file')
        resources = source.get("resources")
        if "jobs" not in resources:
            raise ValueError('No "jobs" defined in source YAML file')
        jobs = resources.get("jobs")
        if workflow_name not in jobs:
            raise ValueError(f'No workflow found with name "{workflow_name}"')
        return jobs.get(workflow_name)
