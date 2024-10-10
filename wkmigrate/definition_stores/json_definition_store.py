""" This module defines the ``JSONDefinitionStore`` class."""
import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional
from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.enums.json_source_type import JSONSourceType
from wkmigrate.pipeline_translators.pipeline_translator import translate_pipeline


@dataclass
class JSONDefinitionStore(DefinitionStore):
    """This class is used to create a target JSON file."""
    json_file_path: str
    json_source_type: Optional[JSONSourceType] = JSONSourceType.DATA_FACTORY_PIPELINE
    _pipeline_getter: Optional[callable] = field(init=False)

    def __post_init__(self) -> None:
        """ Validates the provided file path and sets the correct pipeline getter."""
        if not os.path.isfile(self.json_file_path):
            raise ValueError(f'No file found at path {self.json_file_path}')
        if self.json_source_type == JSONSourceType.DATABRICKS_WORKFLOW:
            self._pipeline_getter = JSONDefinitionStore._databricks_workspace_pipeline_getter
            return
        self._pipeline_getter = JSONDefinitionStore._factory_pipeline_getter

    def load(self, pipeline_name: str) -> dict:
        """ Gets a dictionary representation of a Databricks workflow from the JSON file.
            :parameter pipeline_name: Name of the source pipeline as a ``str``
            :return: ``dict`` representation of the Databricks workflow
        """
        pipeline = self._get_pipeline(pipeline_name)
        if self.json_source_type == JSONSourceType.DATA_FACTORY_PIPELINE:
            return translate_pipeline(pipeline)
        return pipeline

    def dump(self, job_definition: dict) -> None:
        """ Writes a workflow definition to the JSON file following the Databricks SDK ``Job`` object definition.
            :parameter job_definition: Databricks workflow definition as a ``dict``
            :return: ``None``
        """
        self._write_file(job_definition)

    def _read_file(self) -> Any:
        """ Reads the JSONDefinitionStore file into an object.
            :return: Parsed JSON object.
        """
        with open(self.json_file_path, 'r') as json_file:
            return json.load(json_file)

    def _write_file(self, data: dict) -> None:
        """ Writes an object to the JSONDefinitionStore file.
            :parameter data: Object to write to JSON as a ``dict``
        """
        with open(self.json_file_path, 'w') as json_file:
            return json.dump(data, json_file, sort_keys=False)

    def _get_pipeline(self, pipeline_name: str) -> dict:
        """ Gets a pipeline definition with the provided pipeline name from the JSONDefinitionStore file.
            :parameter pipeline_name: Pipeline name as a ``str``
            :return: Pipeline definition as a ``dict``
        """
        source = self._read_file()
        if not isinstance(source, list):
            return source
        return self._pipeline_getter(source, pipeline_name)

    @staticmethod
    def _factory_pipeline_getter(pipelines: list[dict], pipeline_name: str) -> dict:
        """ Gets a pipeline definition from a list of definitions in Data Factory's object model.
            :parameter pipelines: Pipeline definitions as a ``list[dict]``
            :parameter pipeline_name: Pipeline name as a ``str``
            :return: Pipeline definition as a ``dict``
        """
        for pipeline in pipelines:
            if 'name' not in pipeline:
                continue
            if pipeline.get('name') == pipeline_name:
                return pipeline
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')

    @staticmethod
    def _databricks_workspace_pipeline_getter(pipelines: list[dict], pipeline_name) -> dict:
        """ Gets a pipeline definition from a list of definitions in the Databricks SDK object model.
            :parameter pipelines: Pipeline definitions as a ``list[dict]``
            :parameter pipeline_name: Pipeline name as a ``str``
            :return: Pipeline definition as a ``dict``
        """
        for pipeline in pipelines:
            if 'settings' not in pipeline:
                continue
            settings = pipeline.get('settings')
            if 'name' not in settings:
                continue
            if settings.get('name') == pipeline_name:
                return pipeline
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')
