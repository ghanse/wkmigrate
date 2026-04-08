"""Definition store that loads ADF pipeline definitions from local JSON files.

Loads pipeline, trigger, dataset, and linked-service JSON files from
subdirectories of a user-specified source directory and exposes the same
interface as the API-backed store. No Azure credentials required.

Expected directory structure::

    source_directory/
    ├── pipelines/          # Pipeline JSON files
    ├── triggers/           # Trigger JSON files (optional)
    ├── datasets/           # Dataset JSON files (optional)
    └── linked_services/    # Linked-service JSON files (optional)
"""

from __future__ import annotations

import json
import logging
import os
from concurrent import futures
from dataclasses import dataclass, field
from pathlib import Path

from wkmigrate.definition_stores.definition_store import DefinitionStore
from wkmigrate.definition_stores.pipeline_adapter import PipelineAdapter
from wkmigrate.enums.source_property_case import SourcePropertyCase
from wkmigrate.enums.workflow_source_type import WorkflowSourceType
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.translators.pipeline_translators.pipeline_translator import translate_pipeline
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

logger = logging.getLogger(__name__)


def _load_json_list(path: Path) -> list[dict]:
    """Load a JSON file as a list of dicts (or single dict wrapped in a list)."""
    with open(path, "rb") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    return []


def _load_all_json_from_dir(directory: Path) -> list[dict]:
    """Load and merge all JSON files from a directory. Returns empty list if directory does not exist."""
    if not directory.is_dir():
        return []
    result: list[dict] = []
    for path in sorted(directory.glob("*.json")):
        result.extend(_load_json_list(path))
    return result


@dataclass(slots=True)
class JsonDefinitionStore(DefinitionStore):
    """
    Definition store backed by a directory of JSON files.

    Loads pipeline, trigger, dataset, and linked-service definitions from
    subdirectories of ``source_directory``. All loaded data is normalized to
    snake_case at load time when ``source_property_case`` is ``CAMEL``.

    Attributes:
        source_directory: Path to the root directory containing the subdirectories.
        source_property_case: Property casing convention in the source files.
            Defaults to ``SourcePropertyCase.CAMEL`` (portal-exported JSON uses camelCase).
        source_system: The workflow source system type (default: ADF).
    """

    source_directory: str | Path | None = None
    source_property_case: SourcePropertyCase = SourcePropertyCase.CAMEL
    source_system: WorkflowSourceType = WorkflowSourceType.ADF

    _pipelines: list[dict] = field(init=False)
    _triggers: list[dict] = field(init=False)
    _datasets: list[dict] = field(init=False)
    _linked_services: list[dict] = field(init=False)
    _adapter: PipelineAdapter | None = field(init=False)

    def __post_init__(self) -> None:
        if self.source_directory is None:
            raise ValueError("source_directory must be provided")
        base = Path(self.source_directory)
        if not base.is_dir():
            raise ValueError(f"source_directory is not a directory: {base}")

        self._pipelines = _load_all_json_from_dir(base / "pipelines")
        self._triggers = _load_all_json_from_dir(base / "triggers")
        self._datasets = _load_all_json_from_dir(base / "datasets")
        self._linked_services = _load_all_json_from_dir(base / "linked_services")

        if self.source_property_case == SourcePropertyCase.CAMEL:
            self._pipelines = [recursive_camel_to_snake(p) for p in self._pipelines]
            self._triggers = [recursive_camel_to_snake(t) for t in self._triggers]
            self._datasets = [recursive_camel_to_snake(d) for d in self._datasets]
            self._linked_services = [recursive_camel_to_snake(ls) for ls in self._linked_services]

        if not self.list_pipelines():
            raise ValueError(
                f"No pipeline JSON files found in {base / 'pipelines'}. "
                "Add one or more .json files to the pipelines/ subdirectory."
            )

        self._adapter = PipelineAdapter(
            get_dataset=self.get_dataset,
            get_linked_service=self.get_linked_service,
            get_pipeline=self._get_raw_pipeline,
            source_property_case=SourcePropertyCase.SNAKE,  # already normalized
        )

    def list_pipelines(self) -> list[str]:
        """Return the names of all loaded pipelines."""
        pipeline_names = [p.get("name") for p in self._pipelines]
        return [name for name in pipeline_names if isinstance(name, str)]

    def load(self, pipeline_name: str) -> Pipeline:
        """Load, enrich, and translate a pipeline by name.

        Args:
            pipeline_name: Name of the pipeline to load.

        Returns:
            Translated ``Pipeline`` IR.
        """
        if self._adapter is None:
            raise ValueError("Store is not initialized")

        pipeline = normalize_arm_pipeline(dict(self.get_pipeline(pipeline_name)))
        trigger = self.get_trigger(pipeline_name)

        enriched = self._adapter.adapt(pipeline, trigger)
        return translate_pipeline(enriched)

    def load_all(self, pipeline_names: list[str] | None = None) -> list[Pipeline]:
        """Load and translate multiple pipelines. Failures are logged and skipped.

        Args:
            pipeline_names: Names to translate. When ``None``, all pipelines are loaded.

        Returns:
            Translated ``Pipeline`` objects.
        """
        if pipeline_names is None:
            pipeline_names = self.list_pipelines()
        results: list[Pipeline] = []
        with futures.ThreadPoolExecutor(max_workers=_get_worker_count()) as executor:
            tasks = {executor.submit(self.load, name): name for name in pipeline_names}
            for task in futures.as_completed(tasks):
                try:
                    results.append(task.result())
                except Exception as exc:
                    logger.warning(f"Could not load pipeline '{tasks[task]}'", exc_info=exc)
        return results

    def get_pipeline(self, pipeline_name: str) -> dict:
        """Return the pipeline dict for the given name."""
        for pipeline in self._pipelines:
            if pipeline.get("name") == pipeline_name:
                return pipeline
        raise ValueError(f'No pipeline found with name "{pipeline_name}"')

    def get_trigger(self, pipeline_name: str) -> dict | None:
        """Return the trigger for the given pipeline name, or None."""
        for trigger in self._triggers:
            properties = trigger.get("properties")
            if not isinstance(properties, dict):
                continue
            pipelines = properties.get("pipelines") or []
            for pipeline in pipelines:
                pipeline_reference = pipeline.get("pipeline_reference") if isinstance(pipeline, dict) else None
                if not isinstance(pipeline_reference, dict):
                    continue
                if (
                    pipeline_reference.get("type") == "PipelineReference"
                    and pipeline_reference.get("reference_name") == pipeline_name
                ):
                    return trigger
        return None

    def _get_raw_pipeline(self, pipeline_name: str) -> dict:
        """Return a pipeline dict suitable for adapter enrichment.

        ARM normalization is intentionally *not* applied here because
        ``_adapt_child_pipeline`` in the adapter already calls
        ``normalize_arm_pipeline``.  Applying it twice is harmless but
        wasteful and confusing.

        Args:
            pipeline_name: Name of the pipeline to look up.

        Returns:
            Raw pipeline dict.
        """
        return dict(self.get_pipeline(pipeline_name))

    def get_dataset(self, dataset_name: str) -> dict:
        """Return the dataset dict for the given name."""
        for dataset in self._datasets:
            if dataset.get("name") == dataset_name:
                return dataset
        raise ValueError(f'No dataset found with name "{dataset_name}"')

    def get_linked_service(self, linked_service_name: str) -> dict:
        """Return the linked-service dict for the given name."""
        for linked_service in self._linked_services:
            if linked_service.get("name") == linked_service_name:
                return linked_service
        raise ValueError(f'No linked service found with name "{linked_service_name}"')


def _get_worker_count() -> int:
    """Return the number of threadpool workers to use."""
    cpu_count = os.cpu_count()
    return cpu_count * 2 if cpu_count is not None else 1
