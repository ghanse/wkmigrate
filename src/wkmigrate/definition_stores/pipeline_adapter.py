"""Adapter for raw ADF pipeline dictionaries. Appends dataset and linked-service
metadata.

``PipelineEnricher`` attaches resolved dataset and linked-service definitions
to activity dicts so that translators have the full context they need. It is
injected into concrete definition stores via composition and does not perform
translation — that responsibility stays with the store.

All public methods are pure: they return new dicts rather than mutating inputs.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from wkmigrate.enums.source_property_case import SourcePropertyCase
from wkmigrate.utils import normalize_arm_pipeline, recursive_camel_to_snake

logger = logging.getLogger(__name__)

DatasetGetter = Callable[[str], dict]
LinkedServiceGetter = Callable[[str], dict]
PipelineGetter = Callable[[str], dict]


@dataclass(slots=True, frozen=True)
class PipelineAdapter:
    """
    Adapts dictionaries from ADF pipelines to include referenced dataset and linked-service metadata. Returns a plain
    dict ready for ``translate_pipeline``. Lookup callables are injected at construction time.

    Attributes:
        get_dataset: Returns a dataset dict given a dataset name.
        get_linked_service: Returns a linked-service dict given a name.
        get_pipeline: Optional callable that returns a pipeline dict given a pipeline name.
            When provided, Execute Pipeline activities are enriched with the child pipeline definition.
        source_property_case: Property casing of the source data. When ``CAMEL``,
            fetched dicts are normalized to snake_case.
    """

    get_dataset: DatasetGetter
    get_linked_service: LinkedServiceGetter
    get_pipeline: PipelineGetter | None = None
    source_property_case: SourcePropertyCase = SourcePropertyCase.SNAKE
    _normalized_cache: dict[tuple[str, str], dict] = field(init=False, default_factory=dict, hash=False, compare=False)

    def adapt(self, pipeline: dict, trigger: dict | None = None) -> dict:
        """
        Returns a new pipeline dictionary with dataset and linked-service metadata.

        Args:
            pipeline: Raw pipeline definition. Not mutated.
            trigger: Trigger definition for the pipeline, or ``None``.

        Returns:
            New dict containing the original pipeline fields plus enriched
            activities and the (optionally normalized) trigger.
        """
        activities = pipeline.get("activities") or []
        enriched_activities = [self._enrich_activity(a) for a in activities]
        enriched_trigger = self.normalize_casing(trigger) if trigger else None
        return {**pipeline, "activities": enriched_activities, "trigger": enriched_trigger}

    def normalize_casing(self, source: dict | None, cache_key: tuple[str, str] | None = None) -> dict | None:
        """
        Normalizes a dictionary to use the proper casing. Optionally caches the dictionary when a cache key is provided.

        Args:
            source: Source dictionary.
            cache_key: Optional key for caching the normalized output.

        Returns:
            Normalized dictionary with the proper casing.
        """
        if source is None:
            return None
        if self.source_property_case != SourcePropertyCase.CAMEL:
            return source
        if cache_key is not None and cache_key in self._normalized_cache:
            return self._normalized_cache[cache_key]
        out = recursive_camel_to_snake(source)
        if cache_key is not None:
            self._normalized_cache[cache_key] = out
        return out

    def _enrich_activity(self, activity: dict) -> dict:
        """
        Returns an activity dict enriched with datasets, linked services, and child pipelines.

        Args:
            activity: Input activity as a dictionary.

        Returns:
            Activity with dataset, linked service, and child pipeline metadata.
        """
        result = self._enrich_datasets(activity)
        result = self._enrich_linked_service(result)
        result = self._enrich_execute_pipeline(result)
        return result

    def _enrich_datasets(self, activity: dict) -> dict:
        """
        Returns an activity dictionary with input and output dataset definitions populated.

        Args:
            activity: Input activity as a dictionary.

        Returns:
            Activity with input and output dataset metadata.
        """
        additions: dict = {}

        input_datasets = activity.get("inputs")
        if input_datasets is not None:
            names = [dataset.get("reference_name") for dataset in input_datasets if isinstance(dataset, dict)]
            additions["input_dataset_definitions"] = [
                self.normalize_casing(self.get_dataset(name), ("dataset", name)) for name in names if name
            ]

        output_datasets = activity.get("outputs")
        if output_datasets is not None:
            names = [dataset.get("reference_name") for dataset in output_datasets if isinstance(dataset, dict)]
            additions["output_dataset_definitions"] = [
                self.normalize_casing(self.get_dataset(name), ("dataset", name)) for name in names if name
            ]

        dataset_reference = activity.get("dataset")
        if dataset_reference is not None:
            name = dataset_reference.get("reference_name")
            if name:
                additions["input_dataset_definitions"] = [
                    self.normalize_casing(self.get_dataset(name), ("dataset", name))
                ]

        if not additions:
            return activity
        return {**activity, **additions}

    def _enrich_execute_pipeline(self, activity: dict) -> dict:
        """
        Returns an activity dictionary enriched with the child pipeline definition for Execute Pipeline activities.

        When a ``get_pipeline`` callable is available and the activity type is
        ``ExecutePipeline``, the referenced child pipeline is fetched, normalized,
        enriched (recursively), and embedded under the ``pipeline_definition`` key.

        Args:
            activity: Input activity as a dictionary.

        Returns:
            Activity with child pipeline metadata when applicable, otherwise unchanged.
        """
        if self.get_pipeline is None:
            return activity

        activity_type = activity.get("type")
        if activity_type != "ExecutePipeline":
            return activity

        pipeline_ref = activity.get("pipeline")
        if not isinstance(pipeline_ref, dict):
            return activity

        pipeline_name = pipeline_ref.get("reference_name")
        if not pipeline_name:
            return activity

        try:
            raw_pipeline = self.get_pipeline(pipeline_name)
            normalized = self.normalize_casing(raw_pipeline, ("pipeline", pipeline_name))
            if normalized is not None:
                raw_pipeline = normalized
            enriched = self._adapt_child_pipeline(raw_pipeline)
            return {**activity, "pipeline_definition": enriched}
        except (ValueError, KeyError):
            logger.warning(
                f"Child pipeline '{pipeline_name}' not found; Execute Pipeline activity will use a placeholder."
            )
            return activity

    def _adapt_child_pipeline(self, pipeline: dict) -> dict:
        """
        Enriches a child pipeline dict with datasets and linked services.

        Unlike ``adapt`` this skips trigger resolution since child pipelines
        invoked by Execute Pipeline do not carry their own triggers.
        ``normalize_arm_pipeline`` is applied first to flatten any ``properties``
        wrapper and merge ``typeProperties`` into activity dicts.

        Args:
            pipeline: Raw child pipeline definition.

        Returns:
            Enriched pipeline dict ready for translation.
        """
        pipeline = normalize_arm_pipeline(pipeline)
        activities = pipeline.get("activities") or []
        enriched_activities = [self._enrich_activity(a) for a in activities]
        return {**pipeline, "activities": enriched_activities, "trigger": None}

    def _enrich_linked_service(self, activity: dict) -> dict:
        """
        Returns an activity dictionary with linked-service definitions and enriched nested activities.

        Args:
            activity: Input activity as a dictionary.

        Returns:
            Activity with linked service metadata.
        """
        additions: dict = {}

        linked_service_reference = activity.get("linked_service_name")
        if linked_service_reference is not None:
            linked_service_name = linked_service_reference.get("reference_name")
            if linked_service_name:
                try:
                    raw = self.get_linked_service(linked_service_name)
                    additions["linked_service_definition"] = self.normalize_casing(
                        raw, ("linked_service", linked_service_name)
                    )
                except ValueError:
                    logger.warning(
                        f"Linked service '{linked_service_name}' not found; skipping cluster spec for activity."
                    )

        for branch_key in ("if_false_activities", "if_true_activities", "activities"):
            branch = activity.get(branch_key)
            if branch is not None:
                additions[branch_key] = [self._enrich_linked_service(branch_activity) for branch_activity in branch]

        if not additions:
            return activity
        return {**activity, **additions}
