"""Pipeline IR models."""

from __future__ import annotations

from dataclasses import dataclass, field

from wkmigrate.models.ir.activities import Activity


@dataclass
class PipelineTask:
    """
    Wrapper associating an ``Activity`` with a workflow task slot.

    Attributes:
        activity: Translated activity instance that will be executed as a Databricks task.
    """

    activity: Activity


@dataclass
class Pipeline:
    """
    Pipeline IR object produced by the translator.

    Attributes:
        name: Logical pipeline name derived from the ADF pipeline.
        parameters: List of pipeline parameter definitions, or ``None`` when no parameters are defined.
        schedule: Serialized schedule definition for the pipeline trigger, if any.
        tasks: Ordered list of ``PipelineTask`` wrappers that make up the workflow.
        tags: Dictionary of system and user-defined tags attached to the workflow.
        not_translatable: Collection of warnings describing properties that could not be translated.
    """

    name: str
    parameters: list[dict] | None
    schedule: dict | None
    tasks: list[PipelineTask]
    tags: dict
    not_translatable: list[dict] = field(default_factory=list)
