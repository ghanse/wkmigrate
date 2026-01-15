"""This module defines a pipeline-level translator from Azure Data Factory to internal IR.

Pipeline translators in this module call activity, parameter, and trigger translators to produce
an internal representation from an input ADF pipeline. They collect ``UnsupportedValue`` objects
and warnings so that callers can surface translation diagnostics alongside the generated payload.
"""

import warnings

from wkmigrate.translators.activity_translators.activity_translator import translate_activities
from wkmigrate.models.ir.pipeline import Pipeline, PipelineTask
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.translators.pipeline_translators.parameter_translator import translate_parameters
from wkmigrate.translators.trigger_translators.schedule_trigger_translator import translate_schedule_trigger
from wkmigrate.utils import append_system_tags


def translate_pipeline(pipeline: dict) -> Pipeline:
    """
    Translates an ADF pipeline dictionary into a ``Pipeline``.

    Args:
        pipeline: Raw pipeline payload exported from ADF.

    Returns:
        Dataclass representation including tasks, schedule, and tags.
    """
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always", UserWarning)
        if "name" not in pipeline:
            warnings.warn(
                NotTranslatableWarning(
                    "pipeline.name", "No pipeline name in source definition, setting to UNNAMED_WORKFLOW"
                ),
                stacklevel=2,
            )
        translated_tasks = translate_activities(pipeline.get("activities"))
        pipeline_ir = Pipeline(
            name=pipeline.get("name", "UNNAMED_WORKFLOW"),
            parameters=translate_parameters(pipeline.get("parameters")),
            schedule=translate_schedule_trigger(pipeline["trigger"]) if pipeline.get("trigger") is not None else None,
            tasks=[PipelineTask(activity=task) for task in translated_tasks or []],
            tags=append_system_tags(pipeline.get("tags")),
        )

    not_translatable = []
    for warning in caught_warnings:
        if not issubclass(warning.category, UserWarning):
            continue
        message = str(warning.message)
        property_name = getattr(warning.message, "property_name", "unknown")
        activity_name = getattr(warning.message, "activity_name", None)
        activity_type = getattr(warning.message, "activity_type", None)
        entry = {
            "property": property_name,
            "message": message,
        }
        if activity_name is not None:
            entry["activity_name"] = activity_name
        if activity_type is not None:
            entry["activity_type"] = activity_type
        not_translatable.append(entry)
    pipeline_ir.not_translatable = not_translatable
    return pipeline_ir
