"""This module defines methods for translating data pipelines."""

import warnings
from wkmigrate.activity_translators.activity_translator import translate_activities
from wkmigrate.pipeline_translators.parameter_translator import translate_parameters
from wkmigrate.trigger_translators.schedule_trigger_translator import (
    translate_schedule_trigger,
)
from wkmigrate.utils import append_system_tags
from wkmigrate.not_translatable import NotTranslatableWarning


def translate_pipeline(pipeline: dict) -> dict:
    """Translates a data pipeline to a common object model.
    :parameter pipeline: Dictionary definition of the source pipeline
    :return: Dictionary definition of the target workflows"""
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter("always", UserWarning)
        if "name" not in pipeline:
            warnings.warn(
                NotTranslatableWarning(
                    "pipeline.name", "No pipeline name in source definition, setting to UNNAMED_WORKFLOW"
                ),
                stacklevel=2,
            )
        translated_pipeline = {
            "name": pipeline.get("name", "UNNAMED_WORKFLOW"),
            "parameters": translate_parameters(pipeline.get("parameters")),
            "schedule": translate_schedule_trigger(pipeline["trigger"]) if pipeline.get("trigger") is not None else None,
            "tasks": translate_activities(pipeline.get("activities")),
            "tags": append_system_tags(pipeline.get("tags")),
        }

    not_translatable = []
    for warning in caught_warnings:
        if not issubclass(warning.category, UserWarning):
            continue
        message = str(warning.message)
        property_name = getattr(warning.message, "property_name", "unknown")
        not_translatable.append(
            {
                "property": property_name,
                "message": message,
            }
        )
    if not_translatable:
        translated_pipeline["not_translatable"] = not_translatable

    return translated_pipeline
