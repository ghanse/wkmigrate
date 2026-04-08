"""This module defines a translator for translating Execute Pipeline activities.

The translator resolves the child pipeline definition (embedded by the adapter
during enrichment), translates it into a ``Pipeline`` IR, and wraps the result
in an ``ExecutePipelineActivity``.

When the child pipeline definition is not available (e.g. the source definition
store could not resolve it), ``pipeline`` is set to ``None`` so the preparer can
emit a placeholder reference.
"""

from __future__ import annotations

import warnings
from importlib import import_module

from wkmigrate.models.ir.pipeline import ExecutePipelineActivity
from wkmigrate.models.ir.unsupported import UnsupportedValue
from wkmigrate.not_translatable import NotTranslatableWarning
from wkmigrate.utils import parse_mapping


def translate_execute_pipeline_activity(
    activity: dict, base_kwargs: dict
) -> ExecutePipelineActivity | UnsupportedValue:
    """Translates an ADF Execute Pipeline activity into an ``ExecutePipelineActivity``.

    The child pipeline referenced by ``pipeline.reference_name`` is expected to
    have been resolved and embedded under ``pipeline_definition`` by the adapter
    during enrichment.  When present the definition is translated into a
    ``Pipeline`` IR and attached to the resulting activity.

    Args:
        activity: Execute Pipeline activity definition as a ``dict``.
        base_kwargs: Common activity metadata.

    Returns:
        ``ExecutePipelineActivity`` referencing the child pipeline, or an
        ``UnsupportedValue`` if the pipeline reference is missing.
    """
    pipeline_ref = activity.get("pipeline")
    if not isinstance(pipeline_ref, dict):
        return UnsupportedValue(activity, "Missing property 'pipeline' for Execute Pipeline activity")

    pipeline_name = pipeline_ref.get("reference_name")
    if not pipeline_name:
        return UnsupportedValue(activity, "Missing property 'reference_name' in pipeline reference")

    parameters = parse_mapping(activity.get("parameters")) or None
    wait_on_completion = activity.get("wait_on_completion", True)

    if not wait_on_completion:
        warnings.warn(
            NotTranslatableWarning(
                "wait_on_completion",
                f"Execute Pipeline activity '{pipeline_name}' has wait_on_completion=false. "
                "Databricks Run Job tasks always wait for completion; "
                "the fire-and-forget semantic cannot be replicated.",
            ),
            stacklevel=2,
        )

    child_pipeline_ir = None
    pipeline_definition = activity.get("pipeline_definition")
    if pipeline_definition is not None:
        pipeline_translator = import_module("wkmigrate.translators.pipeline_translators.pipeline_translator")
        child_pipeline_ir = pipeline_translator.translate_pipeline(pipeline_definition)
    else:
        warnings.warn(
            NotTranslatableWarning(
                "pipeline_definition",
                f"Child pipeline '{pipeline_name}' definition was not resolved; "
                "the prepared task will reference the pipeline by name only.",
            ),
            stacklevel=2,
        )

    return ExecutePipelineActivity(
        **base_kwargs,
        pipeline_name=pipeline_name,
        pipeline=child_pipeline_ir,
        parameters=parameters,
        wait_on_completion=wait_on_completion,
    )
