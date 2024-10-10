""" This module defines methods for translating data pipelines."""
import warnings
from wkmigrate.activity_translators.activity_translator import translate_activities
from wkmigrate.pipeline_translators.parameter_translator import translate_parameters
from wkmigrate.trigger_translators.schedule_trigger_translator import translate_schedule_trigger
from wkmigrate.utils import append_system_tags


def translate_pipeline(pipeline: dict) -> dict:
    """ Translates a data pipeline to a common object model.
        :parameter pipeline: Dictionary definition of the source pipeline
        :return: Dictionary definition of the target workflows"""
    if 'name' not in pipeline:
        warnings.warn(f'No pipeline name in source definition, setting to UNNAMED_WORKFLOW', stacklevel=2)
    # Translate the pipeline:
    translated_pipeline = {
        'name': pipeline.get('name', 'UNNAMED_WORKFLOW'),
        'parameters': translate_parameters(pipeline.get('parameters')),
        'schedule': translate_schedule_trigger(pipeline.get('trigger')),
        'tasks': translate_activities(pipeline.get('activities')),
        'tags': append_system_tags(pipeline.get('tags'))
    }
    return translated_pipeline
