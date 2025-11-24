"""This module defines methods for translating If Condition activities."""

import warnings
from wkmigrate.activity_translators.parsers import parse_condition_expression
from wkmigrate.utils import translate
from wkmigrate.not_translatable import NotTranslatableWarning

mapping = {
    "op": {
        "key": "expression",
        "parser": lambda x: parse_condition_expression(x).get("op"),
    },
    "left": {
        "key": "expression",
        "parser": lambda x: parse_condition_expression(x).get("left"),
    },
    "right": {
        "key": "expression",
        "parser": lambda x: parse_condition_expression(x).get("right"),
    },
}


def translate_if_condition_activity(activity: dict) -> dict | tuple[dict, list[dict]]:
    """Translates an If Condition activity definition in Data Factory's object model to a Databricks if/else condition
    task in the Databricks SDK object model.
    :parameter activity: If Condition activity definition as a ``dict``
    :return: Databricks if/else condition task properties as a ``dict``
    """
    # Parse the condition expression:
    translated_activity = translate(activity, mapping)
    if translated_activity is None:
        raise ValueError('Translation failed')
    if "if_false_activities" not in activity and "if_true_activities" not in activity:
        warnings.warn(
            NotTranslatableWarning("if_condition.activities", "No child activities of if-else condition activity"),
            stacklevel=2,
        )
        return translated_activity
    child_activities = []
    if "if_false_activities" in activity:
        if_false_activities = activity.get("if_false_activities")
        parent_task_name = activity.get("name")
        if if_false_activities is not None and parent_task_name is not None:
            child_activities.extend(
                parse_child_activities(
                    child_activities=if_false_activities,
                    parent_task_name=parent_task_name,
                    parent_task_outcome="false",
                )
            )
    if "if_true_activities" in activity:
        if_true_activities = activity.get("if_true_activities")
        parent_task_name = activity.get("name")
        if if_true_activities is not None and parent_task_name is not None:
            child_activities.extend(
                parse_child_activities(
                    child_activities=if_true_activities,
                    parent_task_name=parent_task_name,
                    parent_task_outcome="true",
                )
            )
    return translated_activity, child_activities


def parse_child_activities(child_activities: list[dict], parent_task_name: str, parent_task_outcome: str) -> list[dict]:
    """Translates if-else condition dependencies from Data Factory's object model to
    tasks in the Databricks SDK's object model.
    :parameter child_activities: List of child tasks as ``dict``
    :parameter parent_task_name: Name of the parent if-else condition task as a ``str``
    :parameter parent_task_outcome: Outcome of the parent if-else condition (either `"true"` or `"false`")
    :return: List of child tasks as ``dict``
    """
    translated_dependencies = []
    for activity in child_activities:
        if activity["depends_on"] is None or len(activity["depends_on"]) == 0:
            activity["depends_on"].append({"activity": parent_task_name, "outcome": parent_task_outcome})
        translated_dependencies.append(activity)
    return translated_dependencies
