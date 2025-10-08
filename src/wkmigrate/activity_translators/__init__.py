from wkmigrate.activity_translators.copy_activity_translator import (
    translate_copy_activity,
)
from wkmigrate.activity_translators.for_each_activity_translator import (
    translate_for_each_activity,
)
from wkmigrate.activity_translators.if_condition_activity_translator import (
    translate_if_condition_activity,
)
from wkmigrate.activity_translators.notebook_activity_translator import (
    translate_notebook_activity,
)
from wkmigrate.activity_translators.spark_jar_activity_translator import (
    translate_spark_jar_activity,
)
from wkmigrate.activity_translators.spark_python_activity_translator import (
    translate_spark_python_activity,
)


type_mapping = {
    "DatabricksNotebook": "notebook_task",
    "DatabricksSparkJar": "spark_jar_task",
    "DatabricksSparkPython": "spark_python_task",
    "IfCondition": "condition_task",
    "ForEach": "for_each_task",
    "Copy": "copy_data_task",
}

type_translators = {
    "DatabricksNotebook": translate_notebook_activity,
    "DatabricksSparkJar": translate_spark_jar_activity,
    "DatabricksSparkPython": translate_spark_python_activity,
    "IfCondition": translate_if_condition_activity,
    "ForEach": translate_for_each_activity,
    "Copy": translate_copy_activity,
}
