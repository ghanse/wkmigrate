---
sidebar_label: condition_operation_pattern
title: wkmigrate.enums.condition_operation_pattern
---

Regular-expression helpers for IfCondition expressions.

## ConditionOperationPattern Objects

```python
class ConditionOperationPattern(Enum)
```

ADF expression patterns supported by the ``IfCondition`` translator.

Valid options:
    * ``EQUAL_TO``: Matches ``@equals(left, right)`` expressions.
    * ``GREATER_THAN``: Matches ``@greater(left, right)`` expressions.
    * ``GREATER_THAN_OR_EQUAL``: Matches ``@greaterOrEquals(left, right)`` expressions.
    * ``LESS_THAN``: Matches ``@less(left, right)`` expressions.
    * ``LESS_THAN_OR_EQUAL``: Matches ``@lessOrEquals(left, right)`` expressions.
    * ``NOT_EQUAL``: Matches ``@not(equals(left, right))`` expressions.

