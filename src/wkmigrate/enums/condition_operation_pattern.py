"""Regular-expression helpers for IfCondition expressions."""

from enum import Enum


class ConditionOperationPattern(Enum):
    """
    ADF expression patterns supported by the ``IfCondition`` translator.

    Valid options:
        * ``EQUAL_TO``: Matches ``@equals(left, right)`` expressions.
        * ``GREATER_THAN``: Matches ``@greater(left, right)`` expressions.
        * ``GREATER_THAN_OR_EQUAL``: Matches ``@greaterOrEquals(left, right)`` expressions.
        * ``LESS_THAN``: Matches ``@less(left, right)`` expressions.
        * ``LESS_THAN_OR_EQUAL``: Matches ``@lessOrEquals(left, right)`` expressions.
        * ``NOT_EQUAL``: Matches ``@not(equals(left, right))`` expressions.
    """

    EQUAL_TO = r"@equals\((.+),\s*(.+)\)"
    GREATER_THAN = r"@greater\((.+),\s*(.+)\)"
    GREATER_THAN_OR_EQUAL = r"@greaterOrEquals\((.+),\s*(.+)\)"
    LESS_THAN = r"@less\((.+),\s*(.+)\)"
    LESS_THAN_OR_EQUAL = r"@lessOrEquals\((.+),\s*(.+)\)"
    NOT_EQUAL = r"@not\(equals\((.+),\s*(.+)\)\)"
