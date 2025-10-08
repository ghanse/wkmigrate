from enum import Enum


class ConditionOperationPattern(Enum):
    EQUAL_TO = r"@equals\((.+),\s*(.+)\)"
    GREATER_THAN = r"@greater\((.+),\s*(.+)\)"
    GREATER_THAN_OR_EQUAL = r"@greaterOrEquals\((.+),\s*(.+)\)"
    LESS_THAN = r"@less\((.+),\s*(.+)\)"
    LESS_THAN_OR_EQUAL = r"@lessOrEquals\((.+),\s*(.+)\)"
    NOT_EQUAL = r"@not\(equals\((.+),\s*(.+)\)\)"
