"""This module defines models for values which cannot be translated into supported internal representations.

Unsupported values in this module represent values that cannot be translated into supported internal
representations. Each unsupported value contains the original value that could not be translated and
a human-readable explanation of why translation failed.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any


@dataclass
class UnsupportedValue:
    """
    Represents a value that cannot be translated into a supported internal representation.

    Attributes:
        value: Original dictionary payload that failed translation.
        message: Human-readable explanation of why translation failed.
    """

    value: Any
    message: str
