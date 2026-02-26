"""This module defines models for the results of translating ADF payloads into internal representations."""

from __future__ import annotations

from typing import TypeAlias

from wkmigrate.models.ir.pipeline import Activity
from wkmigrate.models.ir.unsupported import UnsupportedValue

TranslationResult: TypeAlias = Activity | UnsupportedValue
