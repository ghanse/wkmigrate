"""Helpers for tracking non-translatable pipeline properties.

These utilities capture contextual metadata (activity name/type) for any warnings
raised during translation. They centralize warning creation to ensure a consistent
schema across all translators and definition stores.
"""

from contextlib import contextmanager
from contextvars import ContextVar


_WARNING_CONTEXT: ContextVar[dict | None] = ContextVar("_WARNING_CONTEXT", default=None)


@contextmanager
def not_translatable_context(activity_name: str | None, activity_type: str | None):
    """
    Captures activity metadata for warnings raised inside the context.

    Args:
        activity_name: Logical name of the activity being translated.
        activity_type: Activity type string emitted by ADF.
    """
    token = _WARNING_CONTEXT.set({"activity_name": activity_name, "activity_type": activity_type})
    try:
        yield
    finally:
        _WARNING_CONTEXT.reset(token)


class NotTranslatableWarning(UserWarning):
    """Custom warning for properties that cannot be translated."""

    def __init__(self, property_name: str, message: str) -> None:
        """
        Initializes the warning and attaches contextual metadata.

        Args:
            property_name: Pipeline property that could not be translated.
            message: Human-readable warning message.
        """
        super().__init__(message)
        self.property_name = property_name
        context = _WARNING_CONTEXT.get()
        if context is None:
            self.activity_name = None
            self.activity_type = None
        else:
            self.activity_name = context.get("activity_name")
            self.activity_type = context.get("activity_type")
