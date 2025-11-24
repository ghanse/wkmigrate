"""Helpers for tracking non-translatable pipeline properties."""


class NotTranslatableWarning(UserWarning):
    """Custom warning for properties that cannot be translated."""

    def __init__(self, property_name: str, message: str):
        super().__init__(message)
        self.property_name = property_name

