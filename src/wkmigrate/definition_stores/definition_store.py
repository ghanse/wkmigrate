"""This module defines the abstract ``DefinitionStore`` class."""

from abc import ABC, abstractmethod


class DefinitionStore(ABC):
    """Abstract source or sink for pipeline definitions."""

    @abstractmethod
    def load(self, pipeline_name: str) -> dict:
        """
        Loads a pipeline definition.

        Args:
            pipeline_name: Pipeline identifier as a ``str``.

        Returns:
            Dictionary representation of the pipeline as a ``dict``.
        """

    @abstractmethod
    def dump(self, pipeline_definition: dict) -> int | None:
        """
        Persists a pipeline definition.

        Args:
            pipeline_definition: Pipeline definition emitted by the translators as a ``dict``.

        Returns:
            Optional identifier for the stored workflow.
        """
