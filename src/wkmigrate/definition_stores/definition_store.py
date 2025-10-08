"""This module defines the abstract ``DefinitionStore`` class."""

from abc import ABC, abstractmethod
from typing import Any


class DefinitionStore(ABC):
    """A source or sink for data pipeline definitions. Used to get or set
    data pipelines during migration.
    """

    @abstractmethod
    def load(self, *args: Any, **kwargs: Any) -> dict:
        """Loads the data pipeline into a dictionary object."""

    @abstractmethod
    def dump(self, *args: Any, **kwargs: Any) -> int | None:
        """Pushes the data pipeline definition into the definition store. This could
        create a file (e.g. JSON or YAML) or a Workflow definition in a Databricks
        workspace."""
