"""This module defines the immutable translation context threaded through activity translation.

The ``TranslationContext`` captures all accumulated state produced during translation.  It is
a frozen dataclass so that every state transition is made explicit: functions receive a context,
and return a new one alongside their result.  This makes the data flow through the translation
pipeline fully transparent and side-effect free.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from wkmigrate.models.ir.pipeline import Activity


@dataclass(frozen=True, slots=True)
class TranslationContext:
    """
    Immutable snapshot of translation state threaded through each visitor call.

    Every function that needs to read or extend the caches receives a
    ``TranslationContext`` and returns a new one — the original is never mutated.

    Attributes:
        activity_cache: Read-only mapping of activity names to translated ``Activity`` objects.
        registry: Read-only mapping of activity type strings to their translator callables.
        variable_cache: Read-only mapping of variable names to the task keys of the task that sets the variable value.
    """

    activity_cache: MappingProxyType[str, Activity] = field(default_factory=lambda: MappingProxyType({}))
    registry: MappingProxyType[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    variable_cache: MappingProxyType[str, str] = field(default_factory=lambda: MappingProxyType({}))

    def with_activity(self, name: str, activity: Activity) -> TranslationContext:
        """
        Returns a new context with an activity added to the cache.

        Args:
            name: Activity name used as the cache key.
            activity: Translated ``Activity`` to store.

        Returns:
            New ``TranslationContext`` containing the updated activity cache.
        """
        return TranslationContext(
            activity_cache=MappingProxyType({**self.activity_cache, name: activity}),
            registry=self.registry,
            variable_cache=self.variable_cache,
        )

    def get_activity(self, activity_name: str) -> Activity | None:
        """
        Looks up a previously translated activity by name.

        Args:
            activity_name: Activity name.

        Returns:
            Cached ``Activity`` or ``None`` if the name has not been visited.
        """
        return self.activity_cache.get(activity_name)

    def with_variable(self, variable_name: str, task_key: str) -> TranslationContext:
        """
        Returns a new context with a variable added to the cache.

        Args:
            variable_name: Variable name used as the cache key.
            task_key: Task key for the task which set the variable value.

        Returns:
            New ``TranslationContext`` containing the updated variable cache.
        """
        return TranslationContext(
            activity_cache=self.activity_cache,
            registry=self.registry,
            variable_cache=MappingProxyType({**self.variable_cache, variable_name: task_key}),
        )

    def get_variable_task_key(self, variable_name: str) -> str | None:
        """
        Looks up the task key which set a variable.

        Args:
            variable_name: Variable name.

        Returns:
            Cached task key of the task that set the variable value.
        """
        return self.variable_cache.get(variable_name)
