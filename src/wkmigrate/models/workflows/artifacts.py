"""This module defines representational classes for Databricks workflow artifacts."""

from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from wkmigrate.models.ir.pipeline import Pipeline
from wkmigrate.models.workflows.instructions import PipelineInstruction, SecretInstruction


@dataclass(slots=True)
class PreparedWorkflow:
    """
    Artifacts generated while preparing a workflow.

    Attributes:
        pipeline: Pipeline IR that this workflow was prepared from.
        activities: Prepared activities that make up this workflow's tasks.
    """

    pipeline: Pipeline
    activities: list[PreparedActivity]

    @property
    def tasks(self) -> list[dict[str, Any]]:
        """Task dicts for the activities at this level."""
        return [activity.task for activity in self.activities]

    @property
    def all_notebooks(self) -> list[NotebookArtifact]:
        """All notebooks across this workflow and any nested inner workflows."""
        result: list[NotebookArtifact] = []
        for activity in self.activities:
            if activity.notebooks:
                result.extend(activity.notebooks)
            if activity.inner_workflow:
                result.extend(activity.inner_workflow.all_notebooks)
        return result

    @property
    def all_pipelines(self) -> list[PipelineInstruction]:
        """All pipeline instructions across this workflow and any nested inner workflows."""
        result: list[PipelineInstruction] = []
        for activity in self.activities:
            if activity.pipelines:
                result.extend(activity.pipelines)
            if activity.inner_workflow:
                result.extend(activity.inner_workflow.all_pipelines)
        return result

    @property
    def all_secrets(self) -> list[SecretInstruction]:
        """All secret instructions across this workflow and any nested inner workflows."""
        result: list[SecretInstruction] = []
        for activity in self.activities:
            if activity.secrets:
                result.extend(activity.secrets)
            if activity.inner_workflow:
                result.extend(activity.inner_workflow.all_secrets)
        return result

    @property
    def inner_workflows(self) -> list["PreparedWorkflow"]:
        """All inner workflows (recursively) produced by activities in this workflow."""
        result: list[PreparedWorkflow] = []
        for activity in self.activities:
            if activity.inner_workflow:
                result.append(activity.inner_workflow)
                result.extend(activity.inner_workflow.inner_workflows)
        return result


@dataclass(slots=True)
class PreparedActivity:
    """
    Artifacts generated while preparing a workflow task.

    Attributes:
        task: Task configuration as a dictionary.
        notebooks: List of ``NotebookArtifact`` objects to upload.
        pipelines: List of ``PipelineInstruction`` objects describing DLT pipelines to create.
        secrets: List of ``SecretInstruction`` objects describing secrets to materialize.
        inner_workflow: Additional workflow settings created for nested ForEach tasks.
    """

    task: dict[str, Any]
    notebooks: list[NotebookArtifact] | None = None
    pipelines: list[PipelineInstruction] | None = None
    secrets: list[SecretInstruction] | None = None
    inner_workflow: "PreparedWorkflow" | None = None


@dataclass(slots=True)
class NotebookArtifact:
    """
    Represents a notebook that needs to be materialized.

    Attributes:
        file_path: Workspace path where the notebook will be created or updated.
        content: Notebook source content as a single string.
        language: Notebook language (for example ``python`` or ``sql``). Defaults to ``\"python\"``.
    """

    file_path: str
    content: str
    language: str = "python"
