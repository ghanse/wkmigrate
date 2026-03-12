# wkmigrate Design Standards and Best Practices

This document captures the architecture, coding conventions, and engineering patterns used in `wkmigrate`. It is intended as a living reference for contributors and AI agents working in this codebase.

---

## 1. Architecture Overview

### Purpose

`wkmigrate` is a Python library that migrates data pipeline definitions from Azure Data Factory (ADF) into Databricks Lakeflow Jobs. The library reads ADF pipeline JSON, translates it into an intermediate representation (IR), then materializes the result as Databricks Jobs, asset bundles, or local files.

### Module Layout

```
src/wkmigrate/
  __init__.py                    # Shared constants (JSON_PATH, YAML_PATH)
  __about__.py                   # Package version
  utils.py                       # Shared translation helpers (mapping, parsing, tags)
  not_translatable.py            # Warning infrastructure for non-translatable properties
  datasets.py                    # Dataset option/secret registries
  code_generator.py              # Spark notebook code-generation helpers

  clients/
    factory_client.py             # Azure Data Factory REST/SDK client wrapper

  definition_stores/
    __init__.py                   # Store type registry (types dict)
    definition_store.py           # Abstract DefinitionStore base class
    definition_store_builder.py   # Factory function: build_definition_store()
    factory_definition_store.py   # ADF source store (loads pipelines from ADF)
    workspace_definition_store.py # Databricks target store (creates jobs, bundles)

  enums/                          # Value-object enums (ComputePolicy, IsolationLevel, etc.)

  models/
    ir/                           # Intermediate Representation dataclasses
      pipeline.py                 # Pipeline, Activity (and all subtypes), Dependency, etc.
      datasets.py                 # Dataset IR
      linked_services.py          # Linked-service IR
      translation_context.py      # Immutable TranslationContext threaded through visitors
      translator_result.py        # TranslationResult type alias
      unsupported.py              # UnsupportedValue sentinel
    workflows/
      artifacts.py                # PreparedWorkflow, PreparedActivity, NotebookArtifact
      instructions.py             # PipelineInstruction, SecretInstruction

  parsers/
    dataset_parsers.py            # ADF dataset JSON -> Dataset IR
    expression_parsers.py         # ADF expression strings -> Python expressions

  preparers/
    preparer.py                   # Top-level prepare_workflow() dispatcher
    copy_activity_preparer.py     # Copy -> notebook + DLT pipeline artifacts
    for_each_activity_preparer.py
    if_condition_activity_preparer.py
    lookup_activity_preparer.py
    notebook_activity_preparer.py
    run_job_activity_preparer.py
    set_variable_activity_preparer.py
    spark_jar_activity_preparer.py
    spark_python_activity_preparer.py
    web_activity_preparer.py
    utils.py                      # Shared preparer helpers

  translators/
    activity_translators/
      activity_translator.py      # Top-level translate_activities() + dispatcher
      copy_activity_translator.py
      databricks_job_activity_translator.py
      for_each_activity_translator.py
      if_condition_activity_translator.py
      lookup_activity_translator.py
      notebook_activity_translator.py
      set_variable_activity_translator.py
      spark_jar_activity_translator.py
      spark_python_activity_translator.py
      web_activity_translator.py
      utils.py
    dataset_translators.py
    linked_service_translators.py
    pipeline_translators/
      pipeline_translator.py      # translate_pipeline()
      parameter_translator.py
      parsers.py
    trigger_translators/
      schedule_trigger_translator.py
      parsers.py
```

### Key Abstractions

| Concept                    | Module | Role |
|----------------------------|---|---|
| **DefinitionStore**        | `definition_stores/definition_store.py` | Abstract source/sink for pipeline definitions. `FactoryDefinitionStore` reads from ADF; `WorkspaceDefinitionStore` writes to Databricks. |
| **Pipeline / Activity IR** | `models/ir/pipeline.py` | Immutable dataclass hierarchy representing a translated pipeline. Activity subtypes include `DatabricksNotebookActivity`, `CopyActivity`, `ForEachActivity`, `IfConditionActivity`, etc. |
| **TranslationContext**     | `models/ir/translation_context.py` | Frozen dataclass threaded through translation visitors. Carries the activity cache, type-translator registry, and variable cache. Every mutation returns a new instance. |
| **PreparedWorkflow**       | `models/workflows/artifacts.py` | Collects the Databricks job payload, notebooks, DLT pipelines, and secrets needed to materialize a translated pipeline. |
| **Translator functions**   | `translators/` | Functions that convert ADF JSON into IR dataclasses. Simple type translators are pure `(dict, dict) -> TranslationResult`; control-flow translators additionally thread `TranslationContext` and return `(TranslationResult, TranslationContext)`. |
| **Preparer functions**     | `preparers/` | Functions that convert IR dataclasses into Databricks-ready task dicts + artifact lists. |
| **Parser functions**       | `parsers/`    | Functions that parse field values from ADF into Databricks equivalents                  |

### Data Flow

```
ADF JSON
  -> FactoryDefinitionStore.load()
    -> FactoryClient (Azure SDK)
    -> translate_pipeline()
      -> translate_activities() (topological sort + dispatch)
        -> per-type translator functions -> Activity IR
  -> Pipeline IR
  -> WorkspaceDefinitionStore.to_job() / .to_asset_bundle()
    -> prepare_workflow()
      -> per-type preparer functions -> PreparedWorkflow
    -> materialize (upload notebooks, create secrets, create jobs)
```

---

## 2. Coding Style

### Naming Conventions

- **Modules**: `snake_case` (e.g. `factory_definition_store.py`, `copy_activity_preparer.py`).
- **Classes**: `PascalCase` (e.g. `FactoryDefinitionStore`, `DatabricksNotebookActivity`).
- **Functions / methods**: `snake_case`, minimum 3 characters. Public translator functions follow the pattern `translate_<type>_activity()`. Public preparer functions follow `prepare_<type>_activity()`.
- **Constants**: `UPPER_CASE` (e.g. `JSON_PATH`, `DATASET_OPTIONS`).
- **Private members**: Single leading underscore (`_appenders`, `_parse_policy`).
- **Variables**: `snake_case`, 2-40 characters. Short names `f`, `i`, `j`, `k`, `df`, `e`, `ex`, `_` are allowed per pylint config.

### Imports

- Standard library first, then third-party, then `wkmigrate` internal imports.
- `from __future__ import annotations` at the top of modules that use forward references.
- Prefer explicit imports (`from wkmigrate.models.ir.pipeline import Activity`) over wildcard imports.
- `isort` is configured via ruff; first-party package is `wkmigrate`.

### Formatting

- **Line length**: 120 characters (Black and Ruff).
- **Formatter**: Black with `skip-string-normalization = true` (single quotes are preserved where used).
- **Target**: Python 3.12.
- Run `make fmt` to apply Black, Ruff (with `--fix`), mypy, and pylint in sequence.

### Docstrings

- Module-level docstrings on every module describing its role and typical usage.
- Google-style docstrings with `Args:`, `Returns:`, `Raises:` sections.
- Pylint docstring checks are relaxed: `missing-module-docstring`, `missing-class-docstring`, and `missing-function-docstring` are disabled, but new code should include docstrings for public APIs.
- Inline code references use double backticks in docstrings (e.g. `` ``Pipeline`` ``).

### Type Annotations

- All public function signatures are type-annotated.
- Union types use the `X | None` syntax (Python 3.10+).
- `TypeAlias` is used for type aliases (e.g. `TranslationResult`).
- mypy is configured with `mypy_path = "src"`, excluding tests, examples, and sandbox.

---

## 3. Engineering Patterns

### Immutable IR with Dataclasses

All IR models use `@dataclass(slots=True)` for memory efficiency and attribute-access safety. The `frozen=True` variant is used where immutability is required (e.g. `TranslationContext`). 
Some IR subtypes—especially deeply nested or control-flow activities—use `kw_only=True` to prevent positional-argument mistakes in complex hierarchies.

```python
@dataclass(slots=True, kw_only=True)
class DatabricksNotebookActivity(Activity):
    notebook_path: str
    base_parameters: dict[str, str] | None = None
```

### Immutable Context Threading

Translation state is captured in a `TranslationContext` (frozen dataclass with `MappingProxyType` fields). Every state transition produces a new context instance. Functions receive a context and return `(result, new_context)` tuples. This makes the data flow fully explicit and side-effect free.

```python
def visit_activity(activity, is_conditional_task, context):
    ...
    translated, context = _dispatch_activity(activity_type, activity, base_kwargs, context)
    context = context.with_activity(name, translated)
    return translated, context
```

### Registry-Based Dispatch

Activity translators are registered in a `dict[str, TypeTranslator]` mapping ADF type strings to translator callables. The dispatcher looks up the registry and falls back to a placeholder for unsupported types. Control-flow types (`IfCondition`, `ForEach`, `SetVariable`) are handled via a `match` statement because they require threading the context through child translations.

### Topological Activity Ordering

`translate_activities_with_context` builds a name-keyed index of activities, then visits them in dependency-first (topological) order. Each activity's `depends_on` upstream references are visited before the activity itself.

### Warning Infrastructure for Non-Translatable Properties

Rather than raising exceptions for unsupported ADF properties, translators emit `NotTranslatableWarning` via Python's `warnings` module. A `ContextVar`-backed context manager (`not_translatable_context`) automatically attaches the current activity name and type to each warning. Warnings are collected and surfaced in `Pipeline.not_translatable` and the `unsupported.json` output file.

### Factory Pattern for Definition Stores

`build_definition_store(type_key, options)` resolves a string key to a concrete `DefinitionStore` class from the `definition_stores.types` registry and instantiates it with the provided options. This decouples CLI/configuration code from specific store implementations.

### Validation in `__post_init__`

Dataclass-based stores (`FactoryDefinitionStore`, `WorkspaceDefinitionStore`) validate required fields in `__post_init__` and raise `ValueError` with descriptive messages for missing configuration. Client objects are also initialized here.

### Error Handling

- `ValueError` is the primary exception for configuration and validation errors.
- Warnings (not exceptions) for non-translatable properties via `NotTranslatableWarning`.
- `warnings.warn()` with explicit `stacklevel` for notebook-not-found and download failures.
- Broad `except Exception` is used sparingly and only at I/O boundaries (e.g. notebook download), where the operation should degrade gracefully.

### Code Generation

The `code_generator.py` module emits Python source fragments for Databricks notebooks. Generated code is formatted with `autopep8.fix_code()`. Preparers compose these fragments into complete notebook content stored as `NotebookArtifact` objects.

---

## 4. Testing Standards

### Organization

Tests live in `tests/` at the project root. Test files mirror source modules:

| Source | Test |
|---|---|
| `translators/activity_translators/` | `test_activity_translators.py`, `test_activity_translator.py` |
| `translators/pipeline_translators/` | `test_pipeline_translator.py`, `test_pipeline_integration.py` |
| `translators/linked_service_translators.py` | `test_linked_service_translator.py`, `test_linked_service_translators.py` |
| `translators/trigger_translators/` | `test_trigger_translator.py` |
| `definition_stores/` | `test_definition_store.py`, `test_definition_store_builder.py` |
| `code_generator.py` | `test_code_generator.py` |
| `utils.py` | `test_utils.py` |

### Fixtures

Test data is loaded from JSON files in `tests/resources/activities/` and `tests/resources/json/`. The `conftest.py` module provides:

- `load_fixtures(filename)` / `get_fixture(fixtures, fixture_id)` helpers for loading and looking up test cases.
- `get_base_kwargs(activity)` to build the standard base-properties dict passed to translators.
- Named pytest fixtures per activity type: `notebook_activity_fixtures`, `spark_jar_activity_fixtures`, `for_each_activity_fixtures`, etc.

### Mocking

External dependencies (Azure SDK, Databricks SDK) are replaced with lightweight doubles defined in `conftest.py`:

- `MockFactoryClient`: Reads from JSON fixture files instead of Azure APIs.
- `MockWorkspaceClient`: In-memory doubles for `jobs`, `workspace`, `pipelines`, and `secrets` APIs.
- Fixtures like `mock_factory_client` and `mock_workspace_client` use `monkeypatch` to swap in the doubles.

### What to Test

- **Translator functions**: Given an ADF activity dict and base kwargs, assert the returned `Activity` subtype has the correct fields. Use JSON fixtures for realistic payloads.
- **Preparer functions**: Given an `Activity` IR, assert the returned `PreparedActivity` has the correct task dict structure, notebook content, and side-effect artifacts.
- **Definition stores**: Use mock clients to test `load()`, `to_job()`, and `to_asset_bundle()` end-to-end without network calls.
- **Parsers**: Test expression and dataset parsing with representative ADF expression strings.
- **Integration tests**: `test_pipeline_integration.py` tests the full `load -> translate -> prepare` pipeline against fixture data.

### Running Tests

```bash
make test          # poetry run pytest
make fmt           # black + ruff + mypy + pylint
```

pytest is configured with `--no-header` and suppresses `DeprecationWarning`.

---

## 5. API Design

### Public API Surface

The primary public API consists of:

1. **`build_definition_store(type_key, options)`** -- Factory function to create a `DefinitionStore` from a string key and options dict.
2. **`FactoryDefinitionStore.load(pipeline_name)`** -- Load an ADF pipeline and return a `Pipeline` IR.
3. **`WorkspaceDefinitionStore.to_job(pipeline)`** -- Materialize a translated pipeline as a Databricks job.
4. **`WorkspaceDefinitionStore.to_asset_bundle(pipeline, directory)`** -- Write a Databricks asset bundle to disk.
5. **`translate_pipeline(pipeline_dict)`** -- Convert raw ADF JSON into a `Pipeline` IR (used internally by `FactoryDefinitionStore.load`).
6. **`translate_activities(activities)`** -- Convert a list of ADF activity dicts into `Activity` IR objects.
7. **`prepare_workflow(pipeline)`** -- Convert a `Pipeline` IR into a `PreparedWorkflow`.

### Deprecation

Deprecated methods use `@deprecated("Use 'new_method' as of wkmigrate X.Y.Z")` from `typing_extensions`. Examples: `dump()` -> `to_job()`, `to_local_files()` -> `to_asset_bundle()`.

### Versioning

Version is maintained in `src/wkmigrate/__about__.py` as `__version__`. The package version in `pyproject.toml` is managed separately by Poetry.

---

## 6. Dependencies

### Runtime

| Library | Role |
|---|---|
| `azure-identity`, `azure-common`, `azure-core`, `azure-mgmt-core`, `azure-mgmt-datafactory` | Azure SDK for authenticating and reading ADF pipeline definitions. |
| `databricks-sdk` | Databricks workspace client for creating jobs, uploading notebooks, managing secrets. |
| `databricks-bundles` | Support for generating Databricks asset bundle manifests. |
| `PyYAML` | Serializing asset bundle YAML files (`databricks.yml`, job resources). |
| `click` | CLI framework for the `wkmigrate` command-line entry point. |
| `autopep8` | Formatting generated Python notebook source code. |

### Development

| Library | Role |
|---|---|
| `pytest` | Test runner. |
| `coverage` | Code coverage measurement. |
| `black` | Code formatter (line length 120, skip string normalization). |
| `ruff` | Linter and import sorter. |
| `pylint` | Static analysis with Google-style configuration and extensive plugin set. |
| `mypy` | Static type checker. |
| `pydoc-markdown` | API documentation generator for Docusaurus. |

### Build

- **Poetry** as the build system (`poetry-core` backend).
- Python 3.12+ required.
- `make dev` installs Poetry 2.2.1 and runs `poetry install`.
- Docker support via `Dockerfile` and `docker-compose.yml` (`make docker`).
