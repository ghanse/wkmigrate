---
sidebar_label: parsers
title: wkmigrate.activity_translators.parsers
---

Utility functions for normalizing activity definitions into IR.

### parse\_dataset

```python
def parse_dataset(datasets: list[dict]) -> Dataset
```

Parses a dataset definition from a Data Factory pipeline activity into IR.

**Arguments**:

- `datasets` - Dataset references supplied on the activity.
  

**Returns**:

  Normalized dataset as a ``Dataset`` object.

### parse\_dataset\_mapping

```python
def parse_dataset_mapping(mapping: dict) -> list[ColumnMapping]
```

Parses a mapping from one set of data columns to another.

**Arguments**:

- `mapping` - Data column mapping definition.
  

**Returns**:

  List of column mapping definitions as ``ColumnMapping`` objects.

### parse\_dataset\_properties

```python
def parse_dataset_properties(dataset_definition: dict) -> DatasetProperties
```

Parses dataset properties from an ADF activity definition to a ``DatasetProperties`` object.

**Arguments**:

- `dataset_definition` - Dataset definition from the activity.
  

**Returns**:

  Dataset properties as a ``DatasetProperties`` object.
  

**Raises**:

- `ValueError` - If the dataset type is missing or not a string.
- `NotTranslatableWarning` - If the dataset property type is not supported.

### parse\_for\_each\_tasks

```python
def parse_for_each_tasks(tasks: list[dict] | None) -> list[Activity]
```

Parses multiple task definitions within a ForEach task.

**Arguments**:

- `tasks` - List of nested activity definitions.
  

**Returns**:

  Translated activities as ``Activity`` objects.

### parse\_for\_each\_items

```python
def parse_for_each_items(items: dict | None) -> str | None
```

Parses a list of items passed to a ForEach task into a serialized list expression.

**Arguments**:

- `items` - Expression describing ForEach items.
  

**Returns**:

  Serialized list expression understood by Databricks Jobs.

### parse\_policy

```python
def parse_policy(policy: dict | None) -> dict
```

Parses a data factory pipeline activity policy into a dictionary of policy settings.

**Arguments**:

- `policy` - Activity policy block from the ADF definition.
  

**Returns**:

  Dictionary containing policy settings.
  

**Raises**:

- `NotTranslatableWarning` - If secure input/output logging is used.

### parse\_dependencies

```python
def parse_dependencies(
        dependencies: list[dict] | None) -> list[Dependency] | None
```

Parses a data factory pipeline activity&#x27;s dependencies.

**Arguments**:

- `dependencies` - Dependency definitions provided by the activity.
  

**Returns**:

  List of ``Dependency`` objects describing upstream relationships.

### parse\_notebook\_parameters

```python
def parse_notebook_parameters(parameters: dict | None) -> dict | None
```

Parses task parameters in a Databricks notebook activity definition.

**Arguments**:

- `parameters` - Parameter dictionary from the ADF activity.
  

**Returns**:

  Mapping of parameter names to their default values.
  

**Raises**:

- `NotTranslatableWarning` - If a parameter cannot be resolved.

### parse\_condition\_expression

```python
def parse_condition_expression(condition: dict) -> dict
```

Parses a condition expression in an If Condition activity definition.

**Arguments**:

- `condition` - Condition expression dictionary from ADF.
  

**Returns**:

  Dictionary describing the parsed operator and its operands.
  

**Raises**:

- `ValueError` - If a valid condition expression cannot be parsed.

