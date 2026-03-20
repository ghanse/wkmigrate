---
sidebar_label: code_generator
title: wkmigrate.code_generator
---

This module defines shared Spark code-generation helpers used by activity preparers.

Helpers in this module emit Python source fragments that read data, configure options,
and manage credentials. They are consumed by the Copy, Lookup, SetVariable, and Web
activity preparers to build Databricks notebooks.

#### get\_set\_variable\_notebook\_content

```python
def get_set_variable_notebook_content(variable_name: str,
                                      variable_value: str) -> str
```

Generates code to set a task value parameter. The notebook evaluates ``variable_value`` and sets a Databricks task
value parameter.

**Arguments**:

- `variable_name` - ADF variable name (used as the task-value key).
- `variable_value` - Python expression string produced by the expression parser.
  

**Returns**:

  Python notebook source string.

#### get\_option\_expressions

```python
def get_option_expressions(
        dataset_definition: dict,
        credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> list[str]
```

Generates code to create a Spark data source options dictionary for the specified dataset definition.

**Arguments**:

- `dataset_definition` - Dataset definition dictionary.
- `credentials_scope` - Name of the Databricks secret scope used for storing credentials.
  

**Returns**:

  List of Python source lines that creates an options dictionary.

#### get\_file\_options

```python
def get_file_options(
        dataset_definition: dict,
        file_type: str,
        credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> list[str]
```

Generates code to create a Spark data source options dictionary for a file dataset.

**Arguments**:

- `dataset_definition` - Dataset definition dictionary.
- `file_type` - File type (for example ``"csv"`` or ``"parquet"``).
- `credentials_scope` - Name of the Databricks secret scope used for storing credentials.
  

**Returns**:

  List of Python source lines that create the options dictionary.

#### get\_database\_options

```python
def get_database_options(
        dataset_definition: dict,
        database_type: str,
        credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> list[str]
```

Generates code to create a Spark data source options dictionary for interacting with a database.

**Arguments**:

- `dataset_definition` - Dataset definition dictionary.
- `database_type` - Database type (for example ``"sqlserver"``).
- `credentials_scope` - Name of the Databricks secret scope used for storing credentials.
  

**Returns**:

  List of Python source lines that create the options dictionary.

#### get\_jdbc\_url

```python
def get_jdbc_url(dataset_definition: dict) -> str
```

Constructs a JDBC connection URL from a flattened dataset definition.

The URL format varies by database type and respects the default port for
each engine when no explicit port is provided.

**Arguments**:

- `dataset_definition` - Flat dataset definition dictionary containing at
  least ``type``, ``host``, and ``database``, and optionally ``port``.
  

**Returns**:

  JDBC connection URL string.

#### get\_read\_expression

```python
def get_read_expression(source_definition: dict,
                        source_query: str | None = None) -> str
```

Generates code to read data from a data source into a DataFrame.

**Arguments**:

- `source_definition` - Dataset definition dictionary.
- `source_query` - Optional SQL query for database sources.
  

**Returns**:

  Python source lines that read data into a DataFrame.
  

**Raises**:

- `ValueError` - If the dataset type is not supported for reading.

#### get\_file\_uri

```python
def get_file_uri(definition: dict) -> str
```

Builds the cloud storage URI for a file dataset definition.

**Arguments**:

- `definition` - Dataset definition dictionary containing provider_type, container,
  folder_path, and (for Azure) storage_account_name.
  

**Returns**:

  Cloud storage URI string (for example ``s3a://bucket/path`` or
  ``abfss://container@account.dfs.core.windows.net/path``).

#### get\_file\_read\_expression

```python
def get_file_read_expression(source_definition: dict) -> str
```

Generates code to read data from a file dataset into a DataFrame.

**Arguments**:

- `source_definition` - Dataset definition dictionary.
  

**Returns**:

  Python source lines that read data into a DataFrame.

#### get\_delta\_read\_expression

```python
def get_delta_read_expression(source_definition: dict) -> str
```

Generates code to read data from a Delta table into a DataFrame.

**Arguments**:

- `source_definition` - Dataset definition dictionary.
  

**Returns**:

  Python source lines that read data into a DataFrame.

#### get\_jdbc\_read\_expression

```python
def get_jdbc_read_expression(source_definition: dict,
                             source_query: str | None = None) -> str
```

Generates code to read data from a database into a DataFrame.

**Arguments**:

- `source_definition` - Dataset definition dictionary.
- `source_query` - Optional SQL query string (default:  ``None``).
  

**Returns**:

  Python source lines that read data into a DataFrame.

#### get\_web\_activity\_notebook\_content

```python
def get_web_activity_notebook_content(
        activity_name: str,
        activity_type: str,
        url: str,
        method: str,
        body: Any,
        headers: dict[str, str] | None,
        authentication: Authentication | None = None,
        disable_cert_validation: bool = False,
        http_request_timeout_seconds: int | None = None,
        turn_off_async: bool = False,
        credentials_scope: str = DEFAULT_CREDENTIALS_SCOPE) -> str
```

Generates notebook source for a Web activity.

The generated notebook submits an HTTP request using the ``requests`` library
and publishes the response body and status code as Databricks task values.

**Arguments**:

- `activity_name` - Logical name of the activity being translated.
- `activity_type` - Activity type string emitted by ADF.
- `url` - Target URL for the HTTP request.
- `method` - HTTP method (for example ``GET``, ``POST``, ``PUT``, ``DELETE``).
- `body` - Optional request body. Passed as JSON when the body is a dict, or as raw data otherwise.
- `headers` - Optional HTTP headers dictionary.
- `authentication` - Parsed authentication configuration, or ``None``.
- `disable_cert_validation` - When ``True``, TLS certificate verification is skipped.
- `http_request_timeout_seconds` - Optional HTTP request timeout in seconds.
- `turn_off_async` - When ``True``, noted in the notebook as a comment for visibility.
- `credentials_scope` - Name of the Databricks secret scope used for storing credentials.
  

**Returns**:

  Formatted Python notebook source as a ``str``.

