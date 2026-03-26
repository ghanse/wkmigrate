# Workflow Migrator Release Notes

## Change History
All notable changes to the Workflow Migrator will be documented in this file.

### Version 0.1.0-post1

#### Added
* Added `JsonDefinitionStore` for translating ADF pipelines exported as JSON files without Azure credentials (#23)
* Added support for PostgreSQL, MySQL, and Oracle SQL datasets in Copy Data activities (#21)
* Added support for cloud file services (Amazon S3, Google Cloud Storage, Azure Blob Storage) as Copy Data sources and sinks (#33)
* Added support for Web activities (#22)
* Added support for SetVariable activities (#20)
* Added support for Databricks Job activities
* Added support for Lookup activities (#16)
* Added `load_all` method for translating multiple ADF pipelines concurrently (#29)
* Added `to_asset_bundle` method for generating Databricks asset bundles from translated pipelines
* Added configurable `credentials_scope` option for WorkspaceDefinitionStore (#37, #45)
* Added override options for WorkspaceDefinitionStore (root_path, compute_type, files_to_delta_sinks) (#32)
* Added containerized deployment support with Docker (#28)
* Added integration test framework with live Azure Data Factory fixtures (#41)
* Added `SourcePropertyCase` and `WorkflowSourceType` enums for configuration
* Added examples for JSON pipeline translation and end-to-end migration

#### Changed
* Split `dataset_translators` into a sub-package with per-type translator modules
* Split `linked_service_translators` into a sub-package with per-type translator modules
* Replaced `BaseFactoryDefinitionStore` inheritance with `PipelineAdapter` composition
* Removed `BaseFactoryClient` abstract class; `FactoryClient` is now a standalone dataclass
* Consolidated `datasets.py` constants into `parsers/dataset_parsers.py`
* Renamed `to_local_files` to `to_asset_bundle` (old method deprecated)
* Upgraded `azure-mgmt-datafactory` dependency from `^8.0.0` to `^9.0.0` for `DatabricksJobActivity` support

#### Fixed
* Fixed timeout parsing to handle all valid ADF formats (d.hh:mm:ss, hh:mm:ss) (#39)
* Fixed `credentials_scope` not being threaded through to `collect_data_source_secrets` (#45)
* Fixed ABFS dataset location key compatibility with Azure SDK `as_dict()` output (`file_system` vs `container`)
* Fixed `storage_account_name` fallback to URL parsing when the property is absent from ABFS linked services
* Fixed activities without `typeProperties` being silently dropped during ARM pipeline normalization

### Version 0.0.2-post1

#### Added
* Added `to_local_files` to dump Databricks definitions to local files
* Added output metadata for unsupported properties

### Version 0.0.2

#### Added
* Added support for Copy Data activities with SQL Server as a target

### Version 0.0.1-post2

#### Added
* Added Databricks notebook tasks as placeholders for unsupported ADF activities
* Added support for `@createArray(...)` lists  in For Each activities
* Added example

#### Modified
* Updated parsing for `@array(...)` lists in For Each activities

### Version 0.0.1-post1

#### Added
* Added Databricks notebook tasks as placeholders for unsupported ADF activities

### Version 0.0.1

#### Added
* Added `README.md`
* Initial release
