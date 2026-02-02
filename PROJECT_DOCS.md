# MVP-SQL-V2 Project Structure & Functionality Reference

This document outlines the functional organization of the MVP-SQL-V2 project, designed to assist developers in quickly
understanding the system architecture and module responsibilities.

## 1. Project Purpose

The project aims to convert relational databases (SQLite) into a rich **Directed Graph (NetworkX DiGraph)**
representation. This graph encapsulates not only the schema structure (tables, columns, foreign keys) but also data
statistics (profiling) and auxiliary metadata. The resulting graph serves as a structured input for downstream tasks
such as Text-to-SQL generation or Graph Neural Network (GNN) analysis.

## 2. Directory Structure Functionality

```text
d:\MVP-SQL-V2/
├── configs/                # Global Configuration
│   └── paths.py            # Defines absolute paths for datasets and output directories
│   └── prompts/            # Decoupled Prompt Templates (YAML)
├── data/                   # Input Data Storage
│   ├── bird/               # BIRD dataset databases (SQLite)
│   └── spider/             # Spider dataset databases (SQLite)
├── output/                 # Processed Artifacts
│   └── converted_graph_pkl/# Final serialized graphs (.pkl files)
├── src/                    # Source Code
│   ├── graph/              # Core ETL & Graph Construction Logic
│   │   ├── core/           # Low-level processing components
│   │   ├── pipeline.py     # Main ETL entry point
│   │   └── batch_run.py    # Batch processing script
│   ├── llm/                # LLM Interaction Module
│   │   ├── clients.py      # OpenAI, Ollama & Gemini Client Implementations
│   │   ├── prompt_manager.py # Prompt Loading & Formatting
│   │   └── example_usage.py # Usage examples for LLM clients
│   └── utils/              # Downstream Consumption Utilities
│       ├── dataloder.py    # NL-SQL dataset loader
│       ├── graph_explorer.py # Graph query API
│       └── schema_generator.py # Schema text description generator
```

## 3. Core Module Details

### 3.1. Graph Construction Engine (`src/graph/`)

This module handles the transformation from SQLite to NetworkX Graph.

* **`pipeline.py` (SchemaPipeline)**:
    * **Role**: The orchestrator of the ETL process.
    * **Function**: Connects to SQLite, iterates through tables/columns, triggers profiling, and directs
      the `GraphBuilder` to construct the graph.
    * **Key Method**: `run()` - Executes the full extraction flow and saves the output.

* **`core/builder.py` (GraphBuilder)**:
    * **Role**: Graph abstraction layer.
    * **Function**: Encapsulates `networkx` operations. Ensures nodes (Tables, Columns) and edges (HAS_COLUMN,
      FOREIGN_KEY) are created with consistent attributes.

* **`core/data_profiler.py` (DataProfiler)**:
    * **Role**: Data statistics analyzer.
    * **Function**: Samples data from columns to compute metrics like null rate, distinct values, numeric distribution (
      mean/mode), and string length. These statistics are embedded as node attributes.

* **`core/metadata_manager.py` (MetadataManager)**:
    * **Role**: External metadata integrator.
    * **Function**: Reads auxiliary CSV files (from `database_description/`) to inject human-readable descriptions into
      the graph nodes.

* **`core/sqlite_handler.py` (SQLiteHandler)**:
    * **Role**: Database access layer.
    * **Function**: Manages safe SQLite connections and executes raw SQL queries for schema extraction and data
      sampling.

### 3.2. LLM Interaction Module (`src/llm/`)

This module provides a unified interface for interacting with Large Language Models and managing prompts.

* **`clients.py` (LLMFactory, BaseLLMClient)**:
    * **Role**: Abstraction layer for LLM providers.
  * **Function**: Supports **OpenAI-compatible APIs** (via `OpenAIClient`), **Private Ollama Models** (
    via `OllamaClient`), and **Google Gemini** (via `GeminiClient`).
    Use `LLMFactory.create_client('openai'|'ollama'|'gemini', **config)` to instantiate. Note that Gemini requires
    the `google-generativeai` package.
* **`prompt_manager.py` (PromptManager)**:
    * **Role**: Prompt Management.
    * **Function**: Decouples prompts from code by loading them from YAML files in `configs/prompts/`. Supports
      templating (e.g., `{schema}`, `{question}`).

### 3.3. Utilities & Consumption (`src/utils/`)

This module provides tools for using the generated graphs and loading training data.

* **`dataloder.py` (DataLoader)**:
    * **Role**: Training data standardized loader.
    * **Function**: Loads and merges Text-to-SQL datasets (Spider, BIRD) from JSON files. Standardizes different formats
      into a unified list of dictionaries.

* **`graph_explorer.py` (GraphExplorer)**:
    * **Role**: Graph Access API.
    * **Function**: A wrapper around the loaded NetworkX graph. Provides high-level methods to retrieve tables, columns,
      foreign keys, and neighbor nodes without dealing with raw NetworkX syntax.

* **`schema_generator.py` (SchemaGenerator)**:
    * **Role**: Prompt/Description Generator.
    * **Function**: Uses `GraphExplorer` to generate structured text descriptions of tables and columns (e.g., for LLM
      prompts). Supports modes like "full", "brief", and "minimal".

## 4. System Data Flow

1. **Input**: Raw SQLite Database (`.sqlite`) + Optional CSV Metadata.
2. **Processing** (`SchemaPipeline`):
    * `SQLiteHandler` extracts schema structure.
    * `DataProfiler` computes data statistics.
    * `GraphBuilder` constructs the in-memory graph.
3. **Output**: Serialized Graph File (`.pkl`).
4. **Consumption**:
    * `DataLoader` loads NL-SQL pairs.
    * `GraphExplorer` loads `.pkl` to provide schema context for models.

## 5. Graph Artifact Specification

The output `.pkl` file contains a `networkx.DiGraph` with the following schema:

* **Nodes**:
    * **Type**: `Table`
        * *Attributes*: `name`, `row_count`, `primary_key`, `foreign_key`, `columns` (list).
    * **Type**: `Column`
        * *Attributes*: `name`, `data_type`, `is_primary_key`, `is_nullable`, `samples`, `numeric_mean`, `null_count`,
          etc.
* **Edges**:
    * **Type**: `HAS_COLUMN` (Table -> Column)
    * **Type**: `FOREIGN_KEY` (Table -> Table)
        * *Attributes*: `from_column`, `to_column`, `reference_path`.
