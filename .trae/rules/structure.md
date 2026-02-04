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
│   └── schema_graph_repo/  # Processed Graph Repository (Schema + Profiling)
│       └── [dataset_name]/ # e.g. bird, spider
│           └── [db_name].pkl # Serialized NetworkX DiGraph
├── src/                    # Source Code
│   ├── graph/              # Core ETL & Graph Construction Logic
│   │   ├── core/           # Low-level processing components
│   │   ├── pipeline.py     # Main ETL entry point
│   │   ├── batch_run.py    # Batch processing script
│   │   ├── vis.py          # Graph Visualization App (Streamlit)
│   │   └── convert_repo.py # Legacy Graph Conversion Tool
│   ├── llm/                # LLM Interaction Module
│   │   ├── clients.py      # OpenAI, Ollama & Gemini Client Implementations
│   │   ├── prompt_manager.py # Prompt Loading & Formatting
│   │   └── example_usage.py # Usage examples for LLM clients
│   ├── schema_linking/     # Schema Linking Module
│   │   └── anchor_generator.py # Schema Anchor Generation
│   └── utils/              # Downstream Consumption Utilities
│       ├── dataloder.py    # NL-SQL dataset loader
│       ├── graph_explorer.py # Graph query API
│       ├── graph_loader.py # Graph Loading Utility
│       ├── graph_schema_extractor.py # Graph-based schema & FK extractor
│       ├── schema_generator.py # Schema text description generator
│       ├── sql_parser.py   # SQL analysis, validation & reporting
│       └── sql_vis.py      # SQL visualization & analysis tool (Streamlit)
```