# Notebook Processing Pipeline

This folder contains the tooling to turn the base Python script into a rich, documented Jupyter Notebook.

## Concept

- The base script (`hybrid_rag_pipeline.py`) contains only code and special placeholders ("handles") that
  indicate where markdown should be inserted. Handles look like:

  ```
  # [[MD:INTRO]]
  # [[MD:CONFIG]]
  # [[MD:S3_SOURCE_CONNECTOR]]
  # [[MD:ES_SOURCE_CONNECTOR]]
  # [[MD:ES_DESTINATION_CONNECTOR]]
  # [[MD:WORKFLOW_NODES]]
  # [[MD:CREATE_WORKFLOWS]]
  # [[MD:RUN_WORKFLOW]]
  # [[MD:JOB_MONITORING]]
  # [[MD:ES_PREPROCESSING]]
  # [[MD:SUMMARY]]
  # [[MD:MAIN]]
  ```

- The markdown for each handle lives in `markdown_blocks.yaml`.
- The enrichment script `enrich_and_convert.py` reads the base file, replaces each handle with the
  corresponding markdown block, writes `hybrid_rag_pipeline_enriched.py`, and converts that to
  `hybrid_rag_pipeline_enriched.ipynb` using jupytext.

## Code Modularization (Proposed Enhancement)

Similar to how markdown content is separated, we can also modularize the Python code into separate script files:

### Code Script Structure

Create a `code-scripts/` folder with individual Python files for each logical component:

```
notebook-processing/
├── code-scripts/
│   ├── __init__.py                    # Empty file for Python module
│   ├── dependencies.py               # ensure_notebook_deps()
│   ├── data_preparation.py           # download_file(), setup_*_data(), prepare_data_sources()
│   ├── connectors.py                 # create_*_connector() functions
│   ├── workflows.py                  # create_workflow_nodes(), create_parallel_workflows()
│   ├── execution.py                  # run_workflow(), poll_job_status()
│   ├── preprocessing.py              # run_elasticsearch_preprocessing()
│   ├── verification.py               # verify_customer_support_results(), print_pipeline_summary()
│   └── main.py                       # main() function
├── code_blocks.yaml                  # Maps code handles to script files
├── enrich_and_convert.py             # Enhanced to handle both MD and CODE handles
└── markdown_blocks.yaml              # Existing markdown content
```

### Code Handle Format

In the main pipeline file, use code handles similar to markdown handles:

```python
#!/usr/bin/env python3
# [[MD:INTRO]]

# [[MD:CONFIG]]

# [[CODE:DEPENDENCIES]]

# [[CODE:DATA_PREPARATION]]

# [[CODE:CONNECTORS]]

# [[CODE:WORKFLOWS]]

# [[CODE:EXECUTION]]

# [[CODE:PREPROCESSING]]

# [[CODE:VERIFICATION]]

# [[CODE:MAIN]]

# [[MD:EXECUTION_FLOW]]

# Run the pipeline
s3_job_id, es_job_id = main()

# Poll both jobs to make sure they have completed before proceeding
es_job_info = poll_job_status(es_job_id, "Elasticsearch Ingest")
s3_job_info = poll_job_status(s3_job_id, "S3 Ingest")

# Verify the results
print("\n🔍 Verifying processed results")
print("-" * 50)
verify_customer_support_results()
```

### Code Blocks Configuration

`code_blocks.yaml` would map handles to script files:

```yaml
DEPENDENCIES: code-scripts/dependencies.py
DATA_PREPARATION: code-scripts/data_preparation.py
CONNECTORS: code-scripts/connectors.py
WORKFLOWS: code-scripts/workflows.py
EXECUTION: code-scripts/execution.py
PREPROCESSING: code-scripts/preprocessing.py
VERIFICATION: code-scripts/verification.py
MAIN: code-scripts/main.py
```

### Benefits

1. **Separation of Concerns**: Each script file handles one logical area
2. **Easier Maintenance**: Update individual components without touching the main file
3. **Reusability**: Code modules can be imported and used in other projects
4. **Testing**: Individual components can be unit tested separately
5. **Collaboration**: Multiple developers can work on different components
6. **Version Control**: Cleaner diffs when changes are made to specific components

## Why this matters

This makes the notebook content repeatable and reviewable. When you say "update the notebook text," you should:
1. Edit the appropriate markdown block(s) in `markdown_blocks.yaml`.
2. Re-run the enrichment script to regenerate the enriched Python file and notebook.

For code changes:
1. Edit the appropriate script file in `code-scripts/`.
2. Re-run the enrichment script to regenerate the enriched Python file and notebook.

This avoids accidental code edits inside the notebook and keeps documentation close to code yet separate.

## Usage

### Basic Usage (Modular Mode - Default)

Run from the repo root:

```bash
/Users/nvannest/Documents/GitHub/rag-over-hybrid-data-sources/venv/bin/python \
  notebook-processing/enrich_and_convert.py
```

This uses the **modular methodology** by default, processing `hybrid_rag_pipeline_modular.py` with both `[[MD:...]]` and `[[CODE:...]]` handles.

### Advanced Usage

```bash
# Use modular approach (default)
python notebook-processing/enrich_and_convert.py --source modular

# Use original approach (markdown only)
python notebook-processing/enrich_and_convert.py --source original

# Custom output suffix
python notebook-processing/enrich_and_convert.py --output-suffix custom

# Combine options
python notebook-processing/enrich_and_convert.py --source original --output-suffix legacy
```

### Output Files

**Modular Mode** (default):
- Source: `hybrid_rag_pipeline_modular.py` (template with handles)
- Output: `hybrid_rag_pipeline_enriched.py` (assembled code)
- Notebook: `hybrid_rag_pipeline_enriched.ipynb` (Jupyter notebook)

**Original Mode**:
- Source: `hybrid_rag_pipeline.py` (original file with MD handles)
- Output: `hybrid_rag_pipeline_enriched.py` (markdown-enriched)
- Notebook: `hybrid_rag_pipeline_enriched.ipynb` (Jupyter notebook)

## Notes
- If any `# [[MD:...]]` handle remains unreplaced, the script will exit with an error in strict mode.
- If any `# [[CODE:...]]` handle remains unreplaced, the script will exit with an error in strict mode.
- Update or add new handles in the base file where you want new documentation sections.
- Add the corresponding block in `markdown_blocks.yaml` or script in `code-scripts/` using the exact same key.

