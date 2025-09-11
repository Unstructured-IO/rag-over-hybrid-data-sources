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

## Why this matters

This makes the notebook content repeatable and reviewable. When you say “update the notebook text,” you should:
1. Edit the appropriate markdown block(s) in `markdown_blocks.yaml`.
2. Re-run the enrichment script to regenerate the enriched Python file and notebook.

This avoids accidental code edits inside the notebook and keeps documentation close to code yet separate.

## Usage

Run from the repo root:

```
/Users/nvannest/Documents/GitHub/rag-over-hybrid-data-sources/venv/bin/python \
  notebook-processing/enrich_and_convert.py
```

Outputs:
- `hybrid_rag_pipeline_enriched.py`
- `hybrid_rag_pipeline_enriched.ipynb`

## Notes
- If any `# [[MD:...]]` handle remains unreplaced, the script will exit with an error in strict mode.
- Update or add new handles in the base file where you want new documentation sections.
- Add the corresponding block in `markdown_blocks.yaml` using the exact same key.

