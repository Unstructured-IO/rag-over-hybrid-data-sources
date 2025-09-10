# Hybrid RAG Pipeline - Notebook Structure (Remaining Tasks Only)

This document tracks only the remaining work to finalize the notebook experience. All code cleanup, NER integration, and index setup are complete.

## Remaining Tasks

1. Convert script to Jupyter Notebook format
   - [ ] Author rich markdown content for each handle in `notebook-processing/markdown_blocks.yaml`
   - [ ] Run the enrichment and conversion script:
     - Enrich base script using `notebook-processing/enrich_and_convert.py`
     - Confirm `hybrid_rag_pipeline_enriched.py` was generated with markdown inserted
     - Confirm Jupytext produced `hybrid_rag_pipeline_enriched.ipynb`
   - [ ] Spot-check cell boundaries and formatting

2. Expand documentation content (in YAML)
   - [ ] Overview and architecture
   - [ ] Data sources (S3 PDFs, Elasticsearch)
   - [ ] Workflow nodes (VLM, Chunker, Embedder, NER)
   - [ ] Results interpretation and validation steps
   - [ ] RAG usage examples and prompts

3. Optional improvements
   - [ ] Enable job polling (replace the placeholder logic)
   - [ ] Add DAG visualization cell
   - [ ] Add links to verification scripts (e.g., `verify_customer_support_index.py`)

## How the Build Works

- Base script with handles: `hybrid_rag_pipeline.py` (contains only code + `# [[MD:...]]` placeholders)
- Markdown blocks mapping: `notebook-processing/markdown_blocks.yaml`
- Enrichment + conversion script: `notebook-processing/enrich_and_convert.py`
- Outputs (auto-generated when you run the script):
  - `hybrid_rag_pipeline_enriched.py`
  - `hybrid_rag_pipeline_enriched.ipynb`

Run locally:
```
/Users/nvannest/Documents/GitHub/rag-over-hybrid-data-sources/venv/bin/python \
  /Users/nvannest/Documents/GitHub/rag-over-hybrid-data-sources/notebook-processing/enrich_and_convert.py
```
