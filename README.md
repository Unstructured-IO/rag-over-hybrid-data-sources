# Hybrid RAG Pipeline over Multiple Data Sources

A comprehensive hybrid Retrieval-Augmented Generation (RAG) pipeline that processes multiple data sources using the Unstructured API to create a unified knowledge base for customer support applications.

## Overview

This project demonstrates how to build a hybrid RAG system that combines:

1. **Technical Documentation** (PDFs from S3) - Product manuals, troubleshooting guides, MSDS documents
2. **Sales Data** (Elasticsearch) - Customer interactions, product information, sales records
3. **Unified Processing** - NER enrichment, chunking, embedding, and vector storage

The pipeline processes both structured and unstructured data sources in parallel, enriches them with Named Entity Recognition (NER), and deposits the results into a unified Elasticsearch index for RAG applications.

## Architecture

### Parallel Workflow Processing

```
┌─────────────────┐                    ┌──────────────────────────────────────────────────────┐
│   S3 PDFs       │                    │              UNSTRUCTURED API PROCESSING             │
│ (Tech Manuals,  │────────────────────┤                                                      │
│  Safety Docs)   │                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
└─────────────────┘                    │  │Connect  │  │ Route   │  │Transform│  │  Chunk  │ │
                                       │  │   ↓     │  │   ↓     │  │   ↓     │  │    ↓    │ │
┌─────────────────┐    WORKFLOW 1      │  │ S3 Src  │→ │VLM Auto │→ │Elements │→ │By Title │ │
│ Elasticsearch   │────────────────────┤  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
│ (Sales Records) │                    │                                                      │
└─────────────────┘                    │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
                                       │  │Connect  │  │ Route   │  │Transform│              │
                   WORKFLOW 2          │  │   ↓     │  │   ↓     │  │   ↓     │              │
                                       │  │ ES Src  │→ │VLM Auto │→ │Elements │──────────────┤
                                       │  └─────────┘  └─────────┘  └─────────┘              │
                                       │                                                      │
                                       │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
                                       │  │ Enrich  │  │ Embed   │  │ Persist │              │
                                       │  │   ↓     │  │   ↓     │  │   ↓     │              │
                                       │  │OpenAI   │→ │OpenAI   │→ │   ES    │              │
                                       │  │  NER    │  │text-emb │  │customer-│              │
                                       │  │         │  │ -3-small│  │support  │              │
                                       │  └─────────┘  └─────────┘  └─────────┘              │
                                       └──────────────────────────────────────────────────────┘
                                                                          │
                                       ┌──────────────────────────────────▼───────────────────┐
                                       │           UNIFIED KNOWLEDGE BASE                      │
                                       │         Elasticsearch: customer-support              │
                                       │                                                       │
                                       │  • PDF content (manuals, troubleshooting)            │
                                       │  • Sales data (customer interactions, products)      │
                                       │  • Consistent chunking & embeddings                  │
                                       │  • Ready for hybrid RAG queries                      │
                                       └───────────────────────────────────────────────────────┘
```

### Unstructured's 7-Stage Pipeline:
1. **Connect**: Source connectors (S3, Elasticsearch) ingest data
2. **Route**: Auto partitioning strategy selects optimal processing (VLM for complex docs)  
3. **Transform**: Documents converted to Unstructured's canonical JSON schema
4. **Chunk**: By-title chunking creates semantically coherent retrieval units
5. **Enrich**: Optional NER extraction adds metadata and entities
6. **Embed**: OpenAI embeddings enable semantic similarity search
7. **Persist**: Destination connector writes processed data to vector database

## Features

### 🔧 **Smart Elasticsearch Preprocessing**
- **Index Validation**: Automatically checks for required `sales-records-consolidated` index
- **Data Verification**: Ensures source data exists before processing
- **Fresh Destination**: Automatically recreates `customer-support` index for clean runs
- **Error Handling**: Fails fast with clear error messages if prerequisites aren't met

### 🚀 **Parallel Workflow Processing**
- **S3 Source Connector**: Processes PDFs with VLM (Vision Language Model) parsing
- **Elasticsearch Source Connector**: Ingests sales records with rich NER data
- **Unified Destination**: Both workflows deposit into the same `customer-support` index

### 🎯 **Advanced Processing Pipeline**
- **VLM Partitioner**: Uses GPT-4o for intelligent document parsing
- **Smart Chunker**: Context-aware chunking with title-based segmentation
- **Vector Embedder**: OpenAI text-embedding-3-small for semantic search
- **NER Enrichment**: Extracts named entities (people, places, organizations, etc.)

### 📊 **Best Practices Implementation**
- **Context Managers**: Proper resource management with `UnstructuredClient`
- **Modern API Usage**: Uses `CreateWorkflowRequest` and `CreateWorkflow` objects
- **Error Handling**: Comprehensive exception handling with clear feedback
- **Logging**: Detailed progress tracking with emoji indicators

## Quick Start

### Prerequisites

1. **Environment Setup**:
   ```bash
   # Clone the repository
   git clone <repository-url>
   cd rag-over-hybrid-data-sources
   
   # Create and activate virtual environment
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Configuration**:
   ```bash
   # Copy environment template
   cp .env.template .env
   
   # Edit .env with your credentials:
   # - UNSTRUCTURED_API_KEY=your-unstructured-api-key
   # - ELASTICSEARCH_HOST=https://your-cluster.es.io:443
   # - ELASTICSEARCH_API_KEY=your-elasticsearch-api-key
   # - AWS_ACCESS_KEY_ID=your-aws-access-key
   # - AWS_SECRET_ACCESS_KEY=your-aws-secret-key
   # - S3_SOURCE_BUCKET=your-pdf-bucket
   # - S3_DESTINATION_BUCKET=your-output-bucket
   ```

### Running the Pipeline

#### Option 1: Python Script
```bash
# Activate virtual environment
source venv/bin/activate

# Run the pipeline
python hybrid_rag_pipeline.py
```

#### Option 2: Jupyter Notebook
```bash
# Start Jupyter
jupyter lab

# Open and run hybrid_rag_pipeline_enriched.ipynb
```

## Data Sources Setup

### 1. Elasticsearch Sales Data

The pipeline requires a `sales-records-consolidated` index with sales data. You can create this using the provided preprocessing tools:

```bash
# Run Elasticsearch preprocessing to create sample data
python elasticsearch_index_preprocessing.py
```

This creates:
- `sales-records` - Raw sales data (100 records)
- `sales-records-consolidated` - Processed sales data optimized for RAG
- `customer-support` - Empty destination index (created fresh each run)

### 2. S3 Technical Documentation

Upload your PDF documents to an S3 bucket. The pipeline supports:
- Product manuals
- Troubleshooting guides
- MSDS documents
- Technical specifications

Supported S3 URL formats:
- `s3://bucket-name/path/`
- `https://bucket-name.s3.region.amazonaws.com/path/`
- Raw bucket names: `bucket-name/path`

## Pipeline Workflow

### Step 0: Elasticsearch Preprocessing
- ✅ Validates `sales-records-consolidated` exists and has data
- ✅ Deletes and recreates fresh `customer-support` index
- ❌ Fails with clear error if source data is missing

### Step 1: Source Connectors
- Creates S3 source connector for PDFs
- Creates Elasticsearch source connector for sales data

### Step 2: Destination Connector  
- Creates Elasticsearch destination connector for `customer-support` index

### Step 3: Workflow Creation
- Creates parallel workflows for S3 and Elasticsearch sources
- Both workflows use identical processing nodes:
  - VLM Partitioner (GPT-4o)
  - Smart Chunker (title-based)
  - Vector Embedder (OpenAI)
  - NER Enrichment (OpenAI)

### Step 4: Execution
- Runs both workflows in parallel
- Monitors job status (optional)
- Reports completion status

## Project Structure

```
rag-over-hybrid-data-sources/
├── hybrid_rag_pipeline.py              # Main pipeline code
├── hybrid_rag_pipeline_enriched.py     # Generated enriched version
├── hybrid_rag_pipeline_enriched.ipynb  # Jupyter notebook
├── elasticsearch_index_preprocessing.py # ES data setup
├── requirements.txt                     # Python dependencies
├── README.md                           # This file
├── notebook-processing/                # Documentation pipeline
│   ├── enrich_and_convert.py          # Notebook generation script
│   ├── markdown_blocks.yaml           # Markdown content
│   └── README.md                       # Documentation workflow
├── elastic-search-index-setup/         # ES setup tools
│   ├── create_consolidated_index.py
│   ├── create_nonconsolidated_index.py
│   └── verify_elasticsearch_data.py
└── elasticsearch-example-data/         # Sample data
    ├── consolidated_examples.json
    └── nonconsolidated_examples.json
```

## Configuration Options

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `UNSTRUCTURED_API_KEY` | Your Unstructured API key | `your-api-key` |
| `ELASTICSEARCH_HOST` | Elasticsearch cluster URL | `https://cluster.es.io:443` |
| `ELASTICSEARCH_API_KEY` | Elasticsearch API key | `your-es-api-key` |
| `ELASTICSEARCH_INDEX` | Source sales data index | `sales-records-consolidated` |
| `AWS_ACCESS_KEY_ID` | AWS access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | `your-secret-key` |
| `S3_SOURCE_BUCKET` | S3 bucket with PDFs | `my-docs-bucket/manuals/` |
| `S3_DESTINATION_BUCKET` | S3 output bucket | `my-output-bucket` |

### Processing Parameters

- **Chunking**: 1500 chars with 2048 max, title-based segmentation
- **Embedding Model**: OpenAI text-embedding-3-small
- **VLM Model**: GPT-4o for document parsing
- **NER Model**: OpenAI NER extraction

## Monitoring and Debugging

### Pipeline Status
The pipeline provides detailed status updates:
- `:white_check_mark:` Success indicators
- `:x:` Error indicators  
- `📊` Progress information
- `🔍` Validation steps

### Common Issues

1. **Missing Sales Data**:
   ```
   ❌ Index 'sales-records-consolidated' does not exist. There is no data to use.
   ```
   **Solution**: Run `python elasticsearch_index_preprocessing.py`

2. **Empty Sales Index**:
   ```
   ❌ Index 'sales-records-consolidated' is empty. There is no data to use.
   ```
   **Solution**: Verify data was properly indexed

3. **S3 Access Issues**:
   ```
   :x: Error creating S3 source connector: Access Denied
   ```
   **Solution**: Check AWS credentials and bucket permissions

## Advanced Usage

### Custom NER Configuration
The NER enrichment node can be customized by updating the `settings` in `create_workflow_nodes()`:

```python
ner_enrichment_node = WorkflowNode(
    name="NER_Enrichment",
    subtype="openai_ner",
    type="prompter",
    settings={
        "prompt": "Extract named entities focusing on products, customers, and locations..."
    }
)
```

### Multiple S3 Sources
To process multiple S3 buckets, modify the S3 source connector creation or create additional workflows.

### Custom Elasticsearch Mapping
The `customer-support` index mapping can be customized in the `run_elasticsearch_preprocessing()` function.

## Development Workflow

### Notebook Content Management

**Important**: Do not edit the Jupyter notebook directly!

Instead, follow this workflow:

1. **Edit Code**: Modify `hybrid_rag_pipeline.py`
2. **Edit Documentation**: Update `notebook-processing/markdown_blocks.yaml`
3. **Regenerate**: Run `python notebook-processing/enrich_and_convert.py`

This process:
- Replaces `[[MD:HANDLE]]` placeholders with markdown content
- Generates `hybrid_rag_pipeline_enriched.py`
- Converts to `hybrid_rag_pipeline_enriched.ipynb` using jupytext

### Testing

```bash
# Test Elasticsearch connection
python elasticsearch-index-setup/simple_check.py

# Verify data setup
python elasticsearch-index-setup/verify_elasticsearch_data.py

# Run pipeline in test mode
python hybrid_rag_pipeline.py
```

## API Reference

### Core Functions

- `run_elasticsearch_preprocessing()` - Validates and prepares ES indices
- `create_s3_source_connector()` - Creates S3 PDF source
- `create_elasticsearch_source_connector()` - Creates ES sales source  
- `create_elasticsearch_destination_connector()` - Creates ES destination
- `create_parallel_workflows()` - Sets up processing workflows
- `run_workflow()` - Executes workflows
- `poll_job_status()` - Monitors job progress

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes following the development workflow
4. Test thoroughly
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues:
1. Check the troubleshooting section above
2. Review Unstructured API documentation
3. Open an issue in the repository

---

**Note**: This pipeline demonstrates advanced RAG techniques using the Unstructured API. It's designed for educational and development purposes. For production use, consider additional error handling, monitoring, and security measures.

